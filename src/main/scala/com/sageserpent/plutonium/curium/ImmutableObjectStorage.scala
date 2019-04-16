package com.sageserpent.plutonium.curium
import java.lang.reflect.Modifier

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.esotericsoftware.kryo.{
  Kryo,
  KryoSerializable,
  ReferenceResolver,
  Serializer
}
import com.google.common.collect.{BiMap, BiMapUsingIdentityOnForwardMappingOnly}
import com.sageserpent.plutonium.classFromType
import com.twitter.chill.{
  CleaningSerializer,
  EmptyScalaKryoInstantiator,
  KryoBase,
  KryoInstantiator,
  KryoPool
}
import net.bytebuddy.description.`type`.TypeDescription
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.bind.annotation.{
  FieldValue,
  Pipe,
  RuntimeType,
  This
}
import net.bytebuddy.implementation.{FieldAccessor, MethodDelegation}
import net.bytebuddy.matcher.ElementMatchers
import net.bytebuddy.{ByteBuddy, NamingStrategy}
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.strategy.StdInstantiatorStrategy

import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.util.hashing.MurmurHash3
import scala.util.{DynamicVariable, Try}

object ImmutableObjectStorage {
  type ObjectReferenceId = Int

  case class TrancheOfData(payload: Array[Byte],
                           objectReferenceIdOffset: ObjectReferenceId) {
    // TODO: have to override the default implementation solely to deal with array
    // equality - should cutover to using the augmented array type in Scala when I
    // remember what it is....
    override def equals(another: Any): Boolean = another match {
      case TrancheOfData(payload, objectReferenceIdOffset) =>
        this.payload
          .sameElements(payload) && this.objectReferenceIdOffset == objectReferenceIdOffset
      case _ => false
    }

    override def toString: String =
      s"TrancheOfData(payload hash: ${MurmurHash3.arrayHash(payload)}, object reference id offset: $objectReferenceIdOffset)"
  }

  type EitherThrowableOr[X] = Either[Throwable, X]

  trait Tranches[TrancheIdImplementation] {
    type TrancheId = TrancheIdImplementation

    def createTrancheInStorage(payload: Array[Byte],
                               objectReferenceIdOffset: ObjectReferenceId,
                               objectReferenceIds: Set[ObjectReferenceId])
      : EitherThrowableOr[TrancheId]

    def objectReferenceIdOffsetForNewTranche
      : EitherThrowableOr[ObjectReferenceId]

    def retrieveTranche(trancheId: TrancheId): EitherThrowableOr[TrancheOfData]

    def retrieveTrancheId(
        objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId]
  }

  trait TranchesContracts[TrancheId] extends Tranches[TrancheId] {
    // NOTE: after some um-ing and ah-ing, the contracts have been lifted into
    // the 'EitherThrowableOr' monad. This is motivated by the lack of transaction
    // support in the current API; it is not reasonable to expect client code to
    // satisfy preconditions dependent on state if the tranches implementation can
    // spontaneously lose data behind the client's back.

    abstract override def createTrancheInStorage(
        payload: Array[Byte],
        objectReferenceIdOffset: ObjectReferenceId,
        objectReferenceIds: Set[ObjectReferenceId])
      : EitherThrowableOr[TrancheId] =
      for {
        _ <- Try {
          require(
            objectReferenceIds.isEmpty || objectReferenceIdOffset <= objectReferenceIds.min)
        }.toEither
        objectReferenceIdOffsetForNewTranche <- this.objectReferenceIdOffsetForNewTranche
        _ <- Try {
          for (objectReferenceId <- objectReferenceIds) {
            require(objectReferenceIdOffsetForNewTranche <= objectReferenceId)
          }
        }.toEither
        id <- super.createTrancheInStorage(payload,
                                           objectReferenceIdOffset,
                                           objectReferenceIds)
      } yield id

    abstract override def retrieveTrancheId(
        objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] =
      for {
        objectReferenceIdOffsetForNewTranche <- this.objectReferenceIdOffsetForNewTranche
        _ <- Try {
          require(objectReferenceIdOffsetForNewTranche > objectReferenceId)
        }.toEither
        trancheId <- super.retrieveTrancheId(objectReferenceId)
      } yield trancheId
  }

  trait Operation[Result]

  type Session[X] = FreeT[Operation, EitherThrowableOr, X]

  protected trait ProxySupport {
    val byteBuddy = new ByteBuddy()

    trait AcquiredState {
      def underlying: AnyRef
    }

    private[curium] trait StateAcquisition {
      def acquire(acquiredState: AcquiredState): Unit
    }

    val kryoClosureMarkerClazz = classOf[Closure]

    val clazzesThatShouldNotBeProxied: Set[Class[_]] =
      Set(
        /*
          The next one is to workaround Kryo leaking its internal
          fudge as to how it registers closure serializers into the
          tranche specific reference resolver class, which in turn
          creates proxies. We don't want to proxy a closure anyway.
         */
        kryoClosureMarkerClazz,
        /*
          The next one is to prevent the proxying of proxies. Strictly
          speaking this shouldn't be needed, because there is logic elsewhere
          that will ensure that a proxy is never stored into a tranche
          as a distinct object from what it proxies; however due to the
          rather dubious way *this* code allows proxy classes to be passed
          to the reference resolver instances, that code will end up seeing
          the class objects for proxies, so this exclusion guards against *that*.
         */
        classOf[StateAcquisition],
        classOf[String],
        classOf[Class[_]]
      )

    val stateAcquisitionClazz = classOf[StateAcquisition]

    def isProxyClazz(clazz: Class[_]): Boolean =
      stateAcquisitionClazz.isAssignableFrom(clazz)

    def isProxy(immutableObject: AnyRef): Boolean =
      stateAcquisitionClazz.isInstance(immutableObject)

    val instantiatorStrategy: StdInstantiatorStrategy =
      new StdInstantiatorStrategy

    type PipeForwarding = Function[AnyRef, Nothing]

    object proxyDelayedLoading {
      @RuntimeType
      def apply(
          @Pipe pipeTo: PipeForwarding,
          @FieldValue("acquiredState") acquiredState: AcquiredState
      ): Any = {
        val underlying: AnyRef = acquiredState.underlying

        pipeTo(underlying)
      }
    }

    object proxySerialization {
      @RuntimeType
      def write(
          kryo: Kryo,
          output: Output,
          @FieldValue("acquiredState") acquiredState: AcquiredState): Unit =
        kryo.writeClassAndObject(output, acquiredState)

      @RuntimeType
      def read(kryo: Kryo, input: Input, @This thiz: StateAcquisition): Unit = {
        thiz.acquire(kryo.readClassAndObject(input).asInstanceOf[AcquiredState])
      }
    }
  }

  case class CompletedOperationData(referenceResolver: ReferenceResolver,
                                    topLevelObject: Any)
}

trait ImmutableObjectStorage[TrancheId] {
  import ImmutableObjectStorage._

  case class Store[X](immutableObject: X) extends Operation[TrancheId]

  case class Retrieve[X](trancheId: TrancheId, clazz: Class[X])
      extends Operation[X]

  def store[X](immutableObject: X): Session[TrancheId] =
    FreeT.liftF[Operation, EitherThrowableOr, TrancheId](Store(immutableObject))

  def retrieve[X: TypeTag](id: TrancheId): Session[X] =
    FreeT.liftF[Operation, EitherThrowableOr, X](
      Retrieve(id, classFromType(typeOf[X])))

  def runToYieldTrancheIds(session: Session[Vector[TrancheId]])
    : Tranches[TrancheId] => EitherThrowableOr[Vector[TrancheId]] =
    unsafeRun(session)

  def runToYieldTrancheId(session: Session[TrancheId])
    : Tranches[TrancheId] => EitherThrowableOr[TrancheId] =
    unsafeRun(session)

  def runForEffectsOnly(
      session: Session[Unit]): Tranches[TrancheId] => EitherThrowableOr[Unit] =
    unsafeRun(session)

  private val sessionReferenceResolver
    : DynamicVariable[Option[ReferenceResolver]] =
    new DynamicVariable(None)

  private object referenceResolver extends ReferenceResolver {
    override def setKryo(kryo: Kryo): Unit = {
      sessionReferenceResolver.value.get.setKryo(kryo)
    }

    override def getWrittenId(immutableObject: Any): ObjectReferenceId =
      sessionReferenceResolver.value.get.getWrittenId(immutableObject)
    override def addWrittenObject(immutableObject: Any): ObjectReferenceId =
      sessionReferenceResolver.value.get.addWrittenObject(immutableObject)
    override def nextReadId(clazz: Class[_]): ObjectReferenceId =
      sessionReferenceResolver.value.get.nextReadId(clazz)
    override def setReadObject(objectReferenceId: ObjectReferenceId,
                               anObject: Any): Unit = {
      sessionReferenceResolver.value.get
        .setReadObject(objectReferenceId, anObject)
    }
    override def getReadObject(clazz: Class[_],
                               objectReferenceId: ObjectReferenceId): AnyRef =
      sessionReferenceResolver.value.get
        .getReadObject(clazz, objectReferenceId)
    override def reset(): Unit = {
      // NOTE: prevent Kryo from resetting the session reference resolver as it will be
      // cached and used to resolve inter-tranche object references once a storage or
      // retrieval operation completes.
    }
    override def useReferences(clazz: Class[_]): Boolean =
      sessionReferenceResolver.value.get.useReferences(clazz)

  }

  trait ReferenceResolverContracts extends ReferenceResolver {

    abstract override def getReadObject(
        clazz: Class[_],
        objectReferenceId: ObjectReferenceId): AnyRef = {
      val result = super.getReadObject(clazz, objectReferenceId)

      val nonProxyClazz =
        if (proxySupport.isProxyClazz(clazz))
          clazz.getSuperclass
        else clazz

      assert(
        nonProxyClazz.isInstance(result) || proxySupport.kryoClosureMarkerClazz
          .isAssignableFrom(nonProxyClazz))

      result
    }
  }

  private val kryoInstantiator: KryoInstantiator =
    new EmptyScalaKryoInstantiator {
      override def newKryo(): KryoBase = {
        val result = super.newKryo()

        result.setReferenceResolver(referenceResolver)

        result.setAutoReset(true) // Kryo should reset its *own* state (but not the states of the reference resolvers) after a tranche has been stored or retrieved.

        result
      }
    }.withRegistrar { kryo =>
      // TODO - check that this is really necessary...
      kryo.register(
        proxySupport.kryoClosureMarkerClazz,
        new CleaningSerializer(
          (new ClosureSerializer).asInstanceOf[Serializer[_ <: AnyRef]]))
    }

  private val kryoPool: KryoPool =
    KryoPool.withByteArrayOutputStream(40, kryoInstantiator)

  protected def configurableProxyExclusion(clazz: Class[_]): Boolean = false

  protected val tranchesImplementationName: String

  object proxySupport extends ProxySupport {
    private val proxySuffix =
      s"delayedLoadProxyFor${tranchesImplementationName}"

    def isNotToBeProxied(clazz: Class[_]): Boolean =
      try {
        clazz.isSynthetic || clazz.isAnonymousClass || clazz.isLocalClass ||
        Modifier.isFinal(clazz.getModifiers) ||
        clazzesThatShouldNotBeProxied
          .exists(_.isAssignableFrom(clazz)) || configurableProxyExclusion(
          clazz)
      } catch {
        case _: InternalError =>
          // Workaround: https://github.com/scala/bug/issues/2034 - if it throws,
          // it's probably an inner class of some kind, so let's assume we don't
          // want to proxy it.
          true
      }

    private def createProxyClass[X <: AnyRef](clazz: Class[X]): Class[X] = {
      // We should never end up having to make chains of delegating proxies!
      require(!clazz.getName.endsWith(proxySuffix))

      byteBuddy
        .`with`(new NamingStrategy.AbstractBase {
          override def name(superClass: TypeDescription): String =
            s"${superClass.getSimpleName}_$proxySuffix"
        })
        .subclass(clazz, ConstructorStrategy.Default.NO_CONSTRUCTORS)
        .method(ElementMatchers.any().and(ElementMatchers.isPublic()))
        .intercept(MethodDelegation
          .withDefaultConfiguration()
          .withBinders(Pipe.Binder.install(classOf[PipeForwarding]))
          .to(proxyDelayedLoading))
        .defineField("acquiredState", classOf[AcquiredState])
        .implement(stateAcquisitionClazz)
        .method(ElementMatchers.named("acquire"))
        .intercept(FieldAccessor.ofField("acquiredState"))
        .implement(classOf[KryoSerializable])
        .method(
          ElementMatchers.named("write").or(ElementMatchers.named("read")))
        .intercept(MethodDelegation.to(proxySerialization))
        .make
        .load(getClass.getClassLoader, ClassLoadingStrategy.Default.INJECTION)
        .getLoaded
        .asInstanceOf[Class[X]]
    }

    private val cachedProxyClassInstantiators
      : MutableMap[Class[_ <: AnyRef], ObjectInstantiator[_ <: AnyRef]] =
      MutableMap.empty

    def createProxy(clazz: Class[_ <: AnyRef],
                    acquiredState: AcquiredState): AnyRef = {
      val proxyClassInstantiator =
        synchronized {
          cachedProxyClassInstantiators.getOrElseUpdate(clazz, {
            instantiatorStrategy.newInstantiatorOf(createProxyClass(clazz))
          })
        }

      val proxy = proxyClassInstantiator
        .newInstance()

      proxy.asInstanceOf[StateAcquisition].acquire(acquiredState)

      proxy
    }
  }

  def unsafeRun[Result](session: Session[Result])(
      tranches: Tranches[TrancheId]): EitherThrowableOr[Result] = {
    object sessionInterpreter extends FunctionK[Operation, EitherThrowableOr] {
      thisSessionInterpreter =>

      // TODO - cutover to using weak references, perhaps via 'WeakCache'?
      val completedOperationDataByTrancheId
        : MutableMap[TrancheId, CompletedOperationData] =
        MutableMap.empty

      val objectToReferenceIdMap: BiMap[AnyRef, ObjectReferenceId] =
        new BiMapUsingIdentityOnForwardMappingOnly

      val referenceIdToObjectMap: BiMap[ObjectReferenceId, AnyRef] =
        objectToReferenceIdMap.inverse()

      val proxyToReferenceIdMap: BiMap[AnyRef, ObjectReferenceId] =
        new BiMapUsingIdentityOnForwardMappingOnly

      val referenceIdToProxyMap: BiMap[ObjectReferenceId, AnyRef] =
        proxyToReferenceIdMap.inverse()

      // NOTE: a class and *not* an object; we want a fresh instance each time, as the tranche-specific
      // reference resolver uses bidirectional maps - so each value can only be associated with *one* key.
      private case class PlaceholderAssociation() extends AnyRef

      private case class AssociatedValueForAlias(immutableObject: AnyRef)
          extends AnyRef

      private def useReferences(clazz: Class[_]): Boolean =
        !clazz.isPrimitive && clazz != classOf[String]

      def retrieveObjectThatIsNotAProxy(
          objectReferenceId: ObjectReferenceId): Option[AnyRef] =
        Option(referenceIdToObjectMap.get(objectReferenceId)).map {
          case AssociatedValueForAlias(immutableObject) => immutableObject
          case immutableObject @ _                      => immutableObject
        }

      def retrieveUnderlying(trancheIdForExternalObjectReference: TrancheId,
                             objectReferenceId: ObjectReferenceId): AnyRef = {
        if (!completedOperationDataByTrancheId.contains(
              trancheIdForExternalObjectReference)) {
          val placeholderClazzForTopLevelTrancheObject = classOf[AnyRef]
          val Right(_) =
            retrieveTrancheTopLevelObject(
              trancheIdForExternalObjectReference,
              placeholderClazzForTopLevelTrancheObject)
        }

        retrieveObjectThatIsNotAProxy(objectReferenceId).get
      }

      class AcquiredState(trancheIdForExternalObjectReference: TrancheId,
                          objectReferenceId: ObjectReferenceId)
          extends proxySupport.AcquiredState {
        @transient
        private var _underlying: Option[AnyRef] = None

        override def underlying: AnyRef = _underlying match {
          case Some(result) => result
          case None =>
            val result = retrieveUnderlying(trancheIdForExternalObjectReference,
                                            objectReferenceId)

            _underlying = Some(result)

            result
        }
      }

      class TrancheSpecificReferenceResolver(
          objectReferenceIdOffset: ObjectReferenceId)
          extends ReferenceResolver {
        var numberOfAssociationsForTheRelevantTrancheOnly: ObjectReferenceId = 0

        def writtenObjectReferenceIds: Set[ObjectReferenceId] =
          (0 until numberOfAssociationsForTheRelevantTrancheOnly) map (objectReferenceIdOffset + _) toSet

        override def getWrittenId(immutableObject: AnyRef): ObjectReferenceId =
          // PLAN: if 'immutableObject' is a proxy, it *must* be found in 'proxyToReferenceIdMap'.
          // Otherwise it must be a non-proxied object in 'objectToReferenceIdMap', or it must be
          // an object not yet introduced to *any* reference resolver by either retrieval or storage.
          //  We assume that any proxy instance
          // must have already been stored in 'proxyToReferenceIdMap', and check accordingly.
          {
            val result = Option(proxyToReferenceIdMap.get(immutableObject))
              .orElse(Option(objectToReferenceIdMap.get(immutableObject)))
              .getOrElse(-1)

            if (-1 == result) {
              assert(!proxySupport.isProxy(immutableObject))
            }

            result
          }

        override def addWrittenObject(
            immutableObject: AnyRef): ObjectReferenceId = {
          require(!proxySupport.isProxy(immutableObject))

          val nextObjectReferenceIdToAllocate = numberOfAssociationsForTheRelevantTrancheOnly + objectReferenceIdOffset
          assert(nextObjectReferenceIdToAllocate >= objectReferenceIdOffset) // No wrapping around.

          val _ @None = Option(
            objectToReferenceIdMap.put(immutableObject,
                                       nextObjectReferenceIdToAllocate))

          numberOfAssociationsForTheRelevantTrancheOnly += 1

          assert(0 <= numberOfAssociationsForTheRelevantTrancheOnly) // No wrapping around.

          nextObjectReferenceIdToAllocate
        }

        override def nextReadId(clazz: Class[_]): ObjectReferenceId = {
          require(!proxySupport.isProxyClazz(clazz))

          val nextObjectReferenceIdToAllocate = numberOfAssociationsForTheRelevantTrancheOnly + objectReferenceIdOffset
          assert(nextObjectReferenceIdToAllocate >= objectReferenceIdOffset) // No wrapping around.

          val _ @None = Option(
            referenceIdToObjectMap.put(nextObjectReferenceIdToAllocate,
                                       PlaceholderAssociation()))

          numberOfAssociationsForTheRelevantTrancheOnly += 1

          assert(0 <= numberOfAssociationsForTheRelevantTrancheOnly) // No wrapping around.

          nextObjectReferenceIdToAllocate
        }

        override def setReadObject(objectReferenceId: ObjectReferenceId,
                                   immutableObject: AnyRef): Unit = {
          require(objectReferenceIdOffset <= objectReferenceId)
          require(!proxySupport.isProxy(immutableObject))

          Option(
            referenceIdToObjectMap.inverse.forcePut(immutableObject,
                                                    objectReferenceId)) match {
            case Some(aliasObjectReferenceId) =>
              val _ @None = Option(
                referenceIdToObjectMap.put(
                  aliasObjectReferenceId,
                  AssociatedValueForAlias(immutableObject)))
            case None =>
          }
        }

        override def getReadObject(
            clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef =
          // PLAN: if 'objectReferenceId' is greater than or equal to 'objectReferenceIdOffset',
          // we can resolve against the tranche associated with this reference resolver. Note that
          // we don't have to check any upper limit (and we couldn't anyway because it won't have been
          // defined yet - this call is *populating* the reference resolver), because the objects in a tranche
          // can only refer to objects in *previous* tranches with lower object reference ids. Also
          // note that we should never yield a proxy in that case.

          // Otherwise we have an inter-tranche resolution request - either yield a proxy (building it on the fly
          // if one has not already been introduced to the reference resolver), or look up an existing object
          // belonging to another tranche, loading that tranche if necessary.
          if (objectReferenceId >= objectReferenceIdOffset)
            retrieveObjectThatIsNotAProxy(objectReferenceId).get
          else
            retrieveObjectThatIsNotAProxy(objectReferenceId)
              .orElse(Option {
                referenceIdToProxyMap.get(objectReferenceId)
              })
              .getOrElse {
                val Right(trancheIdForExternalObjectReference) =
                  tranches
                    .retrieveTrancheId(objectReferenceId)

                if (proxySupport.isNotToBeProxied(clazz))
                  retrieveUnderlying(trancheIdForExternalObjectReference,
                                     objectReferenceId)
                else {
                  val proxy =
                    proxySupport.createProxy(
                      clazz.asInstanceOf[Class[_ <: AnyRef]],
                      new AcquiredState(trancheIdForExternalObjectReference,
                                        objectReferenceId))

                  referenceIdToProxyMap.put(objectReferenceId, proxy)

                  proxy
                }
              }

        override def setKryo(kryo: Kryo): Unit = {}

        override def reset(): Unit = {}

        override def useReferences(clazz: Class[_]): Boolean =
          thisSessionInterpreter.useReferences(clazz)
      }

      def retrieveTrancheTopLevelObject[X](
          trancheId: TrancheId,
          clazz: Class[X]): EitherThrowableOr[X] =
        for {
          tranche <- tranches.retrieveTranche(trancheId)
          result <- Try {
            val objectReferenceIdOffset =
              tranche.objectReferenceIdOffset
            val trancheSpecificReferenceResolver =
              new TrancheSpecificReferenceResolver(objectReferenceIdOffset)
              with ReferenceResolverContracts

            val deserialized =
              sessionReferenceResolver.withValue(
                Some(trancheSpecificReferenceResolver)) {
                kryoPool.fromBytes(tranche.payload)
              }

            completedOperationDataByTrancheId += trancheId -> CompletedOperationData(
              trancheSpecificReferenceResolver,
              deserialized)
            clazz.cast(deserialized)
          }.toEither
        } yield result

      override def apply[X](operation: Operation[X]): EitherThrowableOr[X] =
        operation match {
          case Store(immutableObject) =>
            for {
              objectReferenceIdOffsetForNewTranche <- tranches.objectReferenceIdOffsetForNewTranche
              trancheSpecificReferenceResolver = new TrancheSpecificReferenceResolver(
                objectReferenceIdOffsetForNewTranche)
              with ReferenceResolverContracts
              trancheId <- {
                val serializedRepresentation: Array[Byte] =
                  sessionReferenceResolver.withValue(
                    Some(trancheSpecificReferenceResolver)) {
                    kryoPool.toBytesWithClass(immutableObject)
                  }

                tranches
                  .createTrancheInStorage(
                    serializedRepresentation,
                    objectReferenceIdOffsetForNewTranche,
                    trancheSpecificReferenceResolver.writtenObjectReferenceIds)
              }
            } yield {
              completedOperationDataByTrancheId += trancheId -> CompletedOperationData(
                trancheSpecificReferenceResolver,
                immutableObject)
              trancheId
            }

          case retrieve @ Retrieve(trancheId, clazz) =>
            completedOperationDataByTrancheId
              .get(trancheId)
              .fold(retrieveTrancheTopLevelObject[X](trancheId, clazz)) {
                case CompletedOperationData(_, topLevelObject) =>
                  topLevelObject.asInstanceOf[X].pure[EitherThrowableOr]
              }
        }
    }

    session.foldMap(sessionInterpreter)
  }
}
