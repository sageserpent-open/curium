package com.sageserpent.plutonium.curium
import java.lang.reflect.Modifier
import java.util.concurrent.TimeUnit
import java.util.{HashMap => JavaHashMap}

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.esotericsoftware.kryo.util.Util
import com.esotericsoftware.kryo.{Kryo, ReferenceResolver, Serializer}
import com.github.benmanes.caffeine.cache.Caffeine
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
  RuntimeType
}
import net.bytebuddy.implementation.{FieldAccessor, MethodDelegation}
import net.bytebuddy.matcher.ElementMatchers
import net.bytebuddy.{ByteBuddy, NamingStrategy}
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.strategy.StdInstantiatorStrategy
import scalacache._
import scalacache.caffeine._
import scalacache.memoization._
import scalacache.modes.sync._

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

    def noteObject(objectReferenceId: ObjectReferenceId,
                   immutableObject: AnyRef): Unit

    def objectFor(objectReferenceId: ObjectReferenceId): Option[AnyRef]

    def noteReferenceId(immutableObject: AnyRef,
                        objectReferenceId: ObjectReferenceId): Unit

    def referenceIdFor(immutableObject: AnyRef): Option[ObjectReferenceId]

    def noteTopLevelObject(trancheId: TrancheId, topLevelObject: AnyRef): Unit

    def topLevelObjectFor(trancheId: TrancheId): Option[AnyRef]
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

    /*
      This is tracked to workaround Kryo leaking its internal fudge
      as to how it registers closure serializers into calls on the
      tranche specific reference resolver class' methods.
     */
    val kryoClosureMarkerClazz = classOf[Closure]

    val clazzesThatShouldNotBeProxied: Set[Class[_]] =
      Set(
        classOf[String]
      )

    val stateAcquisitionClazz = classOf[StateAcquisition]

    def isProxyClazz(clazz: Class[_]): Boolean =
      stateAcquisitionClazz.isAssignableFrom(clazz)

    def isProxy(immutableObject: AnyRef): Boolean =
      stateAcquisitionClazz.isInstance(immutableObject)

    def nonProxyClazzFor(clazz: Class[_]): Class[_] =
      if (isProxyClazz(clazz))
        clazz.getSuperclass
      else clazz

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
    private val cacheTimeToLive = None

    private val memoizationConfig = MemoizationConfig(
      (fullClassName: String,
       constructorParameters: IndexedSeq[IndexedSeq[Any]],
       methodName: String,
       parameters: IndexedSeq[IndexedSeq[Any]]) =>
        parameters.head.head.toString)

    private implicit val isNotToBeProxiedCache: Cache[Boolean] =
      CaffeineCache[Boolean](
        Caffeine
          .newBuilder()
          .maximumSize(200L)
          .expireAfterAccess(1, TimeUnit.MINUTES)
          .build[String, Entry[Boolean]])(
        CacheConfig.defaultCacheConfig.copy(memoization = memoizationConfig))

    def isNotToBeProxied(clazz: Class[_]): Boolean =
      memoizeSync(cacheTimeToLive) {
        require(!isProxyClazz(clazz))

        kryoClosureMarkerClazz.isAssignableFrom(clazz) ||
        clazz.isSynthetic || (try {
          clazz.isAnonymousClass ||
          clazz.isLocalClass
        } catch {
          case _: InternalError =>
            // Workaround: https://github.com/scala/bug/issues/2034 - if it throws,
            // it's probably an inner class of some kind.
            true
        }) ||
        configurableProxyExclusion(clazz) ||
        Modifier.isFinal(clazz.getModifiers) ||
        clazzesThatShouldNotBeProxied.exists(_.isAssignableFrom(clazz))
      }

    private val proxySuffix =
      s"delayedLoadProxyFor${tranchesImplementationName}"

    private def createProxyClass[X <: AnyRef](clazz: Class[X]): Class[X] = {
      // We should never end up having to make chains of delegating proxies!
      require(!isProxyClazz(clazz))

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

      trait ReferenceResolverContracts extends ReferenceResolver {

        abstract override def getWrittenId(
            immutableObject: AnyRef): ObjectReferenceId = {
          val result = super.getWrittenId(immutableObject)

          if (-1 == result) {
            assert(!proxySupport.isProxy(immutableObject))
          }

          result
        }

        abstract override def addWrittenObject(
            immutableObject: AnyRef): ObjectReferenceId = {
          require(!proxySupport.isProxy(immutableObject))

          super.addWrittenObject(immutableObject)
        }

        abstract override def nextReadId(clazz: Class[_]): ObjectReferenceId = {
          require(!proxySupport.isProxyClazz(clazz))

          super.nextReadId(clazz)
        }

        abstract override def setReadObject(
            objectReferenceId: ObjectReferenceId,
            immutableObject: AnyRef): Unit = {
          require(!proxySupport.isProxy(immutableObject))

          super.setReadObject(objectReferenceId, immutableObject)
        }

        abstract override def getReadObject(
            clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef = {
          val result = super.getReadObject(clazz, objectReferenceId)

          val nonProxyClazz =
            proxySupport.nonProxyClazzFor(clazz)

          assert(
            nonProxyClazz
              .isInstance(result) || proxySupport.kryoClosureMarkerClazz
              .isAssignableFrom(nonProxyClazz))

          result
        }
      }

      // TODO - cutover to using weak references, perhaps via 'WeakCache'?
      val completedOperationDataByTrancheId
        : MutableMap[TrancheId, CompletedOperationData] =
        MutableMap.empty

      val objectToReferenceIdMap: BiMap[AnyRef, ObjectReferenceId] =
        BiMapUsingIdentityOnForwardMappingOnly.fromReverseMap(
          new JavaHashMap[ObjectReferenceId, AnyRef]())

      val referenceIdToObjectMap: BiMap[ObjectReferenceId, AnyRef] =
        objectToReferenceIdMap.inverse()

      val proxyToReferenceIdMap: BiMap[AnyRef, ObjectReferenceId] =
        BiMapUsingIdentityOnForwardMappingOnly.fromReverseMap(
          new JavaHashMap[ObjectReferenceId, AnyRef]())

      val referenceIdToProxyMap: BiMap[ObjectReferenceId, AnyRef] =
        proxyToReferenceIdMap.inverse()

      private case class AssociatedValueForAlias(immutableObject: AnyRef)
          extends AnyRef

      private def useReferences(clazz: Class[_]): Boolean =
        !Util.isWrapperClass(clazz) &&
          clazz != classOf[String]

      def decodePlaceholder(placeholderOrActualObject: AnyRef): AnyRef =
        placeholderOrActualObject match {
          case AssociatedValueForAlias(immutableObject) => immutableObject
          case immutableObject @ _                      => immutableObject
        }

      def objectWithReferenceId(
          objectReferenceId: ObjectReferenceId): Option[AnyRef] =
        tranches.objectFor(objectReferenceId).orElse {
          Option(referenceIdToObjectMap.get(objectReferenceId))
            .map(decodePlaceholder)
        }

      def retrieveUnderlying(trancheIdForExternalObjectReference: TrancheId,
                             objectReferenceId: ObjectReferenceId): AnyRef =
        objectWithReferenceId(objectReferenceId).orElse {
          if (!completedOperationDataByTrancheId.contains(
                trancheIdForExternalObjectReference)) {
            val placeholderClazzForTopLevelTrancheObject = classOf[AnyRef]
            val Right(_) =
              retrieveTrancheTopLevelObject(
                trancheIdForExternalObjectReference,
                placeholderClazzForTopLevelTrancheObject)
          }

          objectWithReferenceId(objectReferenceId)
        }.get

      class AcquiredState(trancheIdForExternalObjectReference: TrancheId,
                          objectReferenceId: ObjectReferenceId)
          extends proxySupport.AcquiredState {
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
        private var numberOfAssociationsForTheRelevantTrancheOnly
          : ObjectReferenceId = 0

        def writtenObjectReferenceIds: Set[ObjectReferenceId] =
          (0 until numberOfAssociationsForTheRelevantTrancheOnly) map (objectReferenceIdOffset + _) toSet

        override def getWrittenId(immutableObject: AnyRef): ObjectReferenceId =
          // PLAN: if 'immutableObject' is a proxy, it *must* be found in 'proxyToReferenceIdMap'.
          // Otherwise it must be a non-proxied object in 'objectToReferenceIdMap', or it must be
          // an object not yet introduced to *any* reference resolver by either retrieval or storage.
          // We assume that any proxy instance must have already been stored in 'proxyToReferenceIdMap',
          // and check accordingly.
          tranches
            .referenceIdFor(immutableObject)
            .orElse(
              Option(proxyToReferenceIdMap.get(immutableObject))
                .orElse(Option(objectToReferenceIdMap
                  .get(immutableObject))))
            .getOrElse(-1)

        override def addWrittenObject(
            immutableObject: AnyRef): ObjectReferenceId = {
          val nextObjectReferenceIdToAllocate = numberOfAssociationsForTheRelevantTrancheOnly + objectReferenceIdOffset
          assert(nextObjectReferenceIdToAllocate >= objectReferenceIdOffset) // No wrapping around.

          val _ @None = Option(
            objectToReferenceIdMap
              .put(immutableObject, nextObjectReferenceIdToAllocate))

          tranches.noteObject(nextObjectReferenceIdToAllocate, immutableObject)
          tranches.noteReferenceId(immutableObject,
                                   nextObjectReferenceIdToAllocate)

          numberOfAssociationsForTheRelevantTrancheOnly += 1

          assert(0 <= numberOfAssociationsForTheRelevantTrancheOnly) // No wrapping around.

          nextObjectReferenceIdToAllocate
        }

        override def nextReadId(clazz: Class[_]): ObjectReferenceId = {
          val nextObjectReferenceIdToAllocate = numberOfAssociationsForTheRelevantTrancheOnly + objectReferenceIdOffset
          assert(nextObjectReferenceIdToAllocate >= objectReferenceIdOffset) // No wrapping around.

          numberOfAssociationsForTheRelevantTrancheOnly += 1

          assert(0 <= numberOfAssociationsForTheRelevantTrancheOnly) // No wrapping around.

          nextObjectReferenceIdToAllocate
        }

        override def setReadObject(objectReferenceId: ObjectReferenceId,
                                   immutableObject: AnyRef): Unit = {
          require(objectReferenceIdOffset <= objectReferenceId)

          Option(
            referenceIdToObjectMap.inverse
              .forcePut(immutableObject, objectReferenceId)) match {
            case Some(aliasObjectReferenceId) =>
              val associatedValueForAlias = AssociatedValueForAlias(
                immutableObject)
              val _ @None = Option(
                referenceIdToObjectMap.put(aliasObjectReferenceId,
                                           associatedValueForAlias))
              tranches.noteObject(aliasObjectReferenceId,
                                  associatedValueForAlias)
            case None =>
          }

          tranches.noteObject(objectReferenceId, immutableObject)
          tranches.noteReferenceId(immutableObject, objectReferenceId)
        }

        override def getReadObject(
            clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef = {
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
            objectWithReferenceId(objectReferenceId).get
          else
            objectWithReferenceId(objectReferenceId)
              .orElse(Option {
                referenceIdToProxyMap.get(objectReferenceId)
              })
              .getOrElse {
                val Right(trancheIdForExternalObjectReference) =
                  tranches
                    .retrieveTrancheId(objectReferenceId)

                val nonProxyClazz =
                  proxySupport.nonProxyClazzFor(clazz)

                if (proxySupport.isNotToBeProxied(nonProxyClazz))
                  retrieveUnderlying(trancheIdForExternalObjectReference,
                                     objectReferenceId)
                else {
                  val proxy =
                    proxySupport.createProxy(
                      nonProxyClazz.asInstanceOf[Class[_ <: AnyRef]],
                      new AcquiredState(trancheIdForExternalObjectReference,
                                        objectReferenceId))

                  referenceIdToProxyMap.put(objectReferenceId, proxy)

                  tranches.noteReferenceId(proxy, objectReferenceId)

                  proxy
                }
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
            tranches
              .topLevelObjectFor(trancheId)
              .orElse(
                completedOperationDataByTrancheId
                  .get(trancheId)
                  .map(_.topLevelObject))
              .fold {
                for {
                  topLevelObject <- retrieveTrancheTopLevelObject[X](trancheId,
                                                                     clazz)
                } yield {
                  tranches.noteTopLevelObject(
                    trancheId,
                    topLevelObject.asInstanceOf[AnyRef])
                  topLevelObject
                }
              }(_.asInstanceOf[X].pure[EitherThrowableOr])
        }
    }

    session.foldMap(sessionInterpreter)
  }
}
