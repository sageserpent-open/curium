package com.sageserpent.plutonium.curium
import java.lang.reflect.Modifier
import java.util.{HashMap => JavaHashMap}

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.esotericsoftware.kryo.util.Util
import com.esotericsoftware.kryo.{
  Kryo,
  ReferenceResolver,
  Registration,
  Serializer
}
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.collect.{BiMap, BiMapUsingIdentityOnReverseMappingOnly}
import com.sageserpent.plutonium.{caffeineBuilder, classFromType}
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

import scala.collection.mutable
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

  trait CompletedOperation {
    def topLevelObject: Any
    def objectWithReferenceId(objectReferenceId: ObjectReferenceId): AnyRef
    def payloadSize: Int
  }

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

    val objectToReferenceIdCache: Cache[AnyRef, ObjectReferenceId] =
      CacheBuilder
        .newBuilder()
        .asInstanceOf[CacheBuilder[AnyRef, ObjectReferenceId]]
        .weakKeys()
        .build()

    def noteReferenceId(immutableObject: AnyRef,
                        objectReferenceId: ObjectReferenceId): Unit = {
      objectToReferenceIdCache.put(immutableObject, objectReferenceId)
    }

    def referenceIdFor(immutableObject: AnyRef): Option[ObjectReferenceId] =
      Option(objectToReferenceIdCache.getIfPresent(immutableObject))

    val referenceIdToProxyCache: Cache[ObjectReferenceId, AnyRef] =
      CacheBuilder
        .newBuilder()
        .asInstanceOf[CacheBuilder[ObjectReferenceId, AnyRef]]
        .weakValues()
        .build()

    def noteProxy(objectReferenceId: ObjectReferenceId,
                  immutableObject: AnyRef): Unit = {
      referenceIdToProxyCache.put(objectReferenceId, immutableObject)
    }

    def proxyFor(objectReferenceId: ObjectReferenceId) =
      Option(referenceIdToProxyCache.getIfPresent(objectReferenceId))

    val trancheIdToCompletedOperationCache
      : Cache[TrancheId, CompletedOperation] =
      CacheBuilder
        .newBuilder()
        .asInstanceOf[CacheBuilder[TrancheId, CompletedOperation]]
        .maximumSize(100)
        .build()

    def noteCompletedOperation(trancheId: TrancheId,
                               completedOperation: CompletedOperation): Unit = {
      trancheIdToCompletedOperationCache.put(trancheId, completedOperation)
    }

    def completedOperationFor(
        trancheId: TrancheId): Option[CompletedOperation] =
      Option(trancheIdToCompletedOperationCache.getIfPresent(trancheId))
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
            assert(objectReferenceIdOffsetForNewTranche <= objectReferenceId)
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
          assert(objectReferenceIdOffsetForNewTranche > objectReferenceId)
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
  }
}

trait ImmutableObjectStorage[TrancheId] {
  storage =>

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
        // NASTY HACK: extracted from overridden method in superclass, had to to this as
        // neither Kryo nor Chill's Kryobase allow customization of class registration
        // once the instance has been constructed. Sheez...

        val result = new KryoBase {
          override def getRegistration(clazz: Class[_]): Registration =
            super.getRegistration(proxySupport.nonProxyClazzFor(clazz))
        }

        result.setRegistrationRequired(false)
        result.setInstantiatorStrategy(
          new org.objenesis.strategy.StdInstantiatorStrategy)
        // ... end of nasty hack.

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

  protected def isExcludedFromBeingProxied(clazz: Class[_]): Boolean = false

  // NOTE: this is a potential danger area when an override is defined - returning
  // true indicates that all uses of a proxied object can be performed via the supertype
  // and / or interfaces. Obvious examples where this is not true would include a final
  // class that doesn't extend an interface and only has 'AnyRef' as a superclass - how
  // would client code do something useful with it? Scala case classes that are declared
  // as final and form a union type hierarchy that pattern matching is performed on will
  // also fail. The reason why this exists at all is to provide as an escape hatch for
  // the multitude of Scala case classes declared as final that are actually used as part
  // of an object-oriented interface hierarchy - the collection classes being the main
  // offenders in that regard.
  protected def canBeProxiedViaSuperTypes(clazz: Class[_]): Boolean = false

  protected val tranchesImplementationName: String

  private def useReferences(clazz: Class[_]): Boolean =
    !Util.isWrapperClass(clazz) &&
      clazz != classOf[String]

  object proxySupport extends ProxySupport {
    case class SuperClazzAndInterfaces(superClazz: Class[_],
                                       interfaces: Seq[Class[_]])

    val superClazzAndInterfacesCache
      : Cache[Class[_], Option[SuperClazzAndInterfaces]] =
      CacheBuilder
        .newBuilder()
        .asInstanceOf[CacheBuilder[Class[_], Option[SuperClazzAndInterfaces]]]
        .build()

    private def shouldNotBeProxiedAsItsOwnType(clazz: Class[_]): Boolean =
      Modifier.isFinal(clazz.getModifiers) ||
        clazz.isSynthetic ||
        (try {
          clazz.isAnonymousClass ||
          clazz.isLocalClass
        } catch {
          case _: InternalError =>
            // Workaround: https://github.com/scala/bug/issues/2034 - if it throws,
            // it's probably an inner class of some kind.
            true
        })

    def superClazzAndInterfacesToProxy(
        clazz: Class[_]): Option[SuperClazzAndInterfaces] =
      superClazzAndInterfacesCache.get(
        clazz, { () =>
          require(!isProxyClazz(clazz))

          val clazzShouldNotBeProxiedAtAll = kryoClosureMarkerClazz.isAssignableFrom(
            clazz) || !useReferences(clazz) || isExcludedFromBeingProxied(clazz)

          if (!clazzShouldNotBeProxiedAtAll)
            if (shouldNotBeProxiedAsItsOwnType(clazz))
              if (canBeProxiedViaSuperTypes(clazz)) {
                val superClazz = clazz.getSuperclass
                if (shouldNotBeProxiedAsItsOwnType(superClazz))
                  superClazzAndInterfacesToProxy(superClazz).map(
                    superClazzAndInterfaces =>
                      superClazzAndInterfaces.copy(
                        interfaces = superClazzAndInterfaces.interfaces ++ clazz.getInterfaces))
                else
                  Some(
                    SuperClazzAndInterfaces(clazz.getSuperclass,
                                            clazz.getInterfaces))
              } else None
            else Some(SuperClazzAndInterfaces(clazz, Seq.empty))
          else None
        }
      )

    def canBeProxied[Result](immutableObject: AnyRef) =
      superClazzAndInterfacesToProxy(immutableObject.getClass).isDefined

    private val proxySuffix =
      s"delayedLoadProxyFor${tranchesImplementationName}"

    private implicit val superClazzBagConfiguration =
      mutable.HashedBagConfiguration.compact[TypeDescription]

    private val superClazzBag: mutable.HashBag[TypeDescription] =
      mutable.HashBag.empty

    private def createProxyClass(
        superClazzAndInterfaces: SuperClazzAndInterfaces): Class[_] = {
      // We should never end up having to make chains of delegating proxies!
      require(!isProxyClazz(superClazzAndInterfaces.superClazz))

      byteBuddy
        .`with`(new NamingStrategy.AbstractBase {
          override def name(superClass: TypeDescription): String = {
            superClazzBag += superClass
            s"${superClass.getSimpleName}_${superClazzBag(superClass).size}_$proxySuffix"
          }
        })
        .subclass(superClazzAndInterfaces.superClazz,
                  ConstructorStrategy.Default.NO_CONSTRUCTORS)
        .method(ElementMatchers.isPublic())
        .intercept(MethodDelegation
          .withDefaultConfiguration()
          .withBinders(Pipe.Binder.install(classOf[PipeForwarding]))
          .to(proxyDelayedLoading))
        .implement(superClazzAndInterfaces.interfaces: _*)
        .method(ElementMatchers.isPublic())
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
    }

    val cachedProxyClassInstantiators
      : Cache[SuperClazzAndInterfaces, ObjectInstantiator[_]] =
      CacheBuilder
        .newBuilder()
        .asInstanceOf[CacheBuilder[SuperClazzAndInterfaces,
                                   ObjectInstantiator[_]]]
        .build()

    val proxiedClazzCache: Cache[Class[_], Class[_]] =
      CacheBuilder
        .newBuilder()
        .asInstanceOf[CacheBuilder[Class[_], Class[_]]]
        .build()

    def createProxy(clazz: Class[_], acquiredState: AcquiredState): AnyRef = {
      val proxyClassInstantiator =
        synchronized {
          val superClazzAndInterfaces =
            superClazzAndInterfacesToProxy(clazz).get
          cachedProxyClassInstantiators.get(
            // TODO: there should be a test that fails if we just consult 'superClazzAndInterfacesCache'
            // rather than ensuring that it is populated as is being done here, or at least proves that it
            // is populated beforehand elsewhere. Specifically, what happens when a tranche is loaded into
            // a session where a proxy class has not already been created?
            superClazzAndInterfaces, { () =>
              val proxyClazz = createProxyClass(superClazzAndInterfaces)

              proxiedClazzCache.put(proxyClazz, clazz)
              instantiatorStrategy.newInstantiatorOf(proxyClazz)
            }
          )
        }

      val proxy = proxyClassInstantiator
        .newInstance()

      proxy.asInstanceOf[StateAcquisition].acquire(acquiredState)

      proxy.asInstanceOf[AnyRef]
    }

    def nonProxyClazzFor(clazz: Class[_]): Class[_] =
      Option(proxiedClazzCache.getIfPresent(clazz)).getOrElse(clazz)
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
          require(!proxySupport.isProxyClazz(clazz))

          val result = super.getReadObject(clazz, objectReferenceId)

          assert((proxySupport.superClazzAndInterfacesToProxy(clazz) match {
            case Some(
                proxySupport.SuperClazzAndInterfaces(superClazz, interfaces)) =>
              superClazz.isInstance(result) && interfaces.forall(
                _.isInstance(result))
            case None =>
              clazz
                .isInstance(result)
          }) || proxySupport.kryoClosureMarkerClazz
            .isAssignableFrom(clazz))

          result
        }
      }

      class CompleteOperationImplementation(
          override val topLevelObject: Any,
          trancheSpecificReferenceResolver: TrancheSpecificReferenceResolver,
          override val payloadSize: Int)
          extends CompletedOperation {
        override def objectWithReferenceId(
            objectReferenceId: ObjectReferenceId): AnyRef =
          trancheSpecificReferenceResolver
            .objectWithReferenceId(objectReferenceId)
      }

      private case class AssociatedValueForAlias(immutableObject: AnyRef)
          extends AnyRef

      def decodePlaceholder(placeholderOrActualObject: AnyRef): AnyRef =
        placeholderOrActualObject match {
          case AssociatedValueForAlias(immutableObject) => immutableObject
          case immutableObject @ _                      => immutableObject
        }

      def retrieveUnderlying(trancheIdForExternalObjectReference: TrancheId,
                             objectReferenceId: ObjectReferenceId): AnyRef =
        tranches
          .completedOperationFor(trancheIdForExternalObjectReference)
          .orElse {
            val placeholderClazzForTopLevelTrancheObject = classOf[AnyRef]
            val Right(_) =
              retrieveTrancheTopLevelObject(
                trancheIdForExternalObjectReference,
                placeholderClazzForTopLevelTrancheObject)

            tranches.completedOperationFor(trancheIdForExternalObjectReference)
          }
          .get
          .objectWithReferenceId(objectReferenceId)

      class AcquiredState(trancheIdForExternalObjectReference: TrancheId,
                          objectReferenceId: ObjectReferenceId)
          extends proxySupport.AcquiredState {
        private var _underlying: Option[AnyRef] = None

        override def underlying: AnyRef = _underlying match {
          case Some(result) => result
          case None =>
            val result =
              retrieveUnderlying(trancheIdForExternalObjectReference,
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

        private val referenceIdToObjectMap: BiMap[ObjectReferenceId, AnyRef] =
          BiMapUsingIdentityOnReverseMappingOnly.fromForwardMap(
            new JavaHashMap())

        def writtenObjectReferenceIds: Set[ObjectReferenceId] =
          (0 until numberOfAssociationsForTheRelevantTrancheOnly) map (objectReferenceIdOffset + _) toSet

        def objectWithReferenceId(
            objectReferenceId: ObjectReferenceId): AnyRef =
          Option(referenceIdToObjectMap.get(objectReferenceId))
            .map(decodePlaceholder)
            .get

        override def getWrittenId(
            immutableObject: AnyRef): ObjectReferenceId = {
          (if (proxySupport.isProxy(immutableObject) || proxySupport
                 .canBeProxied(immutableObject))
             tranches
               .referenceIdFor(immutableObject)
           else
             Option(referenceIdToObjectMap.inverse().get(immutableObject)))
            .getOrElse(-1)
        }

        override def addWrittenObject(
            immutableObject: AnyRef): ObjectReferenceId = {
          val nextObjectReferenceIdToAllocate = numberOfAssociationsForTheRelevantTrancheOnly + objectReferenceIdOffset
          assert(nextObjectReferenceIdToAllocate >= objectReferenceIdOffset) // No wrapping around.

          val _ @None = Option(
            referenceIdToObjectMap
              .put(nextObjectReferenceIdToAllocate, immutableObject))

          if (proxySupport.canBeProxied(immutableObject)) {
            tranches.noteReferenceId(immutableObject,
                                     nextObjectReferenceIdToAllocate)
          }

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
            referenceIdToObjectMap
              .inverse()
              .forcePut(immutableObject, objectReferenceId)) match {
            case Some(aliasObjectReferenceId) =>
              val associatedValueForAlias =
                AssociatedValueForAlias(immutableObject)
              val _ @None = Option(
                referenceIdToObjectMap
                  .put(aliasObjectReferenceId, associatedValueForAlias))
            case None =>
          }

          if (proxySupport.canBeProxied(immutableObject)) {
            tranches.noteReferenceId(immutableObject, objectReferenceId)
          }
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
            objectWithReferenceId(objectReferenceId)
          else
            tranches.proxyFor(objectReferenceId).getOrElse {
              val Right(trancheIdForExternalObjectReference) =
                tranches
                  .retrieveTrancheId(objectReferenceId)

              val proxy =
                proxySupport.createProxy(
                  clazz,
                  new AcquiredState(trancheIdForExternalObjectReference,
                                    objectReferenceId))

              tranches.noteReferenceId(proxy, objectReferenceId)
              tranches.noteProxy(objectReferenceId, proxy)

              proxy
            }
        }

        override def setKryo(kryo: Kryo): Unit = {}

        override def reset(): Unit = {}

        override def useReferences(clazz: Class[_]): Boolean =
          storage.useReferences(clazz)
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

            tranches.noteCompletedOperation(trancheId,
                                            new CompleteOperationImplementation(
                                              deserialized,
                                              trancheSpecificReferenceResolver,
                                              tranche.payload.length))

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
              serializedRepresentation: Array[Byte] = sessionReferenceResolver
                .withValue(Some(trancheSpecificReferenceResolver)) {
                  kryoPool.toBytesWithClass(immutableObject)
                }
              trancheId <- tranches
                .createTrancheInStorage(
                  serializedRepresentation,
                  objectReferenceIdOffsetForNewTranche,
                  trancheSpecificReferenceResolver.writtenObjectReferenceIds)
            } yield {
              tranches.noteCompletedOperation(
                trancheId,
                new CompleteOperationImplementation(
                  immutableObject,
                  trancheSpecificReferenceResolver,
                  serializedRepresentation.length))

              trancheId
            }

          case retrieve @ Retrieve(trancheId, clazz) =>
            tranches
              .completedOperationFor(trancheId)
              .map(_.topLevelObject)
              .fold {
                for {
                  topLevelObject <- retrieveTrancheTopLevelObject[X](trancheId,
                                                                     clazz)
                } yield topLevelObject

              }(_.asInstanceOf[X].pure[EitherThrowableOr])
        }
    }

    session.foldMap(sessionInterpreter)
  }
}
