package com.sageserpent.plutonium.curium
import java.lang.reflect.Modifier
import java.util.UUID

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.ClosureSerializer
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
  KryoBase,
  KryoInstantiator,
  KryoPool,
  ScalaKryoInstantiator
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
import scalacache._
import scalacache.caffeine._
import scalacache.memoization._
import scalacache.modes.sync._

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable.{
  Map => MutableMap,
  SortedMap => MutableSortedMap
}
import scala.concurrent.duration._
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.util.{DynamicVariable, Try}

object ImmutableObjectStorage {
  type TrancheId = UUID

  type ObjectReferenceId = Int

  case class TrancheOfData(serializedRepresentation: Array[Byte],
                           objectReferenceIdOffset: ObjectReferenceId)

  type EitherThrowableOr[X] = Either[Throwable, X]

  trait Tranches {
    // TODO: either move 'createTrancheInStorage' into 'ImmutableObjectStorage', making tranche ids the responsibility of said class,
    //  or go the other way and delegate the creation of the tranche id to the implementing subclass, so that say, a database backend
    // can automatically generate the tranche ids as primary keys. The current state of affairs works, but seems to be sitting on the fence
    // regarding tranche id responsibility.

    def createTrancheInStorage(serializedRepresentation: Array[Byte],
                               objectReferenceIdOffset: ObjectReferenceId,
                               objectReferenceIds: Seq[ObjectReferenceId])
      : EitherThrowableOr[TrancheId] = {
      require(
        objectReferenceIds.isEmpty || objectReferenceIdOffset <= objectReferenceIds.min)

      val id = UUID.randomUUID()

      val tranche =
        TrancheOfData(serializedRepresentation, objectReferenceIdOffset)
      for {
        _ <- storeTrancheAndAssociatedObjectReferenceIds(id,
                                                         tranche,
                                                         objectReferenceIds)
      } yield id
    }

    protected def storeTrancheAndAssociatedObjectReferenceIds(
        trancheId: TrancheId,
        tranche: TrancheOfData,
        objectReferenceIds: Seq[ObjectReferenceId]): EitherThrowableOr[Unit]

    def objectReferenceIdOffsetForNewTranche
      : EitherThrowableOr[ObjectReferenceId]

    def retrieveTranche(trancheId: TrancheId): EitherThrowableOr[TrancheOfData]
    def retrieveTrancheId(
        objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId]
  }

  trait Operation[Result]

  case class Store[X](immutableObject: X) extends Operation[TrancheId]

  case class Retrieve[X](trancheId: TrancheId, clazz: Class[X])
      extends Operation[X]

  type Session[X] = FreeT[Operation, EitherThrowableOr, X]

  def store[X](immutableObject: X): Session[TrancheId] =
    FreeT.liftF[Operation, EitherThrowableOr, TrancheId](Store(immutableObject))

  def retrieve[X: TypeTag](id: TrancheId): Session[X] =
    FreeT.liftF[Operation, EitherThrowableOr, X](
      Retrieve(id, classFromType(typeOf[X])))

  def runToYieldTrancheIds(session: Session[Vector[TrancheId]])
    : Tranches => EitherThrowableOr[Vector[TrancheId]] =
    unsafeRun(session)

  def runToYieldTrancheId(
      session: Session[TrancheId]): Tranches => EitherThrowableOr[TrancheId] =
    unsafeRun(session)

  def runForEffectsOnly(
      session: Session[Unit]): Tranches => EitherThrowableOr[Unit] =
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
    new ScalaKryoInstantiator {
      override def newKryo(): KryoBase = {
        val result = super.newKryo()

        result.setReferenceResolver(referenceResolver)

        result.setAutoReset(true) // Kryo should reset its *own* state (but not the states of the reference resolvers) after a tranche has been stored or retrieved.

        result
      }
    }.withRegistrar(
      kryo =>
        // TODO - check that this is really necessary...
        kryo.register(
          classOf[ClosureSerializer.Closure],
          new CleaningSerializer(
            (new ClosureSerializer).asInstanceOf[Serializer[_ <: AnyRef]])))

  private val kryoPool: KryoPool =
    KryoPool.withByteArrayOutputStream(40, kryoInstantiator)

  object proxySupport {
    private val byteBuddy = new ByteBuddy()

    private val proxySuffix = "delayedLoadProxy"

    trait AcquiredState {
      def underlying: AnyRef
    }

    private[curium] trait StateAcquisition {
      def acquire(acquiredState: AcquiredState): Unit
    }

    val clazzesThatShouldNotBeProxied: Set[Class[_]] =
      Set(classOf[StateAcquisition], classOf[String], classOf[Class[_]])

    def isNotToBeProxied(clazz: Class[_]): Boolean =
      // Start with a workaround for: https://github.com/scala/bug/issues/2034 - if it throws,
      // it's probably an inner class of some kind, so let's assume we don't want to proxy it.
      Try { clazz.getSimpleName }.isFailure ||
        clazz.isSynthetic || clazz.isAnonymousClass || clazz.isLocalClass ||
        Modifier.isFinal(clazz.getModifiers) || clazzesThatShouldNotBeProxied
        .exists(_.isAssignableFrom(clazz)) || clazz.getSimpleName.startsWith(
        "Function") || clazz.getSimpleName.startsWith("Tuple")

    private def createProxyClass[X <: AnyRef](clazz: Class[X]): Class[X] = {
      // We should never end up having to make chains of delegating proxies!
      require(!clazz.getSimpleName.endsWith(proxySuffix))

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
        def read(kryo: Kryo,
                 input: Input,
                 @This thiz: StateAcquisition): Unit = {
          thiz.acquire(
            kryo.readClassAndObject(input).asInstanceOf[AcquiredState])
        }
      }

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
        .implement(classOf[StateAcquisition])
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

    private val instantiatorStrategy: StdInstantiatorStrategy =
      new StdInstantiatorStrategy

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
      tranches: Tranches): EitherThrowableOr[Result] = {
    object sessionInterpreter extends FunctionK[Operation, EitherThrowableOr] {
      thisSessionInterpreter =>

      // TODO - cutover to using weak references, perhaps via 'WeakCache'?
      val completedOperationDataByTrancheId
        : MutableSortedMap[TrancheId, CompletedOperationData] =
        MutableSortedMap.empty

      private val referenceResolverCacheTimeToLive = Some(10 minutes)

      // TODO: if the keys are identity hash codes, what's going to stop the original object
      // from being garbage collected and another one taking the same identity hash code? Oh dear....
      private val objectReferenceIdCache: Cache[ObjectReferenceId] =
        CaffeineCache[ObjectReferenceId](
          CacheConfig.defaultCacheConfig.copy(
            memoization = MemoizationConfig(
              (fullClassName: String,
               constructorParameters: IndexedSeq[IndexedSeq[Any]],
               methodName: String,
               parameters: IndexedSeq[IndexedSeq[Any]]) =>
                System.identityHashCode(parameters.head).toString)))

      private val objectCache: Cache[AnyRef] = CaffeineCache[AnyRef](
        CacheConfig.defaultCacheConfig.copy(
          memoization = MemoizationConfig(
            (fullClassName: String,
             constructorParameters: IndexedSeq[IndexedSeq[Any]],
             methodName: String,
             parameters: IndexedSeq[IndexedSeq[Any]]) =>
              parameters.head.toString)))

      val proxyToReferenceIdMap: BiMap[AnyRef, ObjectReferenceId] =
        new BiMapUsingIdentityOnForwardMappingOnly

      val referenceIdToProxyMap: BiMap[ObjectReferenceId, AnyRef] =
        proxyToReferenceIdMap.inverse()

      // NOTE: a class and *not* an object; we want a fresh instance each time, as the tranche-specific
      // reference resolver uses bidirectional maps - so each value can only be associated with *one* key.
      class PlaceholderAssociatedWithFreshObjectReferenceId

      private def useReferences(clazz: Class[_]): Boolean =
        !clazz.isPrimitive && clazz != classOf[String]

      def retrieveUnderlying(trancheIdForExternalObjectReference: TrancheId,
                             objectReferenceId: ObjectReferenceId,
                             clazz: Class[_ <: AnyRef]): AnyRef = {
        if (!completedOperationDataByTrancheId.contains(
              trancheIdForExternalObjectReference)) {
          val _ =
            thisSessionInterpreter(
              Retrieve(trancheIdForExternalObjectReference, clazz))
        }

        completedOperationDataByTrancheId(trancheIdForExternalObjectReference).referenceResolver
          .getReadObjectConsultingOnlyThisTranche(objectReferenceId)
      }

      class AcquiredState(trancheIdForExternalObjectReference: TrancheId,
                          objectReferenceId: ObjectReferenceId,
                          clazz: Class[_ <: AnyRef])
          extends proxySupport.AcquiredState {
        @transient
        private var _underlying: Option[AnyRef] = None

        override def underlying: AnyRef = _underlying match {
          case Some(result) => result
          case None =>
            val result = retrieveUnderlying(trancheIdForExternalObjectReference,
                                            objectReferenceId,
                                            clazz)

            _underlying = Some(result)

            result
        }
      }

      class TrancheSpecificReferenceResolver(
          objectReferenceIdOffset: ObjectReferenceId)
          extends ReferenceResolver {
        val objectToReferenceIdMap: BiMap[AnyRef, ObjectReferenceId] =
          new BiMapUsingIdentityOnForwardMappingOnly

        val referenceIdToObjectMap: BiMap[ObjectReferenceId, AnyRef] =
          objectToReferenceIdMap.inverse()

        def writtenObjectReferenceIds: immutable.IndexedSeq[ObjectReferenceId] =
          (0 until objectToReferenceIdMap.size) map (objectReferenceIdOffset + _)

        override def getWrittenId(immutableObject: AnyRef): ObjectReferenceId =
          memoizeSync(referenceResolverCacheTimeToLive) {
            val resultFromExSessionReferenceResolver =
              completedOperationDataByTrancheId.view
                .map {
                  case (_, CompletedOperationData(referenceResolver, _)) =>
                    referenceResolver.getWrittenIdConsultingOnlyThisTranche(
                      immutableObject)
                }
                .find(-1 != _)
            resultFromExSessionReferenceResolver
              .orElse(Option(proxyToReferenceIdMap.get(immutableObject))) // TODO - why isn't this consulted first?
              .getOrElse(getWrittenIdConsultingOnlyThisTranche(immutableObject))
          }(objectReferenceIdCache, mode, implicitly[Flags])

        private def getWrittenIdConsultingOnlyThisTranche(
            immutableObject: AnyRef): ObjectReferenceId =
          Option(objectToReferenceIdMap.get(immutableObject)).getOrElse(-1)

        override def addWrittenObject(
            immutableObject: AnyRef): ObjectReferenceId = {
          val nextObjectReferenceIdToAllocate = objectToReferenceIdMap.size + objectReferenceIdOffset
          val _ @None = Option(
            objectToReferenceIdMap.putIfAbsent(immutableObject,
                                               nextObjectReferenceIdToAllocate))
          nextObjectReferenceIdToAllocate
        }

        override def nextReadId(clazz: Class[_]): ObjectReferenceId = {
          val nextObjectReferenceIdToAllocate = referenceIdToObjectMap.size + objectReferenceIdOffset
          val _ @None = Option(
            referenceIdToObjectMap.putIfAbsent(
              nextObjectReferenceIdToAllocate,
              new PlaceholderAssociatedWithFreshObjectReferenceId))
          nextObjectReferenceIdToAllocate
        }

        override def setReadObject(objectReferenceId: ObjectReferenceId,
                                   immutableObject: AnyRef): Unit = {
          require(objectReferenceIdOffset <= objectReferenceId)
          val debugging =
            referenceIdToObjectMap.put(objectReferenceId, immutableObject)
          assert(null == debugging || debugging
            .isInstanceOf[PlaceholderAssociatedWithFreshObjectReferenceId] || debugging == immutableObject)
        }

        override def getReadObject(
            @cacheKeyExclude clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef =
          if (objectReferenceId >= objectReferenceIdOffset)
            getReadObjectConsultingOnlyThisTranche(objectReferenceId)
          else
            memoizeSync(referenceResolverCacheTimeToLive) {
              val Right(trancheIdForExternalObjectReference) =
                tranches
                  .retrieveTrancheId(objectReferenceId)

              val resultFromCompletedReferenceResolver = completedOperationDataByTrancheId
                .get(trancheIdForExternalObjectReference) map (_.referenceResolver
                .getReadObjectConsultingOnlyThisTranche(objectReferenceId))

              val resultFromExistingAssociation =
                resultFromCompletedReferenceResolver.orElse {
                  Option(referenceIdToProxyMap.get(objectReferenceId))
                }

              resultFromExistingAssociation.getOrElse {
                val clazzWithAppropriateUpperBoundType =
                  clazz.asInstanceOf[Class[_ <: AnyRef]]
                if (proxySupport.isNotToBeProxied(
                      clazzWithAppropriateUpperBoundType))
                  retrieveUnderlying(trancheIdForExternalObjectReference,
                                     objectReferenceId,
                                     clazzWithAppropriateUpperBoundType)
                else {
                  val proxy =
                    proxySupport.createProxy(
                      clazzWithAppropriateUpperBoundType,
                      new AcquiredState(trancheIdForExternalObjectReference,
                                        objectReferenceId,
                                        clazzWithAppropriateUpperBoundType))

                  referenceIdToProxyMap.put(objectReferenceId, proxy)

                  proxy
                }
              }
            }(objectCache, mode, implicitly[Flags])

        def getReadObjectConsultingOnlyThisTranche(
            objectReferenceId: ObjectReferenceId): AnyRef =
          referenceIdToObjectMap.get(objectReferenceId)

        override def setKryo(kryo: Kryo): Unit = {}

        override def reset(): Unit = {}

        override def useReferences(clazz: Class[_]): Boolean =
          thisSessionInterpreter.useReferences(clazz)
      }

      case class CompletedOperationData(
          referenceResolver: TrancheSpecificReferenceResolver,
          topLevelObject: Any)

      override def apply[X](operation: Operation[X]): EitherThrowableOr[X] =
        operation match {
          case Store(immutableObject) =>
            for {
              objectReferenceIdOffsetForNewTranche <- tranches.objectReferenceIdOffsetForNewTranche
              trancheSpecificReferenceResolver = new TrancheSpecificReferenceResolver(
                objectReferenceIdOffsetForNewTranche)
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
            for {
              tranche <- tranches.retrieveTranche(trancheId)
              result <- Try {
                (completedOperationDataByTrancheId.get(trancheId) match {
                  case Some(CompletedOperationData(_, topLevelObject)) =>
                    topLevelObject

                  case None =>
                    val objectReferenceIdOffset =
                      tranche.objectReferenceIdOffset
                    val trancheSpecificReferenceResolver =
                      new TrancheSpecificReferenceResolver(
                        objectReferenceIdOffset)

                    val deserialized =
                      sessionReferenceResolver.withValue(
                        Some(trancheSpecificReferenceResolver)) {
                        kryoPool.fromBytes(tranche.serializedRepresentation)
                      }

                    completedOperationDataByTrancheId += trancheId -> CompletedOperationData(
                      trancheSpecificReferenceResolver,
                      deserialized)
                    clazz.cast(deserialized)
                }).asInstanceOf[X]
              }.toEither
            } yield result

        }
    }

    session.foldMap(sessionInterpreter)
  }
}
