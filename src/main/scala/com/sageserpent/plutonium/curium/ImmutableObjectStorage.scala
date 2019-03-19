package com.sageserpent.plutonium.curium
import java.lang.reflect.Method
import java.util.UUID

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.{Kryo, ReferenceResolver}
import com.google.common.collect.{BiMap, HashBiMap}
import com.sageserpent.plutonium.classFromType
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}
import net.bytebuddy.description.`type`.TypeDescription
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.bind.annotation.{
  AllArguments,
  FieldValue,
  Origin,
  RuntimeType
}
import net.bytebuddy.implementation.{FieldAccessor, MethodDelegation}
import net.bytebuddy.matcher.{ElementMatcher, ElementMatchers}
import net.bytebuddy.{ByteBuddy, NamingStrategy}
import scalacache._
import scalacache.caffeine._
import scalacache.memoization._
import scalacache.modes.sync._
import sun.misc.Unsafe

import scala.collection.immutable
import scala.collection.mutable.{
  Map => MutableMap,
  SortedMap => MutableSortedMap
}
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag
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

  case class Retrieve[X: TypeTag](trancheId: TrancheId) extends Operation[X] {
    val capturedTypeTag = implicitly[TypeTag[X]]
  }

  type Session[X] = FreeT[Operation, EitherThrowableOr, X]

  def store[X](immutableObject: X): Session[TrancheId] =
    FreeT.liftF[Operation, EitherThrowableOr, TrancheId](Store(immutableObject))

  def retrieve[X: TypeTag](id: TrancheId): Session[X] =
    FreeT.liftF[Operation, EitherThrowableOr, X](Retrieve(id))

  def runToYieldTrancheIds(session: Session[Vector[TrancheId]])
    : Tranches => EitherThrowableOr[Vector[TrancheId]] =
    run(session)

  def runToYieldTrancheId(
      session: Session[TrancheId]): Tranches => EitherThrowableOr[TrancheId] =
    run(session)

  def runForEffectsOnly(
      session: Session[Unit]): Tranches => EitherThrowableOr[Unit] =
    run(session)

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

  private val kryoInstantiator: ScalaKryoInstantiator =
    new ScalaKryoInstantiator {
      override def newKryo(): KryoBase = {
        val result = super.newKryo()

        result.setReferenceResolver(referenceResolver)

        result.setAutoReset(true) // Kryo should reset its *own* state (but not the states of the reference resolvers) after a tranche has been stored or retrieved.

        result
      }
    }

  private val kryoPool: KryoPool =
    KryoPool.withByteArrayOutputStream(40, kryoInstantiator)

  object proxySupport {
    private val byteBuddy = new ByteBuddy()

    private val proxySuffix = "delayedLoadProxy"

    trait AcquiredState {
      def underlying: Any
    }

    private[curium] trait StateAcquisition {
      def acquire(acquiredState: AcquiredState): Unit
    }

    object proxyDelayedLoading {
      @RuntimeType
      def apply(
          @Origin method: Method,
          @AllArguments arguments: Array[AnyRef],
          @FieldValue("acquiredState") acquiredState: AcquiredState
      ): Any = {
        val underlying = acquiredState.underlying
        method.invoke(underlying, arguments) // TODO - I can do better than this!!!!
      }
    }

    private val methodsInAny: Array[MethodDescription.ForLoadedMethod] = classOf[
      Any].getMethods map (new MethodDescription.ForLoadedMethod(_))

    private val delegateToProxy: ElementMatcher[MethodDescription] =
      methodDescription => !methodsInAny.contains(methodDescription)

    private def createProxyClass(clazz: Class[_]): Class[_] = {
      byteBuddy
        .`with`(new NamingStrategy.AbstractBase {
          override def name(superClass: TypeDescription): String =
            s"${superClass.getSimpleName}_$proxySuffix"
        })
        .subclass(clazz, ConstructorStrategy.Default.NO_CONSTRUCTORS)
        .method(delegateToProxy)
        .intercept(MethodDelegation.to(proxyDelayedLoading))
        .defineField("acquiredState", classOf[AcquiredState])
        .implement(classOf[StateAcquisition])
        .method(ElementMatchers.named("acquire"))
        .intercept(FieldAccessor.ofField("acquiredState"))
        .make
        .load(getClass.getClassLoader, ClassLoadingStrategy.Default.INJECTION)
        .getLoaded
    }

    private val cachedProxyClasses: MutableMap[Class[_], Class[_]] =
      MutableMap.empty

    // TODO - this what copied and pasted without any thought whatsoever from the 'objenesis' library sources.
    // Do we have to do this? For that matter, why not just use the original library?
    val theUnsafeField = {
      val result = classOf[Unsafe]
        .getDeclaredField("theUnsafe")
      result.setAccessible(true)
      result
    }

    def createProxy[Result](clazz: Class[_],
                            acquiredState: AcquiredState): AnyRef = {
      val proxyClazz = synchronized {
        cachedProxyClasses.getOrElseUpdate(clazz, {
          createProxyClass(clazz)
        })
      }

      val unsafe = theUnsafeField
        .get(null)
        .asInstanceOf[Unsafe]

      val proxy = unsafe
        .allocateInstance(proxyClazz)
        .asInstanceOf[StateAcquisition]

      proxy.acquire(acquiredState)

      proxy
    }
  }

  private def run[Result](session: Session[Result])(
      tranches: Tranches): EitherThrowableOr[Result] = {
    object sessionInterpreter extends FunctionK[Operation, EitherThrowableOr] {
      thisSessionInterpreter =>

      // TODO - cutover to using weak references, perhaps via 'WeakCache'?
      val completedOperationDataByTrancheId
        : MutableSortedMap[TrancheId, CompletedOperationData] =
        MutableSortedMap.empty

      private val referenceResolverCacheTimeToLive = Some(10 seconds)

      private implicit val referenceResolverCacheConfiguration: CacheConfig =
        CacheConfig.defaultCacheConfig.copy(
          cacheKeyBuilder = new CacheKeyBuilder {
            override def toCacheKey(parts: Seq[Any]): String =
              System.identityHashCode(parts.head).toString
            override def stringToCacheKey(key: String): String = key
          })

      private val objectReferenceIdCache: Cache[ObjectReferenceId] =
        CaffeineCache[ObjectReferenceId](
          CacheConfig.defaultCacheConfig.copy(
            memoization = MemoizationConfig(
              (fullClassName: String,
               constructorParameters: IndexedSeq[IndexedSeq[Any]],
               methodName: String,
               parameters: IndexedSeq[IndexedSeq[Any]]) =>
                s"${System.identityHashCode(parameters.head)}")))

      private val objectCache: Cache[AnyRef] = CaffeineCache[AnyRef]

      case class ReferenceBasedComparison(underlying: AnyRef) {
        override def equals(other: Any): Boolean = other match {
          case ReferenceBasedComparison(otherUnderlying) =>
            underlying eq otherUnderlying
          case _ => false
        }

        override def hashCode(): ObjectReferenceId =
          System.identityHashCode(underlying)
      }

      val proxyToReferenceIdMap
        : BiMap[ReferenceBasedComparison, ObjectReferenceId] =
        HashBiMap.create()

      val referenceIdToProxyMap
        : BiMap[ObjectReferenceId, ReferenceBasedComparison] =
        proxyToReferenceIdMap.inverse()

      class TrancheSpecificReferenceResolver(
          objectReferenceIdOffset: ObjectReferenceId)
          extends ReferenceResolver {
        val objectToReferenceIdMap
          : BiMap[ReferenceBasedComparison, ObjectReferenceId] =
          HashBiMap.create()

        val referenceIdToObjectMap
          : BiMap[ObjectReferenceId, ReferenceBasedComparison] =
          objectToReferenceIdMap.inverse()

        def writtenObjectReferenceIds: immutable.IndexedSeq[ObjectReferenceId] =
          (0 until objectToReferenceIdMap.size) map (objectReferenceIdOffset + _)

        override def getWrittenId(immutableObject: AnyRef): ObjectReferenceId =
          memoizeSync(referenceResolverCacheTimeToLive) {
            val resultFromExSessionReferenceResolver =
              completedOperationDataByTrancheId.view
                .map {
                  case (_, CompletedOperationData(referenceResolver, _)) =>
                    val objectReferenceId =
                      referenceResolver.getWrittenIdConsultingOnlyThisTranche(
                        immutableObject)
                    if (-1 != objectReferenceId) Some(objectReferenceId)
                    else None
                }
                .collectFirst {
                  case Some(objectReferenceId) => objectReferenceId
                }
            resultFromExSessionReferenceResolver
              .orElse(Option(proxyToReferenceIdMap.get(
                ReferenceBasedComparison(immutableObject))))
              .getOrElse(getWrittenIdConsultingOnlyThisTranche(immutableObject))
          }(objectReferenceIdCache, mode, implicitly[Flags])

        private def getWrittenIdConsultingOnlyThisTranche(
            immutableObject: AnyRef): ObjectReferenceId =
          Option(
            objectToReferenceIdMap.get(
              ReferenceBasedComparison(immutableObject))).getOrElse(-1)

        override def addWrittenObject(
            immutableObject: AnyRef): ObjectReferenceId = {
          val nextObjectReferenceIdToAllocate = objectToReferenceIdMap.size + objectReferenceIdOffset
          val _ @None = Option(
            objectToReferenceIdMap.putIfAbsent(
              ReferenceBasedComparison(immutableObject),
              nextObjectReferenceIdToAllocate))
          nextObjectReferenceIdToAllocate
        }

        override def nextReadId(clazz: Class[_]): ObjectReferenceId = {
          val nextObjectReferenceIdToAllocate = referenceIdToObjectMap.size + objectReferenceIdOffset
          val _ @None = Option(
            referenceIdToObjectMap.putIfAbsent(nextObjectReferenceIdToAllocate,
                                               null))
          nextObjectReferenceIdToAllocate
        }

        override def setReadObject(objectReferenceId: ObjectReferenceId,
                                   immutableObject: AnyRef): Unit = {
          require(objectReferenceIdOffset <= objectReferenceId)
          referenceIdToObjectMap.forcePut(
            objectReferenceId,
            ReferenceBasedComparison(immutableObject))
        }

        override def getReadObject(
            @cacheKeyExclude clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef =
          memoizeSync(referenceResolverCacheTimeToLive) {
            if (objectReferenceId >= objectReferenceIdOffset)
              getReadObjectConsultingOnlyThisTranche(objectReferenceId)
            else {
              // TODO: always look at the other tranche resolvers first to see if
              // the inter-tranche reference can be resolved immediately without
              // using a proxy. If not, look in the session's
              // proxy resolver, which is the last resort for resolution, and finally
              // fallback to generating the proxy on the fly and storing it in the
              // proxy resolver.

              // TODO: add this in on the write side too!

              // TODO: clean up the 'containsKey' + 'get' sequences.

              val Right(trancheIdForExternalObjectReference) =
                tranches
                  .retrieveTrancheId(objectReferenceId)

              if (completedOperationDataByTrancheId.contains(
                    trancheIdForExternalObjectReference)) {
                completedOperationDataByTrancheId(
                  trancheIdForExternalObjectReference).referenceResolver
                  .getReadObjectConsultingOnlyThisTranche(objectReferenceId)
              } else if (referenceIdToProxyMap.containsKey(objectReferenceId)) {
                referenceIdToProxyMap.get(objectReferenceId).underlying
              } else {
                val acquiredState = new proxySupport.AcquiredState {
                  private var _underlying: Option[Any] = None

                  override def underlying: Any = _underlying match {
                    case Some(result) => result
                    case None =>
                      if (!completedOperationDataByTrancheId.contains(
                            trancheIdForExternalObjectReference)) {
                        val _ =
                          thisSessionInterpreter(
                            Retrieve(trancheIdForExternalObjectReference))
                      }

                      val result = completedOperationDataByTrancheId(
                        trancheIdForExternalObjectReference).referenceResolver
                        .getReadObjectConsultingOnlyThisTranche(
                          objectReferenceId)

                      _underlying = Some(result)

                      result
                  }
                }

                val proxy = proxySupport.createProxy(clazz, acquiredState)

                referenceIdToProxyMap.put(objectReferenceId,
                                          ReferenceBasedComparison(proxy))

                proxy
              }
            }
          }(objectCache, mode, implicitly[Flags])

        private def getReadObjectConsultingOnlyThisTranche(
            objectReferenceId: ObjectReferenceId): AnyRef =
          referenceIdToObjectMap.get(objectReferenceId).underlying

        override def setKryo(kryo: Kryo): Unit = {}

        override def reset(): Unit = {}

        override def useReferences(clazz: Class[_]): Boolean = true
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

          case retrieve @ Retrieve(trancheId) =>
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
                    classFromType(retrieve.capturedTypeTag.tpe)
                      .cast(deserialized)
                }).asInstanceOf[X]
              }.toEither
            } yield result

        }
    }

    session.foldMap(sessionInterpreter)
  }
}
