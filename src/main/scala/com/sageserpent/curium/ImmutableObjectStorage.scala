package com.sageserpent.curium

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import com.esotericsoftware.kryo.util.{DefaultClassResolver, Pool, Util}
import com.esotericsoftware.kryo.{
  Kryo,
  KryoCopyable,
  ReferenceResolver,
  Registration
}
import com.github.benmanes.caffeine.cache.{Cache, Scheduler}
import com.google.common.collect.{BiMap, BiMapFactory}
import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo
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

import java.io.ByteArrayOutputStream
import java.lang.reflect.Modifier
import java.util.concurrent.TimeUnit
import java.util.{Map => JavaMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters.{MapHasAsJava, MapHasAsScala}
import scala.ref.WeakReference
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.util.Using.Releasable
import scala.util.hashing.MurmurHash3
import scala.util.{DynamicVariable, Success, Try, Using}

object ImmutableObjectStorage {
  type TrancheLocalObjectReferenceId = Int
  type CanonicalObjectReferenceId[TrancheId] =
    (TrancheId, TrancheLocalObjectReferenceId)
  type EitherThrowableOr[X] = Either[Throwable, X]
  type Session[X]           = FreeT[Operation, EitherThrowableOr, X]
  val maximumObjectReferenceId: TrancheLocalObjectReferenceId = Int.MaxValue

  trait Tranches[TrancheIdImplementation] {
    type TrancheId = TrancheIdImplementation

    def createTrancheInStorage(
        tranche: TrancheOfData[TrancheId]
    ): EitherThrowableOr[TrancheId]

    def retrieveTranche(
        trancheId: TrancheId
    ): EitherThrowableOr[TrancheOfData[TrancheId]]
  }

  trait Operation[Result]

  protected trait ProxySupport {
    type PipeForwarding = Function[AnyRef, Nothing]
    val byteBuddy = new ByteBuddy()
    /* This is tracked to workaround Kryo leaking its internal fudge as to how
     * it registers closure serializers into calls on the tranche specific
     * reference resolver class' methods. */
    val kryoClosureMarkerClazz = classOf[Closure]
    val stateAcquisitionClazz  = classOf[StateAcquisition]
    val kryoCopyableClazz      = classOf[KryoCopyable[StateAcquisition]]
    val instantiatorStrategy: StdInstantiatorStrategy =
      new StdInstantiatorStrategy

    def isProxyClazz(clazz: Class[_]): Boolean =
      stateAcquisitionClazz.isAssignableFrom(clazz)

    def isProxy(immutableObject: AnyRef): Boolean =
      stateAcquisitionClazz.isInstance(immutableObject)

    trait AcquiredState {
      def underlying: AnyRef
    }

    private[curium] trait StateAcquisition {
      def acquire(acquiredState: AcquiredState): Unit
    }

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

    object proxyCopying {
      @RuntimeType
      def copy(
          @RuntimeType kryo: Kryo,
          @FieldValue("acquiredState") acquiredState: AcquiredState
      ): Any = {
        val underlying: AnyRef = acquiredState.underlying

        val copyOfUnderlying = kryo.copy(underlying)
        kryo.reference(copyOfUnderlying)

        copyOfUnderlying
      }
    }

  }

  case class TrancheOfData[TrancheId](
      payload: Array[Byte],
      interTrancheObjectReferenceIdTranslation: Map[
        TrancheLocalObjectReferenceId,
        CanonicalObjectReferenceId[TrancheId]
      ]
  ) {
    // NOTE: 'WrappedArray' isn't used here as it could require non-trivial
    // runtime conversions if the array type is cast, or needs support to work
    // with Doobie and other things that need an explicit typeclass for it.

    override def equals(another: Any): Boolean = another match {
      case TrancheOfData(
            payload,
            interTrancheObjectReferenceIdTranslation
          ) =>
        this.payload
          .sameElements(
            payload
          ) && this.interTrancheObjectReferenceIdTranslation == interTrancheObjectReferenceIdTranslation
      case _ => false
    }

    override def hashCode(): Int = MurmurHash3.productHash(
      (
        MurmurHash3.bytesHash(payload),
        interTrancheObjectReferenceIdTranslation
      )
    )

    override def toString: String =
      s"TrancheOfData(payload hash: ${MurmurHash3.bytesHash(payload)}, inter-tranche object reference id translation: $interTrancheObjectReferenceIdTranslation)"
  }

  class IntersessionState[TrancheId] {
    private val objectToReferenceIdCache
        : Cache[AnyRef, CanonicalObjectReferenceId[TrancheId]] =
      caffeineBuilder()
        .scheduler(Scheduler.systemScheduler())
        .executor(_.run())
        .weakKeys()
        .build[AnyRef, CanonicalObjectReferenceId[TrancheId]]()

    private val referenceIdToObjectCache
        : Cache[CanonicalObjectReferenceId[TrancheId], AnyRef] =
      caffeineBuilder()
        .scheduler(Scheduler.systemScheduler())
        .executor(_.run())
        .expireAfterWrite(30, TimeUnit.SECONDS)
        .build[CanonicalObjectReferenceId[TrancheId], AnyRef]()

    private val proxyToReferenceIdCache
        : Cache[AnyRef, CanonicalObjectReferenceId[TrancheId]] =
      caffeineBuilder()
        .scheduler(Scheduler.systemScheduler())
        .executor(_.run())
        .weakKeys()
        .build[AnyRef, CanonicalObjectReferenceId[TrancheId]]()

    private val referenceIdToProxyCache
        : Cache[CanonicalObjectReferenceId[TrancheId], AnyRef] =
      caffeineBuilder()
        .scheduler(Scheduler.systemScheduler())
        .executor(_.run())
        .weakValues()
        .build[CanonicalObjectReferenceId[TrancheId], AnyRef]()

    val trancheIdToStuffCache: Cache[TrancheId, (Any, ObjectLookup)] =
      caffeineBuilder()
        .scheduler(Scheduler.systemScheduler())
        .executor(_.run())
        .expireAfterWrite(
          5,
          TimeUnit.MINUTES
        ) // NASTY HACK - leave it here for now while experimenting.
        .build[TrancheId, (Any, ObjectLookup)]()

    def noteReferenceIdForNonProxy(
        immutableObject: AnyRef,
        objectReferenceId: CanonicalObjectReferenceId[TrancheId]
    ): Unit = {
      objectToReferenceIdCache.put(immutableObject, objectReferenceId)
    }

    def referenceIdFor(
        immutableObject: AnyRef
    ): Option[CanonicalObjectReferenceId[TrancheId]] =
      Option(proxyToReferenceIdCache.getIfPresent(immutableObject))
        .orElse(
          Option(objectToReferenceIdCache.getIfPresent(immutableObject))
        )

    def nonProxyFor(
        objectReferenceId: CanonicalObjectReferenceId[TrancheId],
        population: CanonicalObjectReferenceId[TrancheId] => AnyRef
    ): AnyRef =
      referenceIdToObjectCache.get(objectReferenceId, population(_))

    def noteProxy(
        objectReferenceId: CanonicalObjectReferenceId[TrancheId],
        immutableObject: AnyRef
    ): Unit = {
      referenceIdToProxyCache.put(objectReferenceId, immutableObject)
      proxyToReferenceIdCache.put(immutableObject, objectReferenceId)
    }

    def proxyFor(objectReferenceId: CanonicalObjectReferenceId[TrancheId]) =
      Option(referenceIdToProxyCache.getIfPresent(objectReferenceId))

    def clear(): Unit = {
      objectToReferenceIdCache.invalidateAll()
      referenceIdToObjectCache.invalidateAll()
      referenceIdToProxyCache.invalidateAll()
    }
  }

  object standaloneExemplarToEnticeScalaKyro

  case class AssociatedValueForAlias(immutableObject: AnyRef) extends AnyRef

  def decodePlaceholder(placeholderOrActualObject: AnyRef): AnyRef =
    placeholderOrActualObject match {
      case AssociatedValueForAlias(immutableObject) => immutableObject
      case immutableObject @ _                      => immutableObject
    }

  trait ObjectLookup {
    protected val referenceIdToLocalObjectMap: JavaMap[
      TrancheLocalObjectReferenceId,
      AnyRef
    ]

    def objectWithReferenceId(
        objectReferenceId: TrancheLocalObjectReferenceId
    ): AnyRef =
      Option(referenceIdToLocalObjectMap.get(objectReferenceId))
        .map(decodePlaceholder)
        .get
  }

  case class StandaloneObjectLookup(
      override protected val referenceIdToLocalObjectMap: JavaMap[
        TrancheLocalObjectReferenceId,
        AnyRef
      ]
  ) extends ObjectLookup
}

trait ImmutableObjectStorage[TrancheId] {
  storage =>

  import ImmutableObjectStorage._

  protected val tranchesImplementationName: String
  private val sessionReferenceResolver
      : DynamicVariable[Option[ReferenceResolver]] =
    new DynamicVariable(None)
  private val kryoPool: Pool[Kryo] =
    new Pool[Kryo](true, false) {
      override def create(): Kryo = {
        val result = new ScalaKryo(
          classResolver = new DefaultClassResolver,
          referenceResolver = referenceResolver
        ) {
          override def getRegistration(clazz: Class[_]): Registration =
            super.getRegistration(proxySupport.nonProxyClazzFor(clazz))
        }

        // NASTY HACK - treat `ScalaKryo` as a whitebox and pull out the shared
        // instance of `ScalaObjectSerializer` that it maintains. Then tell it
        // that, yes, it can perform a copy by simply yielding the original
        // immutable instance. Finally, submit a pull request to:
        // https://github.com/altoo-ag/akka-kryo-serialization .
        result
          .getDefaultSerializer(
            classOf[standaloneExemplarToEnticeScalaKyro.type]
          )
          .setImmutable(true)

        result.setRegistrationRequired(false)
        result.setInstantiatorStrategy(
          new org.objenesis.strategy.StdInstantiatorStrategy
        )

        result.register(
          proxySupport.kryoClosureMarkerClazz,
          new ClosureCleaningSerializer()
        )

        result.setAutoReset(
          true
        ) // Kryo should reset its *own* state (but not the states of the reference resolvers) after a tranche has been stored or retrieved.

        result
      }
    }

  private val inputPool: Pool[Input] = new Pool[Input](true, false) {
    override def create(): Input = new Input()
  }

  private val outputPool: Pool[Output] = new Pool[Output](true, false) {
    override def create(): Output = new Output(new ByteArrayOutputStream())
  }

  def store[X](immutableObject: X): Session[TrancheId] =
    FreeT.liftF[Operation, EitherThrowableOr, TrancheId](Store(immutableObject))

  def retrieve[X: TypeTag](id: TrancheId): Session[X] =
    FreeT.liftF[Operation, EitherThrowableOr, X](
      Retrieve(id, classFromType(typeOf[X]))
    )

  def runToYieldTrancheIds(
      session: Session[Vector[TrancheId]],
      intersessionState: IntersessionState[TrancheId]
  ): Tranches[TrancheId] => EitherThrowableOr[Vector[TrancheId]] =
    unsafeRun(session, intersessionState)

  def runToYieldTrancheId(
      session: Session[TrancheId],
      intersessionState: IntersessionState[TrancheId]
  ): Tranches[TrancheId] => EitherThrowableOr[TrancheId] =
    unsafeRun(session, intersessionState)

  def runForEffectsOnly(
      session: Session[Unit],
      intersessionState: IntersessionState[TrancheId]
  ): Tranches[TrancheId] => EitherThrowableOr[Unit] =
    unsafeRun(session, intersessionState)

  private def unsafeRun[Result](
      session: Session[Result],
      intersessionState: IntersessionState[TrancheId]
  )(tranches: Tranches[TrancheId]): EitherThrowableOr[Result] = {
    object sessionInterpreter extends FunctionK[Operation, EitherThrowableOr] {
      thisSessionInterpreter =>

      def retrieveUnderlying(
          canonicalObjectReferenceId: CanonicalObjectReferenceId[TrancheId]
      ): AnyRef = intersessionState
        .nonProxyFor(
          canonicalObjectReferenceId,
          {
            case (
                  trancheIdForExternalObjectReference,
                  trancheLocalObjectReferenceId
                ) =>
              val Right((_, trancheSpecificReferenceResolver)) =
                loadTranche(trancheIdForExternalObjectReference)

              trancheSpecificReferenceResolver.objectWithReferenceId(
                trancheLocalObjectReferenceId
              )
          }
        )

      override def apply[X](operation: Operation[X]): EitherThrowableOr[X] =
        operation match {
          case Store(immutableObject) =>
            val trancheSpecificReferenceResolver =
              new TrancheSpecificWritingReferenceResolver
                with ReferenceResolverContracts
            val serializedRepresentation: Array[Byte] = sessionReferenceResolver
              .withValue(Some(trancheSpecificReferenceResolver)) {
                serializationFacade.toBytesWithClass(immutableObject)
              }

            for {
              trancheId <- tranches
                .createTrancheInStorage(
                  TrancheOfData(
                    serializedRepresentation,
                    trancheSpecificReferenceResolver.interTrancheObjectReferenceIdTranslation
                  )
                )
              _ = {
                intersessionState.trancheIdToStuffCache.put(
                  trancheId,
                  immutableObject -> trancheSpecificReferenceResolver
                    .objectLookup()
                )
                trancheSpecificReferenceResolver.noteTrancheId(trancheId)
              }
            } yield trancheId

          case Retrieve(trancheId, clazz) =>
            loadTranche(trancheId)
              .flatMap { case (topLevelObject, _) =>
                Try {
                  clazz.cast(topLevelObject)
                }.toEither
              }
        }

      def loadTranche(
          trancheId: TrancheId
      ): EitherThrowableOr[(Any, ObjectLookup)] =
        Option(
          intersessionState.trancheIdToStuffCache.getIfPresent(trancheId)
        ) match {
          case Some(payload) => Right(payload)

          case None =>
            for {
              tranche <- tranches.retrieveTranche(trancheId)
              result <- Try {
                val trancheSpecificReferenceResolver =
                  new TrancheSpecificReadingReferenceResolver(
                    trancheId,
                    tranche.interTrancheObjectReferenceIdTranslation
                  ) with ReferenceResolverContracts

                val topLevelObject =
                  sessionReferenceResolver.withValue(
                    Some(trancheSpecificReferenceResolver)
                  ) {
                    serializationFacade.fromBytes(tranche.payload)
                  }

                intersessionState.trancheIdToStuffCache.put(
                  trancheId,
                  topLevelObject -> trancheSpecificReferenceResolver
                    .objectLookup()
                )

                trancheSpecificReferenceResolver.noteTrancheId(trancheId)

                topLevelObject -> trancheSpecificReferenceResolver
              }.toEither
            } yield result
        }

      trait AbstractTrancheSpecificReferenceResolver
          extends ReferenceResolver
          with ObjectLookup {
        protected val _interTrancheObjectReferenceIdTranslation
            : BiMap[TrancheLocalObjectReferenceId, CanonicalObjectReferenceId[
              TrancheId
            ]] =
          BiMapFactory.empty()

        protected val referenceIdToLocalObjectMap
            : BiMap[TrancheLocalObjectReferenceId, AnyRef] =
          BiMapFactory.usingIdentityForInverse()
        protected var _numberOfLocalObjects: Int = 0

        def objectLookup(): ObjectLookup = StandaloneObjectLookup(
          referenceIdToLocalObjectMap
        )

        def noteTrancheId(trancheId: TrancheId): Unit = {
          referenceIdToLocalObjectMap.forEach {
            (objectReferenceId, immutableObject) =>
              if (allowInterTrancheReferences(immutableObject)) {
                intersessionState.noteReferenceIdForNonProxy(
                  immutableObject,
                  trancheId -> objectReferenceId
                )
              }
          }
        }

        override def setKryo(kryo: Kryo): Unit = {}

        override def reset(): Unit = {}

        override def useReferences(clazz: Class[_]): Boolean =
          storage.useReferences(clazz)

        protected def minimumInterTrancheObjectReferenceId
            : TrancheLocalObjectReferenceId =
          maximumObjectReferenceId - _interTrancheObjectReferenceIdTranslation
            .size()
      }

      class AcquiredState(
          canonicalObjectReferenceId: CanonicalObjectReferenceId[TrancheId]
      ) extends proxySupport.AcquiredState {
        private var _underlying: Option[WeakReference[AnyRef]] = None

        override def underlying: AnyRef = _underlying match {
          case Some(WeakReference(result)) => result
          case _ =>
            val result =
              retrieveUnderlying(canonicalObjectReferenceId)

            _underlying = Some(WeakReference(result))

            result
        }
      }

      class TrancheSpecificWritingReferenceResolver
          extends AbstractTrancheSpecificReferenceResolver {
        private val notImplementedError = new NotImplementedError(
          s"``${getClass.getSimpleName}`` does not support reading operations."
        )

        def interTrancheObjectReferenceIdTranslation
            : Map[TrancheLocalObjectReferenceId, CanonicalObjectReferenceId[
              TrancheId
            ]] =
          _interTrancheObjectReferenceIdTranslation.asScala.toMap

        override def getWrittenId(
            immutableObject: AnyRef
        ): TrancheLocalObjectReferenceId =
          intersessionState
            .referenceIdFor(immutableObject)
            .map(interTrancheObjectReferenceIdFor)
            .orElse(
              Option(referenceIdToLocalObjectMap.inverse().get(immutableObject))
            )
            .getOrElse(notYetWritten)

        private def interTrancheObjectReferenceIdFor(
            canonicalReference: CanonicalObjectReferenceId[TrancheId]
        ): TrancheLocalObjectReferenceId =
          _interTrancheObjectReferenceIdTranslation
            .inverse()
            .computeIfAbsent(
              canonicalReference, {_ =>
                require(
                  minimumInterTrancheObjectReferenceId > _numberOfLocalObjects
                )

                val numberOfInterTrancheReferences =
                  _interTrancheObjectReferenceIdTranslation.size()

                maximumObjectReferenceId - numberOfInterTrancheReferences
              }
            )

        override def addWrittenObject(
            immutableObject: AnyRef
        ): TrancheLocalObjectReferenceId = {
          require(minimumInterTrancheObjectReferenceId > _numberOfLocalObjects)

          val result = _numberOfLocalObjects

          val _ @None = Option(
            referenceIdToLocalObjectMap
              .put(result, immutableObject)
          )

          _numberOfLocalObjects += 1

          result
        }

        override def nextReadId(
            clazz: Class[_]
        ): TrancheLocalObjectReferenceId = throw notImplementedError

        override def setReadObject(
            objectReferenceId: TrancheLocalObjectReferenceId,
            immutableObject: AnyRef
        ): Unit = throw notImplementedError

        override def getReadObject(
            clazz: Class[_],
            objectReferenceId: TrancheLocalObjectReferenceId
        ): AnyRef = throw notImplementedError
      }

      class TrancheSpecificReadingReferenceResolver(
          trancheId: TrancheId,
          interTrancheObjectReferenceIdTranslation: Map[
            TrancheLocalObjectReferenceId,
            CanonicalObjectReferenceId[TrancheId]
          ]
      ) extends AbstractTrancheSpecificReferenceResolver {
        {
          _interTrancheObjectReferenceIdTranslation.putAll(
            interTrancheObjectReferenceIdTranslation.asJava
          )
        }

        private val notImplementedError = new NotImplementedError(
          s"`${getClass.getSimpleName}` does not support writing operations."
        )

        override def nextReadId(
            clazz: Class[_]
        ): TrancheLocalObjectReferenceId = {
          require(minimumInterTrancheObjectReferenceId > _numberOfLocalObjects)

          val nextObjectReferenceIdToAllocate = _numberOfLocalObjects

          _numberOfLocalObjects += 1

          nextObjectReferenceIdToAllocate
        }

        override def setReadObject(
            objectReferenceId: TrancheLocalObjectReferenceId,
            immutableObject: AnyRef
        ): Unit = {
          Option(
            referenceIdToLocalObjectMap
              .inverse()
              .forcePut(immutableObject, objectReferenceId)
          ) match {
            case Some(aliasObjectReferenceId) =>
              val associatedValueForAlias =
                AssociatedValueForAlias(immutableObject)
              val _ @None = Option(
                referenceIdToLocalObjectMap
                  .put(aliasObjectReferenceId, associatedValueForAlias)
              )
            case None =>
          }

          if (allowInterTrancheReferences(immutableObject)) {
            val canonicalObjectReferenceId =
              trancheId -> objectReferenceId

            intersessionState.noteReferenceIdForNonProxy(
              immutableObject,
              canonicalObjectReferenceId
            )
          }
        }

        override def getReadObject(
            clazz: Class[_],
            objectReferenceId: TrancheLocalObjectReferenceId
        ): AnyRef = {
          val yieldObjectLocalToTranche =
            minimumInterTrancheObjectReferenceId > objectReferenceId

          if (yieldObjectLocalToTranche)
            objectWithReferenceId(objectReferenceId)
          else {
            val canonicalObjectReferenceId = canonicalReferenceFor(
              objectReferenceId
            )

            intersessionState.proxyFor(canonicalObjectReferenceId).getOrElse {
              if (proxySupport.canBeProxied(clazz)) {
                val proxy =
                  proxySupport.createProxy(
                    clazz,
                    new AcquiredState(
                      canonicalObjectReferenceId
                    )
                  )

                intersessionState.noteProxy(canonicalObjectReferenceId, proxy)

                proxy
              } else
                retrieveUnderlying(canonicalObjectReferenceId)
            }
          }
        }

        private def canonicalReferenceFor(
            objectReferenceId: TrancheLocalObjectReferenceId
        ): CanonicalObjectReferenceId[TrancheId] = {
          Option(
            _interTrancheObjectReferenceIdTranslation.get(objectReferenceId)
          ).get
        }

        override def getWrittenId(
            immutableObject: AnyRef
        ): TrancheLocalObjectReferenceId = throw notImplementedError

        override def addWrittenObject(
            immutableObject: AnyRef
        ): TrancheLocalObjectReferenceId = throw notImplementedError
      }
    }

    session.foldMap(sessionInterpreter)
  }

  private def useReferences(clazz: Class[_]): Boolean =
    !Util.isWrapperClass(clazz) &&
      clazz != classOf[String]

  protected def allowInterTrancheReferences(immutableObject: AnyRef) =
    proxySupport.canBeProxied(
      immutableObject.getClass
    )

  def runToYieldResult[Result](
      session: Session[Result],
      intersessionState: IntersessionState[TrancheId]
  ): Tranches[TrancheId] => EitherThrowableOr[Result] =
    (unsafeRun(session, intersessionState)(_)).andThen(result =>
      result.map(serializationFacade.copy)
    )

  protected def isExcludedFromBeingProxied(clazz: Class[_]): Boolean = false

  // NOTE: this is a potential danger area when an override is defined -
  // returning true indicates that all uses of a proxied object can be performed
  // via the supertype and / or interfaces. Obvious examples where this is not
  // true would include a final class that doesn't extend an interface and only
  // has 'AnyRef' as a superclass - how would client code do something useful
  // with it? Scala case classes that are declared as final and form a union
  // type hierarchy that pattern matching is performed on will also fail. The
  // reason why this exists at all is to provide as an escape hatch for the
  // multitude of Scala case classes declared as final that are actually used as
  // part of an object-oriented interface hierarchy - the collection classes
  // being the main offenders in that regard.
  protected def canBeProxiedViaSuperTypes(clazz: Class[_]): Boolean = false

  case class Store[X](immutableObject: X) extends Operation[TrancheId]

  case class Retrieve[X](trancheId: TrancheId, clazz: Class[X])
      extends Operation[X]

  // Replacement for the now removed use of Chill's `KryoPool`...
  object serializationFacade {
    def evidence[X](pool: Pool[X]): Releasable[X] = pool.free

    implicit val kryoEvidence: Releasable[Kryo]     = evidence(kryoPool)
    implicit val inputEvidence: Releasable[Input]   = evidence(inputPool)
    implicit val outputEvidence: Releasable[Output] = evidence(outputPool)

    def fromBytes(bytes: Array[Byte]): Any =
      Using.resources(kryoPool.obtain(), inputPool.obtain()) { (kryo, input) =>
        input.setBuffer(bytes)
        kryo.readClassAndObject(input)
      }

    def toBytesWithClass(immutableObject: Any): Array[Byte] =
      Using.resources(kryoPool.obtain(), outputPool.obtain()) {
        (kryo, output) =>
          val byteArrayOutputStream =
            output.getOutputStream.asInstanceOf[ByteArrayOutputStream]
          byteArrayOutputStream.reset()
          output.reset()

          kryo.writeClassAndObject(output, immutableObject)

          output.flush()
          byteArrayOutputStream.toByteArray
      }

    def copy[X](immutableObject: X): X =
      Using.resource(kryoPool.obtain())(_.copy(immutableObject))
  }

  object proxySupport extends ProxySupport {

    val superClazzAndInterfacesCache
        : Cache[Class[_], Option[SuperClazzAndInterfaces]] =
      caffeineBuilder().build()
    val cachedProxyClassInstantiators
        : Cache[SuperClazzAndInterfaces, ObjectInstantiator[_]] =
      caffeineBuilder().build()
    val proxiedClazzCache: Cache[Class[_], Class[_]] = caffeineBuilder().build()
    private val proxySuffix =
      s"delayedLoadProxyFor${tranchesImplementationName}"
    private val superClazzBag: mutable.Map[TypeDescription, Int] =
      mutable.Map.empty

    def superClazzAndInterfacesToProxy(
        clazz: Class[_]
    ): Option[SuperClazzAndInterfaces] =
      superClazzAndInterfacesCache.get(clazz, _superClazzAndInterfacesToProxy)

    private def _superClazzAndInterfacesToProxy(
        clazz: Class[_]
    ): Option[SuperClazzAndInterfaces] = {
      require(!isProxyClazz(clazz))

      val clazzIsForAStandaloneOrSingletonObject = Try(
        currentMirror
          .reflectClass(currentMirror.classSymbol(clazz))
          .symbol
          .isModuleClass
      ).recoverWith {
        case _: ScalaReflectionException =>
          Success(false)
        case _: AssertionError =>
          // TODO: finesse this - probably misusing the Scala reflection API
          // here...
          Success(false)
      }.get

      val clazzShouldNotBeProxiedAtAll =
        clazzIsForAStandaloneOrSingletonObject ||
          kryoClosureMarkerClazz.isAssignableFrom(clazz) || !useReferences(
            clazz
          ) || isExcludedFromBeingProxied(clazz)

      if (!clazzShouldNotBeProxiedAtAll)
        if (shouldNotBeProxiedAsItsOwnType(clazz))
          if (canBeProxiedViaSuperTypes(clazz)) {
            val superClazz = clazz.getSuperclass
            if (shouldNotBeProxiedAsItsOwnType(superClazz))
              superClazzAndInterfacesToProxy(superClazz).map(
                superClazzAndInterfaces =>
                  superClazzAndInterfaces.copy(
                    interfaces =
                      superClazzAndInterfaces.interfaces ++ clazz.getInterfaces
                  )
              )
            else
              Some(
                SuperClazzAndInterfaces(
                  clazz.getSuperclass,
                  clazz.getInterfaces
                )
              )
          } else None
        else Some(SuperClazzAndInterfaces(clazz, Seq.empty))
      else None

    }

    def canBeProxied(clazz: Class[_]) =
      superClazzAndInterfacesToProxy(clazz).isDefined

    def createProxy(clazz: Class[_], acquiredState: AcquiredState): AnyRef = {
      val proxyClassInstantiator =
        synchronized {
          cachedProxyClassInstantiators.get(
            // TODO: there should be a test that fails if we just consult
            // 'superClazzAndInterfacesCache' rather than ensuring that it is
            // populated as is being done here, or at least proves that it is
            // populated beforehand elsewhere. Specifically, what happens when a
            // tranche is loaded into a session where a proxy class has not
            // already been created?
            superClazzAndInterfacesToProxy(clazz).get,
            { superClazzAndInterfaces =>
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

    private def shouldNotBeProxiedAsItsOwnType(clazz: Class[_]): Boolean =
      Modifier.isFinal(clazz.getModifiers) ||
        clazz.isSynthetic ||
        (try {
          clazz.isAnonymousClass ||
          clazz.isLocalClass
        } catch {
          case _: InternalError =>
            // Workaround: https://github.com/scala/bug/issues/2034 - if it
            // throws, it's probably an inner class of some kind.
            true
        })

    private def createProxyClass(
        superClazzAndInterfaces: SuperClazzAndInterfaces
    ): Class[_] = {
      // We should never end up having to make chains of delegating proxies!
      require(!isProxyClazz(superClazzAndInterfaces.superClazz))

      byteBuddy
        .`with`(new NamingStrategy.AbstractBase {
          override def name(superClass: TypeDescription): String = {
            superClazzBag.updateWith(superClass)(count =>
              count.map(1 + _).orElse(Some(1))
            )
            s"${superClass.getSimpleName}_${superClazzBag(superClass)}_$proxySuffix"
          }
        })
        .subclass(
          superClazzAndInterfaces.superClazz,
          ConstructorStrategy.Default.NO_CONSTRUCTORS
        )
        .implement(superClazzAndInterfaces.interfaces: _*)
        .method(
          ElementMatchers
            .isPublic() // TODO: make this configurable.
            .and(
              ElementMatchers.not(
                ElementMatchers
                  .takesArgument(0, classOf[IterableOnce[_]])
                  .and(ElementMatchers.nameContains("$plus$plus"))
              )
            )
        )
        .intercept(
          MethodDelegation
            .withDefaultConfiguration()
            .withBinders(Pipe.Binder.install(classOf[PipeForwarding]))
            .to(proxyDelayedLoading)
        )
        .defineField("acquiredState", classOf[AcquiredState])
        .implement(stateAcquisitionClazz)
        .method(
          ElementMatchers
            .named("acquire")
            .and(ElementMatchers.isDeclaredBy(stateAcquisitionClazz))
        )
        .intercept(FieldAccessor.ofField("acquiredState"))
        .implement(kryoCopyableClazz)
        .method(
          ElementMatchers
            .named("copy")
            .and(ElementMatchers.isDeclaredBy(kryoCopyableClazz))
        )
        .intercept(MethodDelegation.to(proxyCopying))
        .make
        .load(getClass.getClassLoader, ClassLoadingStrategy.Default.INJECTION)
        .getLoaded
    }

    case class SuperClazzAndInterfaces(
        superClazz: Class[_],
        interfaces: Seq[Class[_]]
    )
  }

  val notYetWritten = -1

  trait ReferenceResolverContracts extends ReferenceResolver {

    abstract override def getWrittenId(
        immutableObject: AnyRef
    ): TrancheLocalObjectReferenceId = {
      val result = super.getWrittenId(immutableObject)

      if (notYetWritten == result) {
        assert(!proxySupport.isProxy(immutableObject))
      }

      result
    }

    abstract override def addWrittenObject(
        immutableObject: AnyRef
    ): TrancheLocalObjectReferenceId = {
      require(!proxySupport.isProxy(immutableObject))

      super.addWrittenObject(immutableObject)
    }

    abstract override def nextReadId(
        clazz: Class[_]
    ): TrancheLocalObjectReferenceId = {
      require(!proxySupport.isProxyClazz(clazz))

      super.nextReadId(clazz)
    }

    abstract override def setReadObject(
        objectReferenceId: TrancheLocalObjectReferenceId,
        immutableObject: AnyRef
    ): Unit = {
      require(!proxySupport.isProxy(immutableObject))

      super.setReadObject(objectReferenceId, immutableObject)
    }

    abstract override def getReadObject(
        clazz: Class[_],
        objectReferenceId: TrancheLocalObjectReferenceId
    ): AnyRef = {
      require(!proxySupport.isProxyClazz(clazz))

      val result = super.getReadObject(clazz, objectReferenceId)

      assert(
        (proxySupport.superClazzAndInterfacesToProxy(clazz) match {
          case Some(
                proxySupport.SuperClazzAndInterfaces(superClazz, interfaces)
              ) =>
            superClazz.isInstance(result) && interfaces.forall(
              _.isInstance(result)
            )
          case None =>
            clazz
              .isInstance(result)
        }) || proxySupport.kryoClosureMarkerClazz
          .isAssignableFrom(clazz)
      )

      result
    }
  }

  private object referenceResolver extends ReferenceResolver {
    override def setKryo(kryo: Kryo): Unit = {
      // NASTY HACK: when copying an object that is escaping the `Session`
      // monad, there won't be a session, but it doesn't matter - it won't be
      // used, so no need to do anything with the `Kryo` instance here.
      sessionReferenceResolver.value.foreach(_.setKryo(kryo))
    }

    override def getWrittenId(
        immutableObject: Any
    ): TrancheLocalObjectReferenceId =
      sessionReferenceResolver.value.get.getWrittenId(immutableObject)

    override def addWrittenObject(
        immutableObject: Any
    ): TrancheLocalObjectReferenceId =
      sessionReferenceResolver.value.get.addWrittenObject(immutableObject)

    override def nextReadId(clazz: Class[_]): TrancheLocalObjectReferenceId =
      sessionReferenceResolver.value.get.nextReadId(clazz)

    override def setReadObject(
        objectReferenceId: TrancheLocalObjectReferenceId,
        anObject: Any
    ): Unit = {
      sessionReferenceResolver.value.get
        .setReadObject(objectReferenceId, anObject)
    }

    override def getReadObject(
        clazz: Class[_],
        objectReferenceId: TrancheLocalObjectReferenceId
    ): AnyRef =
      sessionReferenceResolver.value.get
        .getReadObject(clazz, objectReferenceId)

    override def reset(): Unit = {
      // NOTE: prevent Kryo from resetting the session reference resolver as it
      // will be cached and used to resolve inter-tranche object references once
      // a storage or retrieval operation completes.
    }

    override def useReferences(clazz: Class[_]): Boolean = {
      // NASTY HACK: when copying an object that is escaping the `Session`
      // monad, there won't be a session, but it doesn't matter - just delegate
      // to the storage anyway.
      sessionReferenceResolver.value.fold(ifEmpty =
        storage.useReferences(clazz)
      )(_.useReferences(clazz))
    }

  }
}
