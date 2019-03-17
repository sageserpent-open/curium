package com.sageserpent.plutonium.curium
import java.util.UUID

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.util.ListReferenceResolver
import com.esotericsoftware.kryo.{Kryo, ReferenceResolver}
import com.google.common.collect.{BiMap, HashBiMap}
import com.sageserpent.plutonium.classFromType
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}

import scala.collection.immutable
import scala.collection.mutable.{SortedMap => MutableSortedMap}
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

  private def run[Result](session: Session[Result])(
      tranches: Tranches): EitherThrowableOr[Result] = {
    object sessionInterpreter extends FunctionK[Operation, EitherThrowableOr] {
      thisSessionInterpreter =>

      case class ReferenceBasedComparison(underlying: AnyRef) {
        override def equals(other: Any): Boolean = other match {
          case ReferenceBasedComparison(otherUnderlying) =>
            underlying eq otherUnderlying
          case _ => false
        }

        override def hashCode(): ObjectReferenceId =
          System.identityHashCode(underlying)
      }

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

        override def getWrittenId(
            immutableObject: AnyRef): ObjectReferenceId = {
          val resultFromExSessionReferenceResolver =
            completedOperationDataByTrancheId.view
              .map {
                case (_, CompletedOperationData(referenceResolver, _)) =>
                  val objectReferenceId =
                    referenceResolver.getWrittenIdConsultingOnlyThisTranche(
                      immutableObject)
                  if (-1 != objectReferenceId) Some(objectReferenceId) else None
              }
              .collectFirst {
                case Some(objectReferenceId) => objectReferenceId
              }
          resultFromExSessionReferenceResolver.getOrElse(
            getWrittenIdConsultingOnlyThisTranche(immutableObject))
        }

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
            clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef =
          if (objectReferenceId >= objectReferenceIdOffset)
            getReadObjectConsultingOnlyThisTranche(clazz, objectReferenceId)
          else {
            val Right(trancheIdForExternalObjectReference) =
              tranches
                .retrieveTrancheId(objectReferenceId)
            if (!completedOperationDataByTrancheId.contains(
                  trancheIdForExternalObjectReference)) {
              val _ =
                thisSessionInterpreter(
                  Retrieve(trancheIdForExternalObjectReference))
            }

            completedOperationDataByTrancheId(
              trancheIdForExternalObjectReference).referenceResolver
              .getReadObjectConsultingOnlyThisTranche(clazz, objectReferenceId)
          }

        private def getReadObjectConsultingOnlyThisTranche(
            clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef =
          referenceIdToObjectMap.get(objectReferenceId).underlying

        override def setKryo(kryo: Kryo): Unit = {}

        override def reset(): Unit = {}

        override def useReferences(clazz: Class[_]): Boolean = true
      }

      case class CompletedOperationData(
          referenceResolver: TrancheSpecificReferenceResolver,
          topLevelObject: Any)

      // TODO - cutover to using weak references, perhaps via 'WeakCache'?
      val completedOperationDataByTrancheId
        : MutableSortedMap[TrancheId, CompletedOperationData] =
        MutableSortedMap.empty

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
