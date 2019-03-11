package com.sageserpent.plutonium.curium
import java.util.UUID

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.util.MapReferenceResolver
import com.esotericsoftware.kryo.{Kryo, ReferenceResolver}
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
      require(objectReferenceIds.nonEmpty)
      require(objectReferenceIdOffset <= objectReferenceIds.min)

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

  private def run[Result](session: Session[Result])(
      tranches: Tranches): EitherThrowableOr[Result] = {
    object sessionInterpreter extends FunctionK[Operation, EitherThrowableOr] {
      thisSessionInterpreter =>
      val sessionReferenceResolver: DynamicVariable[Option[ReferenceResolver]] =
        new DynamicVariable(None)

      object referenceResolver extends ReferenceResolver {
        override def setKryo(kryo: Kryo): Unit = {
          sessionReferenceResolver.value.get.setKryo(kryo)
        }

        override def getWrittenId(anObject: Any): ObjectReferenceId =
          sessionReferenceResolver.value.get.getWrittenId(anObject)
        override def addWrittenObject(anObject: Any): ObjectReferenceId =
          sessionReferenceResolver.value.get.addWrittenObject(anObject)
        override def nextReadId(clazz: Class[_]): ObjectReferenceId =
          sessionReferenceResolver.value.get.nextReadId(clazz)
        override def setReadObject(objectReferenceId: ObjectReferenceId,
                                   anObject: Any): Unit = {
          sessionReferenceResolver.value.get
            .setReadObject(objectReferenceId, anObject)
        }
        override def getReadObject(
            clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef =
          sessionReferenceResolver.value.get
            .getReadObject(clazz, objectReferenceId)
        override def reset(): Unit = {
          // NOTE: prevent Kryo from resetting the underlying reference resolver as it will be
          // cached and used to resolve inter-tranche object references once it has been fully built.
        }
        override def useReferences(clazz: Class[_]): Boolean =
          sessionReferenceResolver.value.get.useReferences(clazz)

      }

      class TrancheSpecificReferenceResolver(
          objectReferenceIdOffset: ObjectReferenceId)
          extends MapReferenceResolver {
        // TODO: use 'objectReferenceIdOffsetForNewTranche'.
        def writtenObjectReferenceIds: immutable.IndexedSeq[ObjectReferenceId] =
          (0 until writtenObjects.size) map (objectReferenceIdOffset + _)

        override def getWrittenId(immutableObject: Any): ObjectReferenceId =
          // TODO - how are we going to find an inter-tranche object reference? If the idea is to assume that structure sharing
          // is only possible for objects in the same session, will it enough to scan through the cached reference resolvers?
          {
            val resultFromSuperImplementation =
              super.getWrittenId(immutableObject)
            if (resultFromSuperImplementation != -1)
              objectReferenceIdOffset + resultFromSuperImplementation
            else resultFromSuperImplementation
          }

        override def addWrittenObject(immutableObject: Any): ObjectReferenceId =
          objectReferenceIdOffset + super.addWrittenObject(immutableObject)

        override def nextReadId(clazz: Class[_]): ObjectReferenceId = {
          objectReferenceIdOffset + super.nextReadId(clazz)
        }

        override def setReadObject(objectReferenceId: ObjectReferenceId,
                                   immutableObject: Any): Unit = {
          super.setReadObject(objectReferenceId - objectReferenceIdOffset,
                              immutableObject)
        }

        override def getReadObject(
            clazz: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef =
          if (objectReferenceId >= objectReferenceIdOffset)
            super
              .getReadObject(clazz, objectReferenceId - objectReferenceIdOffset)
          else {
            val Right(trancheIdForExternalObjectReference) =
              tranches
                .retrieveTrancheId(objectReferenceId) // TODO - what happens if the call results in a left-value? I think it will propagate up nicely, but this needs to be checked.
            if (!refererenceResolversByTrancheId.contains(
                  trancheIdForExternalObjectReference)) {
              val _ =
                thisSessionInterpreter(
                  Retrieve(trancheIdForExternalObjectReference))
            }

            refererenceResolversByTrancheId(trancheIdForExternalObjectReference)
              .getReadObject(clazz, objectReferenceId)
          }
      }

      // TODO - cutover to using weak references, perhaps via 'WeakCache'?
      val refererenceResolversByTrancheId
        : MutableSortedMap[TrancheId, ReferenceResolver] =
        MutableSortedMap.empty

      val kryoInstantiator: ScalaKryoInstantiator = new ScalaKryoInstantiator {
        override def newKryo(): KryoBase = {
          val result = super.newKryo()

          result.setReferenceResolver(referenceResolver)

          result.setAutoReset(true) // Kryo should reset its *own* state (but not the reference resolvers') after a tranche has been stored and retrieved.

          result
        }
      }

      val kryoPool: KryoPool =
        KryoPool.withByteArrayOutputStream(40, kryoInstantiator)

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

                assert(
                  trancheSpecificReferenceResolver.writtenObjectReferenceIds.nonEmpty)

                tranches
                  .createTrancheInStorage(
                    serializedRepresentation,
                    objectReferenceIdOffsetForNewTranche,
                    trancheSpecificReferenceResolver.writtenObjectReferenceIds)
              }
            } yield {
              refererenceResolversByTrancheId += trancheId -> trancheSpecificReferenceResolver
              trancheId
            }

          case retrieve @ Retrieve(trancheId) =>
            for {
              tranche <- tranches.retrieveTranche(trancheId)
              result <- Try {
                val objectReferenceIdOffset = tranche.objectReferenceIdOffset
                val trancheSpecificReferenceResolver =
                  new TrancheSpecificReferenceResolver(objectReferenceIdOffset)

                val deserialized =
                  sessionReferenceResolver.withValue(
                    Some(trancheSpecificReferenceResolver)) {
                    kryoPool.fromBytes(tranche.serializedRepresentation)
                  }

                refererenceResolversByTrancheId += trancheId -> trancheSpecificReferenceResolver
                classFromType(retrieve.capturedTypeTag.tpe)
                  .cast(deserialized)
                  .asInstanceOf[X]
              }.toEither
            } yield result

        }
    }

    session.foldMap(sessionInterpreter)
  }
}
