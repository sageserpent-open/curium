package com.sageserpent.plutonium.curium
import java.util.UUID

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.sageserpent.plutonium.classFromType
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

import scala.reflect.runtime.universe.{Try => _, _}
import scala.util.Try

object ImmutableObjectStorage {
  type TrancheId = UUID

  type ObjectReferenceId = Int

  case class TrancheOfData(serializedRepresentation: Array[Byte],
                           minimumObjectReferenceId: ObjectReferenceId)

  type EitherThrowableOr[X] = Either[Throwable, X]

  trait Tranches {
    def createTrancheInStorage(serializedRepresentation: Array[Byte],
                               objectReferenceIds: Seq[ObjectReferenceId])
      : EitherThrowableOr[TrancheId]

    def retrieveTranche(id: TrancheId): EitherThrowableOr[TrancheOfData]
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

  def run(session: Session[Unit], tranches: Tranches): Unit = {
    object sessionInterpreter extends FunctionK[Operation, EitherThrowableOr] {
      val kryoPool: KryoPool =
        ScalaKryoInstantiator.defaultPool

      override def apply[X](operation: Operation[X]): EitherThrowableOr[X] =
        operation match {
          case Store(immutableObject) => {
            val serializedRepresentation: Array[Byte] =
              kryoPool.toBytesWithClass(immutableObject)

            tranches
              .createTrancheInStorage(serializedRepresentation,
                                      Seq.empty /* TODO */ )
          }
          case retrieve @ Retrieve(trancheId) =>
            for {
              tranche <- tranches.retrieveTranche(trancheId)
              result <- Try {
                val deserialized =
                  kryoPool.fromBytes(tranche.serializedRepresentation)
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
