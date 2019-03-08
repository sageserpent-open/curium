package com.sageserpent.plutonium.curium
import java.util.UUID

import cats.arrow.FunctionK
import cats.free.FreeT
import cats.implicits._
import com.esotericsoftware.kryo.ReferenceResolver
import com.esotericsoftware.kryo.util.MapReferenceResolver
import com.sageserpent.plutonium.classFromType
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}

import scala.collection.mutable.{SortedMap => MutableSortedMap}
import scala.reflect.runtime.universe.{Try => _, _}
import scala.util.{DynamicVariable, Try}

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

  def runStorage(session: Session[Vector[TrancheId]])
    : Tranches => EitherThrowableOr[Vector[TrancheId]] = run(session) _

  def runRetrieval(
      session: Session[Unit]): Tranches => EitherThrowableOr[Unit] =
    run(session) _

  private def run[Result](session: Session[Result])(
      tranches: Tranches): EitherThrowableOr[Result] = {
    object sessionInterpreter extends FunctionK[Operation, EitherThrowableOr] {
      val retrievalSessionReferenceResolver
        : DynamicVariable[Option[ReferenceResolver]] = new DynamicVariable(None)

      object referenceResolver extends MapReferenceResolver {
        override def getReadObject(
            `type`: Class[_],
            objectReferenceId: ObjectReferenceId): AnyRef =
          retrievalSessionReferenceResolver.value.get
            .getReadObject(`type`, objectReferenceId)

        override def nextReadId(`type`: Class[_]): ObjectReferenceId =
          retrievalSessionReferenceResolver.value.get.nextReadId(`type`)
      }

      // TODO - cutover to using weak references, perhaps via 'WeakCache'?
      val refererenceResolversByTrancheId
        : MutableSortedMap[TrancheId, ReferenceResolver] =
        MutableSortedMap.empty

      val kryoInstantiator: ScalaKryoInstantiator = new ScalaKryoInstantiator {
        override def newKryo(): KryoBase = {
          val result = super.newKryo()

          result.setReferenceResolver(referenceResolver)

          result.setAutoReset(false)

          result
        }
      }

      val kryoPool: KryoPool =
        KryoPool.withByteArrayOutputStream(40, kryoInstantiator)

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
                object trancheSpecificReferenceResolver
                    extends MapReferenceResolver {
                  override def getReadObject(
                      clazz: Class[_],
                      objectReferenceId: ObjectReferenceId): AnyRef =
                    if (objectReferenceId >= tranche.minimumObjectReferenceId)
                      super.getReadObject(clazz, objectReferenceId)
                    else
                      proxyFor(objectReferenceId, clazz)
                }

                val deserialized =
                  retrievalSessionReferenceResolver.withValue(
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

      private def proxyFor(objectReferenceId: ObjectReferenceId,
                           clazz: Class[_]): AnyRef = ???
    }

    session.foldMap(sessionInterpreter)
  }
}
