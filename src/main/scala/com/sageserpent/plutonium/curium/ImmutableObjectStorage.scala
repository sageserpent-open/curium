package com.sageserpent.plutonium.curium
import java.util.UUID

import cats.Monad
import cats.data.EitherT
import com.esotericsoftware.kryo.ReferenceResolver
import com.sageserpent.plutonium.classFromType
import com.sageserpent.plutonium.curium.ImmutableObjectStorage._
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Try => _, _}
import scala.util.{DynamicVariable, Try}

object ImmutableObjectStorage {
  type TrancheId = UUID

  type ObjectReferenceId = Int

  case class TrancheOfData(serializedRepresentation: Array[Byte],
                           minimumObjectReferenceId: ObjectReferenceId)
}

trait ImmutableObjectStorage[F[_]] {
  implicit val monadEvidence: Monad[F]

  // Imperative...
  def store[X: TypeTag](immutableObject: X): EitherT[F, Throwable, TrancheId]

  // Imperative...
  def retrieve[X: TypeTag](id: TrancheId): EitherT[F, Throwable, X]
}

abstract class ImmutableObjectStorageImplementation[F[_]](
    override implicit val monadEvidence: Monad[F])
    extends ImmutableObjectStorage[F] {
  this: Tranches[F] =>

  val retrievalSessionReferenceResolver
    : DynamicVariable[Option[ReferenceResolver]] = new DynamicVariable(None)
  val kryoPool: KryoPool =
    ScalaKryoInstantiator.defaultPool

  override def store[X: universe.TypeTag](
      immutableObject: X): EitherT[F, Throwable, TrancheId] = {
    val serializedRepresentation: Array[Byte] =
      kryoPool.toBytesWithClass(immutableObject)

    createTrancheInStorage(serializedRepresentation, Seq.empty /* TODO */ )
  }

  override def retrieve[X: universe.TypeTag](
      trancheId: TrancheId): EitherT[F, Throwable, X] = {
    val clazz: Class[X] = classFromType(typeOf[X])
    for {
      tranche <- retrieveTranche(trancheId)
      result <- EitherT.fromEither[F](Try {
        val deserialized = kryoPool.fromBytes(tranche.serializedRepresentation)

        clazz.cast(deserialized)
      }.toEither)
    } yield result
  }

  private def proxyFor(objectReferenceId: ObjectReferenceId,
                       clazz: Class[_]): AnyRef = ???
}

trait Tranches[F[_]] {
  def createTrancheInStorage(serializedRepresentation: Array[Byte],
                             objectReferenceIds: Seq[ObjectReferenceId])
    : EitherT[F, Throwable, TrancheId]

  def retrieveTranche(id: TrancheId): EitherT[F, Throwable, TrancheOfData]
  def retrieveTrancheId(
      objectReferenceId: ObjectReferenceId): EitherT[F, Throwable, TrancheId]
}
