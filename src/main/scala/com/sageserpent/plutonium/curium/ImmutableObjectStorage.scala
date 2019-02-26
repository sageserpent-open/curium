package com.sageserpent.plutonium.curium
import java.util.UUID

import cats.Monad
import cats.data.EitherT
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.Id

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object ImmutableObjectStorage {
  type Id = UUID
}

trait ImmutableObjectStorage[F[_]] {
  // TODO - double-check that this is needed here. I'm presuming that 'store' and 'retrieve' will indeed be implemented to use a for-comprehension....
  implicit val monadEvidence: Monad[F]

  // Imperative...
  def store[X: TypeTag](
      immutableObject: X): EitherT[F, Throwable, ImmutableObjectStorage.Id]

  // Imperative...
  def retrieve[X: TypeTag](
      id: ImmutableObjectStorage.Id): EitherT[F, Throwable, X]
}

trait ImmutableObjectStorageImplementation[F[_]]
    extends ImmutableObjectStorage[F] {
  this: Tranches[F] =>

  override def store[X: universe.TypeTag](
      immutableObject: X): EitherT[F, Throwable, Id] = ???

  override def retrieve[X: universe.TypeTag](id: Id): EitherT[F, Throwable, X] =
    ???
}

object TrancheOfData {
  // DO WE NEED THIS YET?
  type ObjectReferenceId = Int
  // TODO - need to be able to create a tranche from an object.
}

trait TrancheOfData // TODO - add operations, somehow we need to be able to reconstitute an object conforming to a type.

trait Tranches[F[_]] {
  implicit val monadEvidence: Monad[F]

  // Imperative...
  def retrieve(
      id: ImmutableObjectStorage.Id): EitherT[F, Throwable, TrancheOfData]

  // Imperative...
  def store(
      tranche: TrancheOfData): EitherT[F, Throwable, ImmutableObjectStorage.Id]
}
