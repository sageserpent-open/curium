package com.sageserpent.plutonium.curium
import java.util.UUID

import cats.Monad

import scala.reflect.runtime.universe._
import scala.util.Try

object ImmutableObjectStorage {
  type Id = UUID
}

trait ImmutableObjectStorage[F[_]] {
  implicit val monadEvidence: Monad[F]

  // Imperative...
  def store[X: TypeTag](immutableObject: X): F[ImmutableObjectStorage.Id]

  // Imperative...
  def retrieve[X: TypeTag](id: ImmutableObjectStorage.Id): F[X]
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
  def retrieve(referenceId: ImmutableObjectStorage.Id): F[Try[TrancheOfData]]

  // Imperative...
  def store(tranche: TrancheOfData): F[Try[ImmutableObjectStorage.Id]]
}
