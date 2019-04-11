package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.Id
import ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  TrancheOfData,
  Tranches
}

object H2Tranches {
  type Transactor = doobie.util.transactor.Transactor[Id]

  def setupDatabaseTables(transactor: Transactor): EitherThrowableOr[Unit] = ???
}

class H2Tranches[Payload](transactor: H2Tranches.Transactor)
    extends Tranches[UUID, Payload] {
  override def createTrancheInStorage(
      payload: Payload,
      objectReferenceIdOffset: ObjectReferenceId,
      objectReferenceIds: Set[ObjectReferenceId])
    : EitherThrowableOr[TrancheId] = ???

  override def objectReferenceIdOffsetForNewTranche
    : EitherThrowableOr[ObjectReferenceId] = ???

  override def retrieveTranche(
      trancheId: TrancheId): EitherThrowableOr[TrancheOfData[Payload]] = ???

  override def retrieveTrancheId(
      objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] =
    ???
}
