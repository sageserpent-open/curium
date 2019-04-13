package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.effect.IO
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  TrancheOfData,
  Tranches
}

object H2Tranches {
  type Transactor = doobie.util.transactor.Transactor[IO]

  def setupDatabaseTables(transactor: Transactor): IO[Unit] = ???
}

class H2Tranches(transactor: H2Tranches.Transactor) extends Tranches[UUID] {
  override def createTrancheInStorage(
      payload: Array[Byte],
      objectReferenceIdOffset: ObjectReferenceId,
      objectReferenceIds: Set[ObjectReferenceId])
    : EitherThrowableOr[TrancheId] = ???

  override def objectReferenceIdOffsetForNewTranche
    : EitherThrowableOr[ObjectReferenceId] = ???

  override def retrieveTranche(
      trancheId: TrancheId): EitherThrowableOr[TrancheOfData] = ???

  override def retrieveTrancheId(
      objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] =
    ???
}
