package com.sageserpent.plutonium.curium

import java.util.UUID

import doobie._
import doobie.implicits._

import cats.effect.IO
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  TrancheOfData,
  Tranches
}

object H2Tranches {
  type Transactor = doobie.util.transactor.Transactor[IO]

  def setupDatabaseTables(transactor: Transactor): IO[Unit] = {
    val trancheCreation: ConnectionIO[Int] = sql"""
                             CREATE TABLE Tranche(
                                trancheId	IDENTITY	PRIMARY KEY,
                                payload		BLOB		  NOT NULL
                             )
      """.update.run

    val objectReferenceIdCreation: ConnectionIO[Int] =
      sql"""
           CREATE TABLE ObjectReferenceId(
              objectReferenceId	INTEGER		PRIMARY KEY,
           	  trancheId			    BIGINT  	NOT NULL
           )
         """.update.run

    val setup: doobie.ConnectionIO[Unit] = for {
      _ <- trancheCreation
      _ <- objectReferenceIdCreation
    } yield {}

    setup.transact(transactor)
  }
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
