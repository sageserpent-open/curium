package com.sageserpent.plutonium.curium

import java.util.concurrent.ConcurrentMap

import alleycats.std.all._
import cats.effect.IO
import com.google.common.collect.MapMaker
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  TrancheOfData,
  Tranches
}
import doobie._
import doobie.implicits._

import scala.concurrent.duration._
import scala.util.Try

object H2ViaDoobieTranches {
  type Transactor = doobie.util.transactor.Transactor[IO]

  val trancheCreation: ConnectionIO[Int] = sql"""
                             CREATE TABLE Tranche(
                                trancheId	              IDENTITY  PRIMARY KEY,
                                payload		              BLOB      NOT NULL,
                                objectReferenceIdOffset INTEGER   NOT NULL
                             )
      """.update.run

  val objectReferenceIdCreation: ConnectionIO[Int] =
    sql"""
           CREATE TABLE ObjectReference(
              objectReferenceId	INTEGER		PRIMARY KEY,
           	  trancheId			    BIGINT  	REFERENCES Tranche(trancheId)
           )
         """.update.run

  val objectReferenceIdIndexCreation: ConnectionIO[Int] =
    sql"""
         CREATE INDEX ObjectReferenceIdIndex ON ObjectReference(objectReferenceId)
       """.update.run

  def setupDatabaseTables(transactor: Transactor): IO[Unit] = {

    val setup: ConnectionIO[Unit] = for {
      _ <- trancheCreation
      _ <- objectReferenceIdCreation
      _ <- objectReferenceIdIndexCreation
    } yield {}

    setup.transact(transactor)
  }

  def dropDatabaseTables(transactor: Transactor): IO[Unit] = {
    val dropAll: ConnectionIO[Unit] = for {
      _ <- sql"""
           DROP ALL OBJECTS
         """.update.run
    } yield {}

    dropAll.transact(transactor)
  }

  val objectReferenceIdOffsetForNewTrancheQuery
    : ConnectionIO[ObjectReferenceId] =
    sql"""
          SELECT MAX(objectReferenceId) FROM ObjectReference
       """
      .query[Option[ObjectReferenceId]]
      .unique
      .map(_.fold(0)(100 + _)) // TODO - switch back to an offset of 1.
}

class H2ViaDoobieTranches(transactor: H2ViaDoobieTranches.Transactor)
    extends Tranches[Long] {
  import H2ViaDoobieTranches.objectReferenceIdOffsetForNewTrancheQuery

  override def createTrancheInStorage(
      payload: Array[Byte],
      objectReferenceIdOffset: ObjectReferenceId,
      objectReferenceIds: Set[ObjectReferenceId])
    : EitherThrowableOr[TrancheId] = {
    val insertion: ConnectionIO[TrancheId] = for {
      trancheId <- sql"""
          INSERT INTO Tranche(payload, objectReferenceIdOffset) VALUES ($payload, $objectReferenceIdOffset)
       """.update
        .withUniqueGeneratedKeys[Long]("trancheId")

      _ <- Update[(ObjectReferenceId, TrancheId)](
        """
          INSERT INTO ObjectReference(objectReferenceId, trancheId) VALUES (?, ?)
         """).updateMany(objectReferenceIds map (_ -> trancheId))
    } yield trancheId

    Try { insertion.transact(transactor).unsafeRunSync }.toEither
  }

  override def objectReferenceIdOffsetForNewTranche
    : EitherThrowableOr[ObjectReferenceId] =
    Try {
      objectReferenceIdOffsetForNewTrancheQuery
        .transact(transactor)
        .unsafeRunSync
    }.toEither

  override def retrieveTranche(
      trancheId: TrancheId): EitherThrowableOr[TrancheOfData] = {
    val trancheOfDataQuery: ConnectionIO[TrancheOfData] =
      sql"""
          SELECT payload, objectReferenceIdOffset FROM Tranche WHERE $trancheId = TrancheId 
       """.query[TrancheOfData].unique

    Try { trancheOfDataQuery.transact(transactor).unsafeRunSync }.toEither
  }

  override def retrieveTrancheId(
      objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] = {
    val trancheIdQuery: ConnectionIO[TrancheId] =
      sql"""
             SELECT trancheId FROM ObjectReference WHERE $objectReferenceId = objectReferenceId
           """.query[TrancheId].unique

    Try { trancheIdQuery.transact(transactor).unsafeRunSync }.toEither
  }
}
