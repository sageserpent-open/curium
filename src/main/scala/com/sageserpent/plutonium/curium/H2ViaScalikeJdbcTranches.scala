package com.sageserpent.plutonium.curium

import cats.effect.IO
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  TrancheOfData,
  Tranches
}
import scalikejdbc._

import scala.util.Try

object H2ViaScalikeJdbcTranches {
  def setupDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
             CREATE TABLE Tranche(
                trancheId	              IDENTITY  PRIMARY KEY,
                payload		              BLOB      NOT NULL,
                objectReferenceIdOffset INTEGER   NOT NULL
             )
      """.update.apply()
            sql"""
             CREATE TABLE ObjectReference(
                objectReferenceId	INTEGER		PRIMARY KEY,
                trancheId			    BIGINT  	REFERENCES Tranche(trancheId)
             )
         """.update.apply()
          }
      })

  def dropDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
             DROP ALL OBJECTS
         """.update.apply()
          }
      })
}

class H2ViaScalikeJdbcTranches(connectionPool: ConnectionPool)
    extends Tranches[Long] {
  override def createTrancheInStorage(
      payload: Array[Byte],
      objectReferenceIdOffset: ObjectReferenceId,
      objectReferenceIds: Set[ObjectReferenceId])
    : EitherThrowableOr[TrancheId] =
    Try {
      DBResource(connectionPool)
        .use(db =>
          IO {
            db localTx {
              implicit session: DBSession =>
                val trancheId: TrancheId = sql"""
          INSERT INTO Tranche(payload, objectReferenceIdOffset) VALUES ($payload, $objectReferenceIdOffset)
       """.updateAndReturnGeneratedKey("trancheId")
                  .apply()

                val _ = sql"""
          INSERT INTO ObjectReference(objectReferenceId, trancheId) VALUES (?, ?)
         """.batch(objectReferenceIds.toSeq map (objectReferenceId =>
                    Seq(objectReferenceId, trancheId.toString)): _*)
                  .apply()

                trancheId
            }
        })
        .unsafeRunSync()
    }.toEither

  override def objectReferenceIdOffsetForNewTranche
    : EitherThrowableOr[ObjectReferenceId] =
    Try {
      DBResource(connectionPool)
        .use(db =>
          IO {
            db localTx { implicit session: DBSession =>
              sql"""
          SELECT MAX(objectReferenceId) FROM ObjectReference
       """.map(_.intOpt(1)
                  .fold(0)(100 + _) /* TODO - switch back to an offset of 1. */ )
                .single()
                .apply()
                .get
            }
        })
        .unsafeRunSync()
    }.toEither

  override def retrieveTranche(
      trancheId: TrancheId): EitherThrowableOr[TrancheOfData] =
    Try {
      DBResource(connectionPool)
        .use(db =>
          IO {
            db localTx {
              implicit session: DBSession =>
                sql"""
          SELECT payload, objectReferenceIdOffset FROM Tranche WHERE $trancheId = TrancheId
       """.map(resultSet =>
                    TrancheOfData(payload = resultSet.bytes("payload"),
                                  objectReferenceIdOffset =
                                    resultSet.int("objectReferenceIdOffset")))
                  .single()
                  .apply()
                  .get
            }
        })
        .unsafeRunSync()
    }.toEither

  override def retrieveTrancheId(
      objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] =
    Try {
      DBResource(connectionPool)
        .use(db =>
          IO {
            db localTx { implicit session: DBSession =>
              sql"""
           SELECT trancheId FROM ObjectReference WHERE $objectReferenceId = objectReferenceId
         """.map(_.long("trancheId")).single().apply().get
            }
        })
        .unsafeRunSync()
    }.toEither
}
