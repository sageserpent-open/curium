package com.sageserpent.plutonium.curium

import java.util.{Map => JavaMap}

import cats.effect.{IO, Resource}
import com.sageserpent.plutonium.caffeineBuilder
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  TrancheOfData,
  Tranches
}
import scalikejdbc._

import scala.util.Try

object H2ViaScalikeJdbcTranches {
  def dbResource(connectionPool: ConnectionPool): Resource[IO, DB] =
    Resource.make(IO { DB(connectionPool.borrow()) })(db => IO { db.close })

  def setupDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    dbResource(connectionPool)
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
            sql"""
         CREATE INDEX ObjectReferenceIdIndex ON ObjectReference(objectReferenceId)
       """.update.apply()
          }
      })

  def dropDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    dbResource(connectionPool)
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
  import H2ViaScalikeJdbcTranches.dbResource

  override def createTrancheInStorage(
      payload: Array[Byte],
      objectReferenceIdOffset: ObjectReferenceId,
      objectReferenceIds: Set[ObjectReferenceId])
    : EitherThrowableOr[TrancheId] =
    Try {
      dbResource(connectionPool)
        .use(db =>
          IO {
            db localTx {
              implicit session: DBSession =>
                val trancheId: TrancheId = sql"""
          INSERT INTO Tranche(payload, objectReferenceIdOffset) VALUES ($payload, $objectReferenceIdOffset)
       """.map(_.long("trancheId"))
                  .updateAndReturnGeneratedKey
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
      dbResource(connectionPool)
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
      dbResource(connectionPool)
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
      dbResource(connectionPool)
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

  private val referenceIdToObjectCacheBackedMap
    : JavaMap[ObjectReferenceId, AnyRef] =
    caffeineBuilder()
      .softValues()
      .build[ObjectReferenceId, AnyRef]()
      .asMap

  override def noteObject(objectReferenceId: ObjectReferenceId,
                          immutableObject: AnyRef): Unit = {
    referenceIdToObjectCacheBackedMap.put(objectReferenceId, immutableObject)
  }

  override def objectFor(
      objectReferenceId: ObjectReferenceId): Option[AnyRef] = {
    Option(referenceIdToObjectCacheBackedMap.get(objectReferenceId))
  }

  private val objectToReferenceIdCacheBackedMap
    : JavaMap[AnyRef, ObjectReferenceId] =
    caffeineBuilder()
      .weakKeys()
      .build[AnyRef, ObjectReferenceId]()
      .asMap

  override def noteReferenceId(immutableObject: AnyRef,
                               objectReferenceId: ObjectReferenceId): Unit = {
    objectToReferenceIdCacheBackedMap.put(immutableObject, objectReferenceId)
  }

  override def referenceIdFor(
      immutableObject: AnyRef): Option[ObjectReferenceId] =
    Option(objectToReferenceIdCacheBackedMap.get(immutableObject))

  private val trancheToTopLevelObjectCacheBackedMap
    : JavaMap[TrancheId, AnyRef] =
    caffeineBuilder()
      .softValues()
      .build[TrancheId, AnyRef]()
      .asMap

  override def noteTopLevelObject(trancheId: TrancheId,
                                  topLevelObject: AnyRef): Unit = {
    trancheToTopLevelObjectCacheBackedMap.put(trancheId, topLevelObject)
  }

  override def topLevelObjectFor(trancheId: TrancheId): Option[AnyRef] = {
    Option(trancheToTopLevelObjectCacheBackedMap.get(trancheId))
  }
}
