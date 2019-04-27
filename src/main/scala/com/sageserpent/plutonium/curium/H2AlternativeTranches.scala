package com.sageserpent.plutonium.curium

import java.util.concurrent.{ConcurrentMap, TimeUnit}

import cats.effect.{IO, Resource}
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.collect.MapMaker
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  TrancheOfData,
  Tranches
}
import scalacache._
import scalacache.caffeine._
import scalacache.modes.sync._
import scalikejdbc._

import scala.util.Try

object H2AlternativeTranches {
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

class H2AlternativeTranches(connectionPool: ConnectionPool)
    extends Tranches[Long] {
  import H2AlternativeTranches.dbResource

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

  private val objectCacheByReferenceIdTimeToLive = None

  private val objectCacheByReferenceId: Cache[AnyRef] =
    CaffeineCache[AnyRef](
      Caffeine
        .newBuilder()
        /*        .maximumSize(100000L)
        .expireAfterAccess(1, TimeUnit.MINUTES)*/
        .build[String, Entry[AnyRef]])

  override def noteObject(objectReferenceId: ObjectReferenceId,
                          immutableObject: AnyRef): Unit = {
    implicit val cache = objectCacheByReferenceId

    sync.put(objectReferenceId)(immutableObject,
                                objectCacheByReferenceIdTimeToLive)
  }

  override def objectFor(
      objectReferenceId: ObjectReferenceId): Option[AnyRef] = {
    implicit val cache = objectCacheByReferenceId

    sync.get(objectReferenceId)
  }

  private val weakObjectToReferenceIdMap
    : ConcurrentMap[AnyRef, ObjectReferenceId] =
    new MapMaker().weakKeys().makeMap[AnyRef, ObjectReferenceId]()

  override def noteReferenceId(immutableObject: AnyRef,
                               objectReferenceId: ObjectReferenceId): Unit = {
    weakObjectToReferenceIdMap.put(immutableObject, objectReferenceId)
  }

  override def referenceIdFor(
      immutableObject: AnyRef): Option[ObjectReferenceId] =
    Option(weakObjectToReferenceIdMap.get(immutableObject))

  private val topLevelObjectCacheByTrancheIdTimeToLive = None

  private val topLevelObjectCacheByTrancheId: Cache[AnyRef] =
    CaffeineCache[AnyRef](
      Caffeine
        .newBuilder()
        /*        .maximumSize(10000L)
        .expireAfterAccess(1, TimeUnit.MINUTES)*/
        .build[String, Entry[AnyRef]])

  override def noteTopLevelObject(trancheId: TrancheId,
                                  topLevelObject: AnyRef): Unit = {
    implicit val cache = topLevelObjectCacheByTrancheId

    sync.put(trancheId)(topLevelObject,
                        topLevelObjectCacheByTrancheIdTimeToLive)
  }

  override def topLevelObjectFor(trancheId: TrancheId): Option[AnyRef] = {
    implicit val cache = topLevelObjectCacheByTrancheId

    sync.get(trancheId)
  }

}
