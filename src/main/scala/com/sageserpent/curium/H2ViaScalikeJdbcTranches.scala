package com.sageserpent.curium

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.sageserpent.curium.ImmutableObjectStorage.{
  TrancheLocalObjectReferenceId,
  TrancheOfData,
  Tranches
}
import scalikejdbc._

object H2ViaScalikeJdbcTranches {
  def setupDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
             CREATE TABLE Tranche(
                trancheId   IDENTITY        PRIMARY KEY,
                payload     BINARY VARYING  NOT NULL
             )
      """.update.apply()
            sql"""
             CREATE TABLE InterTrancheObjectReferenceIdTranslation(
                referringTrancheId                      BIGINT,
                referringTrancheLocalObjectReferenceId  INTEGER,
                homeTrancheId                           BIGINT,
                homeTrancheLocalObjectReferenceId       INTEGER,
                PRIMARY KEY(referringTrancheId, referringTrancheLocalObjectReferenceId)
             )
         """.update.apply()
          }
        }
      )
}

class H2ViaScalikeJdbcTranches(connectionPool: ConnectionPool)
    extends Tranches[Long] {
  override def createTrancheInStorage(
      tranche: TrancheOfData[TrancheId]
  ): TrancheId =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            val trancheId: TrancheId =
              sql"""
          INSERT INTO Tranche(payload) VALUES (${tranche.payload})
       """.updateAndReturnGeneratedKey("trancheId")
                .apply()

            val _ =
              sql"""
          INSERT INTO InterTrancheObjectReferenceIdTranslation(referringTrancheId, referringTrancheLocalObjectReferenceId, homeTrancheId, homeTrancheLocalObjectReferenceId) VALUES (?, ?, ?, ?)
         """.batch(
                tranche.interTrancheObjectReferenceIdTranslation.toSeq map {
                  case (
                        referringTrancheLocalObjectReferenceId,
                        (homeTrancheId, homeTranceLocalObjectReferenceId)
                      ) =>
                    Seq(
                      trancheId,
                      referringTrancheLocalObjectReferenceId,
                      homeTrancheId,
                      homeTranceLocalObjectReferenceId
                    )
                }: _*
              ).apply()

            trancheId
          }
        }
      )
      .unsafeRunSync()

  override def retrieveTranche(
      trancheId: TrancheId
  ): TrancheOfData[TrancheId] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            (for {
              payload <-
                sql"""
          SELECT payload FROM Tranche WHERE $trancheId = TrancheId
       """.map(_.bytes("payload")).single()

              interTrancheObjectReferenceIdTranslation =
                sql"""SELECT referringTrancheLocalObjectReferenceId, homeTrancheId, homeTrancheLocalObjectReferenceId FROM InterTrancheObjectReferenceIdTranslation WHERE referringTrancheId = $trancheId"""
                  .map { resultSet =>
                    val referringTrancheLocalObjectReferenceId
                        : TrancheLocalObjectReferenceId =
                      resultSet.int("referringTrancheLocalObjectReferenceId")
                    val homeTrancheId: TrancheId =
                      resultSet.long("homeTrancheId")
                    val homeTrancheLocalObjectReferenceId =
                      resultSet.int("homeTrancheLocalObjectReferenceId")

                    referringTrancheLocalObjectReferenceId -> (homeTrancheId, homeTrancheLocalObjectReferenceId)
                  }
                  .toIterable()
                  .toMap
            } yield TrancheOfData(
              payload = payload,
              interTrancheObjectReferenceIdTranslation =
                interTrancheObjectReferenceIdTranslation
            )).get
          }
        }
      )
      .unsafeRunSync()
}
