package com.sageserpent.curium

import cats.effect.{IO, Resource}
import doobie._
import doobie.h2._

import java.util.UUID

object H2ViaDoobieResource {
  val transactorResource: Resource[IO, H2ViaDoobieTranches.Transactor] = for {
    databaseName <- Resource.liftK(IO {
      UUID.randomUUID().toString
    })
    fixedThreadPool <- ExecutionContexts.fixedThreadPool[IO](32)
    transactor <- H2Transactor.newH2Transactor[IO](
      url = s"jdbc:h2:mem:$databaseName",
      user = "automatedTestIdentity",
      pass = "",
      connectEC = fixedThreadPool
    )
    _ <- Resource.make(H2ViaDoobieTranches.setupDatabaseTables(transactor))(_ =>
      H2ViaDoobieTranches.dropDatabaseTables(transactor))
  } yield transactor
}
