package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.effect.{IO, Resource}
import doobie._
import doobie.h2._

import scala.concurrent.ExecutionContext

object H2ViaDoobieResource {
  implicit val cs = IO.contextShift(ExecutionContext.global)

  val transactorResource: Resource[IO, H2ViaDoobieTranches.Transactor] = for {
    databaseName    <- Resource.liftF(IO { UUID.randomUUID().toString })
    fixedThreadPool <- ExecutionContexts.fixedThreadPool[IO](32)
    cachedThreadPool <- ExecutionContexts
      .cachedThreadPool[IO]
    transactor <- H2Transactor.newH2Transactor[IO](
      url = s"jdbc:h2:mem:$databaseName",
      user = "automatedTestIdentity",
      pass = "",
      connectEC = fixedThreadPool,
      transactEC = cachedThreadPool
    )
    _ <- Resource.make(H2ViaDoobieTranches.setupDatabaseTables(transactor))(_ =>
      H2ViaDoobieTranches.dropDatabaseTables(transactor))
  } yield transactor
}
