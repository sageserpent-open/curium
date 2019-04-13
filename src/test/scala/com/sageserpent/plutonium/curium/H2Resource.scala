package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.effect.{IO, Resource}
import doobie._
import doobie.hikari._

import scala.concurrent.ExecutionContext

object H2Resource {
  implicit val cs = IO.contextShift(ExecutionContext.global)

  val transactorResource: Resource[IO, H2Tranches.Transactor] = for {
    databaseName    <- Resource.liftF(IO { UUID.randomUUID().toString })
    fixedThreadPool <- ExecutionContexts.fixedThreadPool[IO](32)
    cachedThreadPool <- ExecutionContexts
      .cachedThreadPool[IO]
    // TODO - use the H2 flavour of transactor, as it has its own connection pool implementation...
    transactor <- HikariTransactor.newHikariTransactor[IO](
      driverClassName = "org.h2.Driver",
      url = s"jdbc:h2:mem:$databaseName;DB_CLOSE_DELAY=-1",
      user = "automatedTestIdentity",
      pass = "",
      connectEC = fixedThreadPool,
      transactEC = cachedThreadPool
    )
    _ <- Resource.liftF(H2Tranches.setupDatabaseTables(transactor))
  } yield transactor

}
