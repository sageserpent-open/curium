package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.effect.{IO, Resource}
import scalikejdbc.{Commons2ConnectionPool, ConnectionPool}

object H2ViaScalikeJdbcResource {
  val connectionPoolResource: Resource[IO, ConnectionPool] = for {
    databaseName <- Resource.liftF(IO { UUID.randomUUID().toString })
    connectionPool <- Resource.make(IO {
      Class.forName("org.h2.Driver")
      new Commons2ConnectionPool(url = s"jdbc:h2:mem:$databaseName",
                                 user = "automatedTestIdentity",
                                 password = "")
    })(connectionPool => IO { connectionPool.close })
    _ <- Resource.make(
      H2ViaScalikeJdbcTranches.setupDatabaseTables(connectionPool))(_ =>
      H2ViaScalikeJdbcTranches.dropDatabaseTables(connectionPool))
  } yield connectionPool
}
