package com.sageserpent.curium

import cats.effect.{IO, Resource}
import scalikejdbc._


trait H2ViaScalikeJdbcDatabaseSetupResource extends ConnectionPoolResource {
  override def connectionPoolResource: Resource[IO, ConnectionPool] =
    for {
      connectionPool <- super.connectionPoolResource
      _ <- Resource.make(
        H2ViaScalikeJdbcTranches.setupDatabaseTables(connectionPool))(_ =>
        IO {})
    } yield connectionPool
}
