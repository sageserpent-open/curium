package com.sageserpent.plutonium.curium

import cats.effect.{IO, Resource}
import scalikejdbc.{ConnectionPool, DB}

object DBResource {
  def apply(connectionPool: ConnectionPool): Resource[IO, DB] =
    Resource.make(IO { DB(connectionPool.borrow()) })(dbConnection =>
      IO { dbConnection.close })
}
