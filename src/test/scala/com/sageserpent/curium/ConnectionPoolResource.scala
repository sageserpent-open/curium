package com.sageserpent.curium

import cats.effect.{IO, Resource}
import com.zaxxer.hikari.HikariDataSource
import scalikejdbc._

import java.util.UUID

trait ConnectionPoolResource extends DirectoryResource {
  def connectionPoolResource: Resource[IO, ConnectionPool] =
    for {
      databaseDirectory <- directoryResource(prefix = "h2Storage")
      databaseName <- Resource.liftK(IO {
        UUID.randomUUID().toString
      })
      dataSource <- Resource.make(IO {
        val result = new HikariDataSource()
        result.setJdbcUrl(
          s"jdbc:h2:file:${databaseDirectory.resolve(databaseName)};DB_CLOSE_ON_EXIT=FALSE;MV_STORE=FALSE;ANALYZE_AUTO=5000;ANALYZE_SAMPLE=50000")
        result.setUsername("automatedTestIdentity")
        result
      })(dataSource => IO {
        dataSource.close()
      })
      connectionPool <- Resource.make(IO {
        new DataSourceConnectionPool(dataSource)
      })(dropDatabaseTables)
    } yield connectionPool

  private def dropDatabaseTables(connectionPool: DataSourceConnectionPool): IO[Unit] = {
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
}
