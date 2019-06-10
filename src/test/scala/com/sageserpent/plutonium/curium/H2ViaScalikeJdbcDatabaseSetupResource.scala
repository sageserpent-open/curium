package com.sageserpent.plutonium.curium

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}
import java.util.UUID

import cats.effect.{IO, Resource}
import com.zaxxer.hikari.HikariDataSource

import scalikejdbc._

trait ConnectionPoolResource {
  def connectionPoolResource: Resource[IO, ConnectionPool] =
    for {
      databaseDirectory <- Resource.make(IO {
        Files.createTempDirectory("h2Storage")
      })(cleanupDatabaseDirectory)
      databaseName <- Resource.liftF(IO { UUID.randomUUID().toString })
      dataSource <- Resource.make(IO {
        val result = new HikariDataSource()
        result.setJdbcUrl(
          s"jdbc:h2:file:${databaseDirectory.resolve(databaseName)};DB_CLOSE_ON_EXIT=FALSE;CACHE_SIZE=524288")
        result.setUsername("automatedTestIdentity")
        result
      })(dataSource => IO { dataSource.close() })
      connectionPool <- Resource.make(IO {
        new DataSourceConnectionPool(dataSource)
      })(dropDatabaseTables)
    } yield connectionPool

  private def dropDatabaseTables(
      connectionPool: DataSourceConnectionPool): IO[Unit] = {
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

  private def cleanupDatabaseDirectory(directory: Path): IO[Unit] = {
    IO {
      Files.walkFileTree(
        directory,
        new FileVisitor[Path] {
          override def preVisitDirectory(
              dir: Path,
              attrs: BasicFileAttributes): FileVisitResult =
            FileVisitResult.CONTINUE

          override def visitFile(
              file: Path,
              attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def visitFileFailed(file: Path,
                                       exc: IOException): FileVisitResult =
            FileVisitResult.CONTINUE

          override def postVisitDirectory(dir: Path,
                                          exc: IOException): FileVisitResult = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }
  }
}

trait H2ViaScalikeJdbcDatabaseSetupResource extends ConnectionPoolResource {
  override def connectionPoolResource: Resource[IO, ConnectionPool] =
    for {
      connectionPool <- super.connectionPoolResource
      _ <- Resource.make(
        H2ViaScalikeJdbcTranches.setupDatabaseTables(connectionPool))(_ =>
        IO {})
    } yield connectionPool
}
