package com.sageserpent.plutonium.curium

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}
import java.util.UUID

import cats.effect.{IO, Resource}
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import scalikejdbc.{
  Commons2ConnectionPool,
  ConnectionPool,
  DataSourceConnectionPool
}

trait ConnectionPoolResource {
  def connectionPoolResource: Resource[IO, ConnectionPool] =
    for {
      databaseDirectory <- Resource.make(IO {
        Files.createTempDirectory("h2Storage")
      })(directory =>
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

              override def postVisitDirectory(
                  dir: Path,
                  exc: IOException): FileVisitResult = {
                Files.delete(dir)
                FileVisitResult.CONTINUE
              }
            }
          )
      })
      databaseName <- Resource.liftF(IO { UUID.randomUUID().toString })
      dataSource <- Resource.make(IO {
        val result = new HikariDataSource()
        result.setJdbcUrl(
          s"jdbc:h2:file:${databaseDirectory.resolve(databaseName)}")
        result.setUsername("automatedTestIdentity")
        result
      })(dataSource => IO { dataSource.close() })
      connectionPool <- Resource.make(IO {
        new DataSourceConnectionPool(dataSource)
      })(_ => IO {})
    } yield connectionPool
}

trait H2ViaScalikeJdbcDatabaseSetupResource extends ConnectionPoolResource {
  override def connectionPoolResource: Resource[IO, ConnectionPool] =
    for {
      connectionPool <- super.connectionPoolResource
      _ <- Resource.make(
        H2ViaScalikeJdbcTranches.setupDatabaseTables(connectionPool))(_ =>
        H2ViaScalikeJdbcTranches.dropDatabaseTables(connectionPool))
    } yield connectionPool
}
