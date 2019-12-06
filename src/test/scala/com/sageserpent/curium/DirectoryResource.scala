package com.sageserpent.curium

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}

import cats.effect.{IO, Resource}

trait DirectoryResource {
  def directoryResource(prefix: String): Resource[IO, Path] =
    Resource.make(IO {
      Files.createTempDirectory(prefix)
    })(cleanupDatabaseDirectory)

  private def cleanupDatabaseDirectory(directory: Path): IO[Unit] = {
    IO {
      Files.walkFileTree(
        directory,
        new FileVisitor[Path] {
          override def preVisitDirectory(dir: Path,
                                         attrs: BasicFileAttributes): FileVisitResult =
            FileVisitResult.CONTINUE

          override def visitFile(file: Path,
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
