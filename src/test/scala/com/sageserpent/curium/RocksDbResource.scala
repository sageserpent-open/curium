package com.sageserpent.curium

import cats.effect.{IO, Resource}
import org.rocksdb.{Options, RocksDB}

trait RocksDbResource extends DirectoryResource {
  def rocksDbResource: Resource[IO, RocksDB] = for {
    databaseDirectory <- directoryResource("rocksDB")
    rocksDb <- Resource.fromAutoCloseable(IO {
      RocksDB.open(new Options().setCreateIfMissing(true), databaseDirectory.toAbsolutePath.toString)
    })
  } yield rocksDb
}
