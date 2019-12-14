package com.sageserpent.curium

import cats.effect.{IO, Resource}
import org.rocksdb.{BlockBasedTableConfig, BloomFilter, Options, RocksDB}

trait RocksDbResource extends DirectoryResource {
  def rocksDbResource: Resource[IO, RocksDB] = for {
    databaseDirectory <- directoryResource("rocksDB")
    rocksDb <- Resource.fromAutoCloseable(IO {
      RocksDB.loadLibrary()
      val blockBasedTableConfig = new BlockBasedTableConfig
      blockBasedTableConfig.setFilterPolicy(new BloomFilter(10, false))
      RocksDB.open(new Options().setCreateIfMissing(true).setTableFormatConfig(blockBasedTableConfig), databaseDirectory.toAbsolutePath.toString)
    })
  } yield rocksDb
}
