package com.sageserpent.curium

import com.google.common.primitives.{Ints, Longs}
import com.sageserpent.curium.ImmutableObjectStorage.{EitherThrowableOr, ObjectReferenceId, TrancheOfData, Tranches}
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyOptions, CompressionType, RocksDB}

import scala.util.Try

object RocksDbTranches {
  val trancheIdKeyPayloadValueColumnFamilyName = "TrancheIdKeyPayloadValue"
  val trancheIdKeyObjectReferenceIdOffsetValueFamilyName = "TrancheIdKeyObjectReferenceIdOffsetValue"
  val objectReferenceIdKeyTrancheIdValueColumnFamilyName = "ObjectReferenceIdKeyTrancheIdValue"
}

class RocksDbTranches(rocksDb: RocksDB) extends Tranches[Long] {

  import RocksDbTranches._

  val sharedColumnFamilyOptions = new ColumnFamilyOptions().setCompressionType(CompressionType.LZ4_COMPRESSION).setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)

  val trancheIdKeyPayloadValueColumnFamily =
    rocksDb.createColumnFamily(new ColumnFamilyDescriptor(trancheIdKeyPayloadValueColumnFamilyName.getBytes, sharedColumnFamilyOptions))
  val trancheIdKeyObjectReferenceIdOffsetValueFamily =
    rocksDb.createColumnFamily(new ColumnFamilyDescriptor(trancheIdKeyObjectReferenceIdOffsetValueFamilyName.getBytes, sharedColumnFamilyOptions))
  val objectReferenceIdKeyTrancheIdValueColumnFamily =
    rocksDb.createColumnFamily(new ColumnFamilyDescriptor(objectReferenceIdKeyTrancheIdValueColumnFamilyName.getBytes, sharedColumnFamilyOptions))

  override def createTrancheInStorage(payload: Array[Byte], objectReferenceIdOffset: ObjectReferenceId, objectReferenceIds: Range): EitherThrowableOr[TrancheId] =
    Try {
      val trancheId = {
        val iterator = rocksDb.newIterator(trancheIdKeyPayloadValueColumnFamily)
        iterator.seekToLast()
        if (iterator.isValid) 1L + Longs.fromByteArray(iterator.key()) else 0L
      }

      val trancheIdKey = Longs.toByteArray(trancheId)

      rocksDb.put(trancheIdKeyPayloadValueColumnFamily, trancheIdKey, payload)

      rocksDb.put(trancheIdKeyObjectReferenceIdOffsetValueFamily, trancheIdKey, Ints.toByteArray(objectReferenceIdOffset))

      for (objectReferenceId <- objectReferenceIds) {
        rocksDb.put(objectReferenceIdKeyTrancheIdValueColumnFamily, Ints.toByteArray(objectReferenceId), trancheIdKey)
      }

      trancheId
    }.toEither

  override def objectReferenceIdOffsetForNewTranche: EitherThrowableOr[ObjectReferenceId] = Try {
    val iterator = rocksDb.newIterator(objectReferenceIdKeyTrancheIdValueColumnFamily)
    iterator.seekToLast()
    if (iterator.isValid) 1 + Ints.fromByteArray(iterator.key()) else 0
  }.toEither

  override def retrieveTranche(trancheId: TrancheId): EitherThrowableOr[ImmutableObjectStorage.TrancheOfData] = Try {
    val trancheIdKey = Longs.toByteArray(trancheId)
    val payload = rocksDb.get(trancheIdKeyPayloadValueColumnFamily, trancheIdKey)
    val objectReferenceIdOffset = Ints.fromByteArray(rocksDb.get(trancheIdKeyObjectReferenceIdOffsetValueFamily, trancheIdKey))
    TrancheOfData(payload, objectReferenceIdOffset)
  }.toEither

  override def retrieveTrancheId(objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] = Try {
    Longs.fromByteArray(rocksDb.get(objectReferenceIdKeyTrancheIdValueColumnFamily, Ints.toByteArray(objectReferenceId)))
  }.toEither
}
