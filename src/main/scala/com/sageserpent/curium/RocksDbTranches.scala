package com.sageserpent.curium

import java.util.UUID

import com.google.common.primitives.Ints
import com.sageserpent.curium.ImmutableObjectStorage.{EitherThrowableOr, ObjectReferenceId, TrancheOfData, Tranches}
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyOptions, CompressionType, RocksDB}

import scala.util.Try

object RocksDbTranches {
  val trancheIdKeyPayloadValueColumnFamilyName = "TrancheIdKeyPayloadValue"
  val trancheIdKeyObjectReferenceIdOffsetValueFamilyName = "TrancheIdKeyObjectReferenceIdOffsetValue"
  val objectReferenceIdKeyTrancheIdValueColumnFamilyName = "ObjectReferenceIdKeyTrancheIdValue"
}

class RocksDbTranches(rocksDb: RocksDB) extends Tranches[Array[Byte]] {

  import RocksDbTranches._

  val sharedColumnFamilyOptions = new ColumnFamilyOptions().setCompressionType(CompressionType.LZ4_COMPRESSION).setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)

  val trancheIdKeyPayloadValueColumnFamily =
    rocksDb.createColumnFamily(new ColumnFamilyDescriptor(trancheIdKeyPayloadValueColumnFamilyName.getBytes, sharedColumnFamilyOptions))
  val trancheIdKeyObjectReferenceIdOffsetValueFamily =
    rocksDb.createColumnFamily(new ColumnFamilyDescriptor(trancheIdKeyObjectReferenceIdOffsetValueFamilyName.getBytes, sharedColumnFamilyOptions))
  val objectReferenceIdKeyTrancheIdValueColumnFamily =
    rocksDb.createColumnFamily(new ColumnFamilyDescriptor(objectReferenceIdKeyTrancheIdValueColumnFamilyName.getBytes, sharedColumnFamilyOptions))

  override def createTrancheInStorage(payload: Array[Byte], objectReferenceIdOffset: ObjectReferenceId, objectReferenceIds: Set[ObjectReferenceId]): EitherThrowableOr[TrancheId] =
    Try {
      val trancheId = UUID.randomUUID().toString.getBytes

      rocksDb.put(trancheIdKeyPayloadValueColumnFamily, trancheId, payload)

      rocksDb.put(trancheIdKeyObjectReferenceIdOffsetValueFamily, trancheId, Ints.toByteArray(objectReferenceIdOffset))

      for (objectReferenceId <- objectReferenceIds) {
        rocksDb.put(objectReferenceIdKeyTrancheIdValueColumnFamily, Ints.toByteArray(objectReferenceId), trancheId)
      }

      trancheId
    } toEither

  override def objectReferenceIdOffsetForNewTranche: EitherThrowableOr[ObjectReferenceId] = Try {
    val iterator = rocksDb.newIterator(objectReferenceIdKeyTrancheIdValueColumnFamily)
    iterator.seekToLast()
    if (iterator.isValid) 1 + Ints.fromByteArray(iterator.key()) else 0
  } toEither

  override def retrieveTranche(trancheId: TrancheId): EitherThrowableOr[ImmutableObjectStorage.TrancheOfData] = Try {
    val payload = rocksDb.get(trancheIdKeyPayloadValueColumnFamily, trancheId)
    val objectReferenceIdOffset = Ints.fromByteArray(rocksDb.get(trancheIdKeyObjectReferenceIdOffsetValueFamily, trancheId))
    TrancheOfData(payload, objectReferenceIdOffset)
  } toEither

  override def retrieveTrancheId(objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] = Try {
    rocksDb.get(objectReferenceIdKeyTrancheIdValueColumnFamily, Ints.toByteArray(objectReferenceId))
  } toEither
}
