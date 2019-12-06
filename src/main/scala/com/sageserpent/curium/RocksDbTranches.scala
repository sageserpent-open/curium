package com.sageserpent.curium

import java.util.UUID

import com.google.common.primitives.Ints
import com.sageserpent.curium.ImmutableObjectStorage.{EitherThrowableOr, ObjectReferenceId, TrancheOfData, Tranches}
import org.rocksdb.{ColumnFamilyDescriptor, RocksDB}

import scala.util.Try

object RocksDbTranches {
  val payloadValueFamilyName = "PayloadValue"
  val objectReferenceIdOffsetValueFamilyName = "ObjectReferenceIdOffsetValue"
  val trancheIdValueFamilyName = "TrancheIdValue"
}

class RocksDbTranches(rocksDb: RocksDB) extends Tranches[Array[Byte]] {

  import RocksDbTranches._

  val payloadValueColumnFamily = rocksDb.createColumnFamily(new ColumnFamilyDescriptor(payloadValueFamilyName.getBytes))
  val objectReferenceIdOffsetValueFamily = rocksDb.createColumnFamily(new ColumnFamilyDescriptor(objectReferenceIdOffsetValueFamilyName.getBytes))
  val trancheIdValueColumnFamily = rocksDb.createColumnFamily(new ColumnFamilyDescriptor(trancheIdValueFamilyName.getBytes))

  override def createTrancheInStorage(payload: Array[Byte], objectReferenceIdOffset: ObjectReferenceId, objectReferenceIds: Set[ObjectReferenceId]): EitherThrowableOr[TrancheId] =
    Try {
      val trancheId = UUID.randomUUID().toString.getBytes

      rocksDb.put(payloadValueColumnFamily, trancheId, payload)

      rocksDb.put(objectReferenceIdOffsetValueFamily, trancheId, Ints.toByteArray(objectReferenceIdOffset))

      for (objectReferenceId <- objectReferenceIds) {
        rocksDb.put(trancheIdValueColumnFamily, Ints.toByteArray(objectReferenceId), trancheId)
      }

      trancheId
    } toEither

  override def objectReferenceIdOffsetForNewTranche: EitherThrowableOr[ObjectReferenceId] = Try {
    val iterator = rocksDb.newIterator(payloadValueColumnFamily)
    iterator.seekToLast()
    if (iterator.isValid) 1 + Ints.fromByteArray(iterator.key()) else 0
  } toEither

  override def retrieveTranche(trancheId: TrancheId): EitherThrowableOr[ImmutableObjectStorage.TrancheOfData] = Try {
    val payload = rocksDb.get(payloadValueColumnFamily, trancheId)
    val objectReferenceIdOffset = Ints.fromByteArray(rocksDb.get(objectReferenceIdOffsetValueFamily, trancheId))
    TrancheOfData(payload, objectReferenceIdOffset)
  } toEither

  override def retrieveTrancheId(objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] = Try {
    rocksDb.get(trancheIdValueColumnFamily, Ints.toByteArray(objectReferenceId))
  } toEither
}
