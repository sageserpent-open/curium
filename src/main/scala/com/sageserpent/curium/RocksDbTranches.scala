package com.sageserpent.curium

import com.google.common.primitives.Longs
import com.sageserpent.curium.ImmutableObjectStorage._
import org.rocksdb._

import java.lang.{Integer => JavaInteger}
import scala.collection.mutable
import scala.util.Try

object RocksDbTranches {
  type AugmentedTrancheLocalObjectReferenceId = Long

  val trancheIdKeyPayloadValueColumnFamilyName = "TrancheIdKeyPayloadValue"
  val trancheIdKeyObjectReferenceIdOffsetValueFamilyName =
    "TrancheIdKeyObjectReferenceIdOffsetValue"
  val objectReferenceIdKeyTrancheIdValueColumnFamilyName =
    "ObjectReferenceIdKeyTrancheIdValue"
  val augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamilyName =
    "AugmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValue"
}

class RocksDbTranches(rocksDb: RocksDB) extends Tranches[Long] {

  import RocksDbTranches._

  val sharedColumnFamilyOptions = new ColumnFamilyOptions()
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
  val trancheIdKeyPayloadValueColumnFamily =
    rocksDb.createColumnFamily(
      new ColumnFamilyDescriptor(
        trancheIdKeyPayloadValueColumnFamilyName.getBytes,
        sharedColumnFamilyOptions
      )
    )
  val trancheIdKeyObjectReferenceIdOffsetValueFamily =
    rocksDb.createColumnFamily(
      new ColumnFamilyDescriptor(
        trancheIdKeyObjectReferenceIdOffsetValueFamilyName.getBytes,
        sharedColumnFamilyOptions
      )
    )
  val objectReferenceIdKeyTrancheIdValueColumnFamily =
    rocksDb.createColumnFamily(
      new ColumnFamilyDescriptor(
        objectReferenceIdKeyTrancheIdValueColumnFamilyName.getBytes,
        sharedColumnFamilyOptions
      )
    )
  val augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamily =
    rocksDb.createColumnFamily(
      new ColumnFamilyDescriptor(
        augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamilyName.getBytes,
        sharedColumnFamilyOptions
      )
    )

  override def createTrancheInStorage(
      tranche: TrancheOfData,
      objectReferenceIds: Seq[CanonicalObjectReferenceId]
  ): EitherThrowableOr[TrancheId] =
    Try {
      val trancheId = {
        val iterator = rocksDb.newIterator(trancheIdKeyPayloadValueColumnFamily)
        try {
          iterator.seekToLast()
          if (iterator.isValid) 1L + Longs.fromByteArray(iterator.key()) else 0L
        } finally {
          iterator.close()
        }
      }

      val trancheIdKey = Longs.toByteArray(trancheId)

      rocksDb.put(
        trancheIdKeyPayloadValueColumnFamily,
        trancheIdKey,
        tranche.payload
      )

      rocksDb.put(
        trancheIdKeyObjectReferenceIdOffsetValueFamily,
        trancheIdKey,
        Longs.toByteArray(tranche.objectReferenceIdOffset)
      )

      for (objectReferenceId <- objectReferenceIds) {
        rocksDb.put(
          objectReferenceIdKeyTrancheIdValueColumnFamily,
          Longs.toByteArray(objectReferenceId),
          trancheIdKey
        )
      }

      for (
        (trancheLocalObjectReferenceId, canonicalObjectReferenceId) <-
          tranche.interTrancheObjectReferenceIdTranslation
      ) {
        val augmentedTrancheLocalObjectReferenceId
            : AugmentedTrancheLocalObjectReferenceId =
          augmentWith(trancheId)(trancheLocalObjectReferenceId)

        rocksDb.put(
          augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamily,
          Longs.toByteArray(augmentedTrancheLocalObjectReferenceId),
          Longs.toByteArray(canonicalObjectReferenceId)
        )
      }

      trancheId
    }.toEither

  override def objectReferenceIdOffsetForNewTranche
      : EitherThrowableOr[CanonicalObjectReferenceId] = Try {
    val iterator =
      rocksDb.newIterator(objectReferenceIdKeyTrancheIdValueColumnFamily)
    try {
      iterator.seekToLast()
      if (iterator.isValid) 1L + Longs.fromByteArray(iterator.key()) else 0L
    } finally {
      iterator.close()
    }
  }.toEither

  override def retrieveTranche(
      trancheId: TrancheId
  ): EitherThrowableOr[ImmutableObjectStorage.TrancheOfData] = Try {
    val trancheIdKey = Longs.toByteArray(trancheId)
    val payload =
      rocksDb.get(trancheIdKeyPayloadValueColumnFamily, trancheIdKey)
    val objectReferenceIdOffset = Longs.fromByteArray(
      rocksDb.get(trancheIdKeyObjectReferenceIdOffsetValueFamily, trancheIdKey)
    )

    val interTrancheObjectReferenceIdTranslation: mutable.Map[
      TrancheLocalObjectReferenceId,
      CanonicalObjectReferenceId
    ] = mutable.Map.empty

    val readOptions: ReadOptions = new ReadOptions()

    val lowerBound = augmentWith(trancheId)(0)
    val upperBound = augmentWith(1L + trancheId)(0)

    readOptions
      .setIterateUpperBound(new Slice(Longs.toByteArray(upperBound)))

    val iterator = rocksDb.newIterator(
      augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamily,
      readOptions
    )

    try {
      iterator.seek(Longs.toByteArray(lowerBound))

      while (iterator.isValid) {
        interTrancheObjectReferenceIdTranslation +=
          (extractTrancheLocalObjectReferenceIdFrom(
            Longs.fromByteArray(iterator.key())
          ) -> Longs.fromByteArray(iterator.value()))

        iterator.next()
      }
    } finally {
      iterator.close()
      readOptions.close()
    }

    TrancheOfData(
      payload,
      objectReferenceIdOffset,
      interTrancheObjectReferenceIdTranslation.toMap
    )
  }.toEither

  private def augmentWith(trancheId: TrancheId)(
      trancheLocalObjectReferenceId: TrancheLocalObjectReferenceId
  ): AugmentedTrancheLocalObjectReferenceId =
    (trancheId << JavaInteger.SIZE) + JavaInteger.toUnsignedLong(
      trancheLocalObjectReferenceId
    )

  private def extractTrancheLocalObjectReferenceIdFrom(
      augmentedTrancheLocalObjectReferenceId: AugmentedTrancheLocalObjectReferenceId
  ): TrancheLocalObjectReferenceId =
    (augmentedTrancheLocalObjectReferenceId & ((1L << JavaInteger.SIZE) - 1L)).toInt

  override def retrieveTrancheId(
      objectReferenceId: CanonicalObjectReferenceId
  ): EitherThrowableOr[TrancheId] = Try {
    Longs.fromByteArray(
      rocksDb.get(
        objectReferenceIdKeyTrancheIdValueColumnFamily,
        Longs.toByteArray(objectReferenceId)
      )
    )
  }.toEither
}
