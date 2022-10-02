package com.sageserpent.curium

import com.google.common.io.ByteStreams
import com.google.common.primitives.Longs
import com.sageserpent.curium.ImmutableObjectStorage._
import org.rocksdb._

import java.lang.{Long => JavaLong}
import scala.collection.mutable
import scala.util.{Try, Using}

object RocksDbTranches {
  val trancheIdKeyPayloadValueColumnFamilyName = "TrancheIdKeyPayloadValue"
  val augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamilyName =
    "AugmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValue"

  val writeOptions = new WriteOptions()
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
  val augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamily =
    rocksDb.createColumnFamily(
      new ColumnFamilyDescriptor(
        augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamilyName.getBytes,
        new ColumnFamilyOptions(sharedColumnFamilyOptions)
          .useFixedLengthPrefixExtractor(JavaLong.BYTES)
      )
    )

  override def createTrancheInStorage(
      tranche: TrancheOfData[TrancheId]
  ): EitherThrowableOr[TrancheId] =
    Try {
      val trancheId = Using.resource(
        rocksDb.newIterator(trancheIdKeyPayloadValueColumnFamily)
      ) { iterator =>
        iterator.seekToLast()
        if (iterator.isValid) 1L + Longs.fromByteArray(iterator.key()) else 0L
      }

      val trancheIdKey = Longs.toByteArray(trancheId)

      Using.resource(new WriteBatch()) { writeBatch =>
        writeBatch.put(
          trancheIdKeyPayloadValueColumnFamily,
          trancheIdKey,
          tranche.payload
        )

        for (
          (trancheLocalObjectReferenceId, canonicalObjectReferenceId) <-
            tranche.interTrancheObjectReferenceIdTranslation
        ) {
          writeBatch.put(
            augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamily,
            byteArrayOf(trancheId -> trancheLocalObjectReferenceId),
            byteArrayOf(canonicalObjectReferenceId)
          )
        }

        rocksDb.write(writeOptions, writeBatch)
      }

      trancheId
    }.toEither

  override def retrieveTranche(
      trancheId: TrancheId
  ): EitherThrowableOr[TrancheOfData[TrancheId]] = Try {
    val trancheIdKey = Longs.toByteArray(trancheId)
    val payload =
      rocksDb.get(trancheIdKeyPayloadValueColumnFamily, trancheIdKey)

    val interTrancheObjectReferenceIdTranslation: mutable.Map[
      TrancheLocalObjectReferenceId,
      CanonicalObjectReferenceId[TrancheId]
    ] = mutable.Map.empty

    val lowerBound = trancheId        -> 0
    val upperBound = (1L + trancheId) -> 0

    Using.resource(
      new ReadOptions()
        .setAutoPrefixMode(true)
        .setIterateUpperBound(new Slice(byteArrayOf(upperBound)))
    ) { readOptions =>
      Using.resource(
        rocksDb.newIterator(
          augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamily,
          readOptions
        )
      ) { iterator =>
        iterator.seek(byteArrayOf(lowerBound))

        while (iterator.isValid) {
          interTrancheObjectReferenceIdTranslation +=
            (trancheLocalObjectReferenceIdOf(
              iterator.key()
            ) -> canonicalObjectReferenceIdOf(
              iterator.value()
            ))

          iterator.next()
        }
      }
    }

    TrancheOfData(
      payload,
      interTrancheObjectReferenceIdTranslation.toMap
    )
  }.toEither

  private def byteArrayOf(
      canonicalObjectReferenceId: CanonicalObjectReferenceId[TrancheId]
  ): Array[Byte] = {
    val (
      trancheId: TrancheId,
      trancheLocalObjectReferenceId: TrancheLocalObjectReferenceId
    ) = canonicalObjectReferenceId

    val output = ByteStreams.newDataOutput()
    output.writeLong(trancheId)
    output.writeInt(trancheLocalObjectReferenceId)
    output.toByteArray
  }

  private def trancheLocalObjectReferenceIdOf(
      payload: Array[Byte]
  ): TrancheLocalObjectReferenceId = {
    val input = ByteStreams.newDataInput(payload)

    input.readLong(): Unit // Skip the tranche id, and fail fast if there aren't enough bytes; therefore prefer this to using `skipBytes`.

    input.readInt()
  }

  private def canonicalObjectReferenceIdOf(
      payload: Array[Byte]
  ): CanonicalObjectReferenceId[TrancheId] = {
    val input = ByteStreams.newDataInput(payload)

    val trancheId: TrancheId = input.readLong()
    val trancheLocalObjectReferenceId: TrancheLocalObjectReferenceId =
      input.readInt()

    trancheId -> trancheLocalObjectReferenceId
  }
}
