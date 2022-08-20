package com.sageserpent.curium

import com.google.common.primitives.Longs
import com.sageserpent.curium.ImmutableObjectStorage._
import org.rocksdb._

import java.lang.{Integer => JavaInteger, Long => JavaLong}
import scala.collection.mutable
import scala.util.{Try, Using}

object RocksDbTranches {
  type AugmentedTrancheLocalObjectReferenceId = BigInt
  val trancheIdKeyPayloadValueColumnFamilyName = "TrancheIdKeyPayloadValue"
  val trancheIdKeyObjectReferenceIdOffsetValueFamilyName =
    "TrancheIdKeyObjectReferenceIdOffsetValue"
  val objectReferenceIdKeyTrancheIdValueColumnFamilyName =
    "ObjectReferenceIdKeyTrancheIdValue"
  val augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamilyName =
    "AugmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValue"

  private val numberOfBytesForTheKeyRepresentationOfAugmentedTrancheLocalObjectReferenceId =
    JavaLong.BYTES + JavaInteger.BYTES

  private def keyFor(
      augmentedTrancheLocalObjectReferenceId: AugmentedTrancheLocalObjectReferenceId
  ): Array[Byte] = {
    val byteArray = augmentedTrancheLocalObjectReferenceId.toByteArray

    // Remember that RocksDB by default uses lexicographical ordering of keys,
    // so we have to left-pad with zero bytes to ensure that small `BigInt`
    // instances that don't require many bytes come before larger values that
    // require more bytes.
    Array.fill[Byte](
      numberOfBytesForTheKeyRepresentationOfAugmentedTrancheLocalObjectReferenceId - byteArray.length
    )(0) ++ byteArray
  }
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
        new ColumnFamilyOptions(sharedColumnFamilyOptions)
          .useFixedLengthPrefixExtractor(JavaLong.BYTES)
      )
    )

  override def createTrancheInStorage(
      tranche: TrancheOfData,
      objectReferenceIds: Seq[CanonicalObjectReferenceId]
  ): EitherThrowableOr[TrancheId] =
    Try {
      val trancheId = Using.resource(
        rocksDb.newIterator(trancheIdKeyPayloadValueColumnFamily)
      ) { iterator =>
        iterator.seekToLast()
        if (iterator.isValid) 1L + Longs.fromByteArray(iterator.key()) else 0L
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
          keyFor(augmentedTrancheLocalObjectReferenceId),
          Longs.toByteArray(canonicalObjectReferenceId)
        )
      }

      trancheId
    }.toEither

  private def augmentWith(trancheId: TrancheId)(
      trancheLocalObjectReferenceId: TrancheLocalObjectReferenceId
  ): AugmentedTrancheLocalObjectReferenceId =
    (BigInt.long2bigInt(trancheId) << JavaInteger.SIZE) + BigInt.long2bigInt(
      JavaInteger.toUnsignedLong(trancheLocalObjectReferenceId)
    )

  override def objectReferenceIdOffsetForNewTranche
      : EitherThrowableOr[CanonicalObjectReferenceId] = Try {
    Using.resource(
      rocksDb.newIterator(objectReferenceIdKeyTrancheIdValueColumnFamily)
    ) { iterator =>
      iterator.seekToLast()
      if (iterator.isValid) 1L + Longs.fromByteArray(iterator.key()) else 0L
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

    val lowerBound = augmentWith(trancheId)(0)
    val upperBound = augmentWith(1L + trancheId)(0)

    Using.resource(
      new ReadOptions()
        .setAutoPrefixMode(true)
        .setIterateUpperBound(new Slice(keyFor(upperBound)))
    ) { readOptions =>
      Using.resource(
        rocksDb.newIterator(
          augmentedTrancheLocalObjectReferenceIdKeyCanonicalObjectReferenceIdValueColumnFamily,
          readOptions
        )
      ) { iterator =>
        iterator.seek(keyFor(lowerBound))

        while (iterator.isValid) {
          interTrancheObjectReferenceIdTranslation +=
            (extractTrancheLocalObjectReferenceIdFrom(
              BigInt(iterator.key())
            ) -> Longs.fromByteArray(iterator.value()))

          iterator.next()
        }
      }
    }

    TrancheOfData(
      payload,
      objectReferenceIdOffset,
      interTrancheObjectReferenceIdTranslation.toMap
    )
  }.toEither

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
