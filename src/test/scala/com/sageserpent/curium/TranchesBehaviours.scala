package com.sageserpent.curium

import cats.effect.{IO, Resource}
import com.sageserpent.curium.ImmutableObjectStorage.{TrancheOfData, Tranches, TranchesContracts}
import com.sageserpent.curium.ImmutableObjectStorageSpec.FakeTranches
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

trait TranchesResource[TrancheId] {
  val tranchesResource: Resource[IO, Tranches[TrancheId]]
}

object TranchesBehaviours {
  val payloadGenerator: Gen[Array[Byte]] =
    Gen.containerOf[Array, Byte](Arbitrary.arbByte.arbitrary)

  val objectReferenceIdCountGenerator: Gen[Int] =
    Gen.posNum[Int].map(_ - 1)

  val fakePayloadAndObjectReferenceIdCountPairsGenerator
  : Gen[(Array[Byte], Int)] =
    Gen.zip(payloadGenerator, objectReferenceIdCountGenerator)
}

trait TranchesBehaviours[TrancheId]
  extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  this: TranchesResource[TrancheId] =>

  import TranchesBehaviours._

  def tranchesBehaviour: Unit = {

    "creating a tranche" should "yield a unique tranche id" in forAll(
      Gen
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdCountPairsGenerator)) {
      payloadAndCountPairs =>
        tranchesResource
          .use(tranches =>
            IO {
              val numberOfPayloads = payloadAndCountPairs.size

              val trancheIds = MutableSet.empty[TrancheId]

              for ((payload, count) <- payloadAndCountPairs) {
                val Right(trancheId) =
                  for {
                    objectReferenceIdOffset <- tranches.objectReferenceIdOffsetForNewTranche
                    objectReferenceIds = objectReferenceIdOffset to count + objectReferenceIdOffset
                    trancheId <- tranches.createTrancheInStorage(
                      payload,
                      objectReferenceIdOffset,
                      objectReferenceIds)
                  } yield trancheId

                trancheIds += trancheId
              }

              trancheIds should have size numberOfPayloads
            })
          .unsafeRunSync
    }

    it should "permit retrieval of that tranche id from any of the associated object reference ids" in forAll(
      Gen
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdCountPairsGenerator)) {
      payloadAndCountPairs =>
        tranchesResource
          .use(tranches =>
            IO {
              val objectReferenceIdsByTrancheId =
                MutableMap.empty[TrancheId, Range]

              for ((payload, count) <- payloadAndCountPairs) {
                val Right((trancheId, objectReferenceIds)) =
                  for {
                    objectReferenceIdOffset <- tranches.objectReferenceIdOffsetForNewTranche
                    objectReferenceIds = objectReferenceIdOffset to count + objectReferenceIdOffset
                    trancheId <- tranches.createTrancheInStorage(
                      payload,
                      objectReferenceIdOffset,
                      objectReferenceIds)
                  } yield trancheId -> objectReferenceIds

                objectReferenceIdsByTrancheId += (trancheId -> objectReferenceIds)
              }

              objectReferenceIdsByTrancheId.foreach {
                case (trancheId, objectReferenceIds) =>
                  objectReferenceIds.foreach { objectReferenceId =>
                    val Right(retrievedTrancheId) =
                      tranches.retrieveTrancheId(objectReferenceId)
                    retrievedTrancheId shouldBe trancheId
                  }
              }
            })
          .unsafeRunSync
    }

    "retrieving a tranche by tranche id" should "yield a tranche that corresponds to what was used to create it" in forAll(
      Gen
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdCountPairsGenerator)) {
      payloadAndCountPairs =>
        tranchesResource
          .use(tranches =>
            IO {
              val trancheIdToExpectedTrancheMapping =
                MutableMap.empty[TrancheId, TrancheOfData]

              for ((payload, count) <- payloadAndCountPairs) {
                val Right((trancheId, tranche)) =
                  for {
                    objectReferenceIdOffset <- tranches.objectReferenceIdOffsetForNewTranche
                    objectReferenceIds = objectReferenceIdOffset to count + objectReferenceIdOffset
                    trancheId <- tranches.createTrancheInStorage(
                      payload,
                      objectReferenceIdOffset,
                      objectReferenceIds)
                  } yield
                    trancheId -> TrancheOfData(payload, objectReferenceIdOffset)

                trancheIdToExpectedTrancheMapping += (trancheId -> tranche)
              }

              trancheIdToExpectedTrancheMapping.foreach {
                case (trancheId, expectedTranche) =>
                  val Right(tranche) = tranches.retrieveTranche(trancheId)

                  tranche shouldBe expectedTranche
              }
            })
          .unsafeRunSync
    }
  }
}

object FakeTranchesResource {
  type TrancheId = FakeTranches#TrancheId
}

trait FakeTranchesResource
  extends TranchesResource[FakeTranchesResource.TrancheId] {

  override val tranchesResource
  : Resource[IO, Tranches[FakeTranchesResource.TrancheId]] =
    Resource.liftF(IO {
      new FakeTranches with TranchesContracts[FakeTranchesResource.TrancheId]
    })
}

class FakeTranchesSpec
  extends TranchesBehaviours[FakeTranchesResource.TrancheId]
    with FakeTranchesResource {
  "Fake tranches" should behave like tranchesBehaviour
}

object H2ViaDoobieTranchesResource {
  type TrancheId = H2ViaDoobieTranches#TrancheId
}

trait H2ViaDoobieTranchesResource
  extends TranchesResource[H2ViaDoobieTranchesResource.TrancheId] {
  override val tranchesResource
  : Resource[IO, Tranches[H2ViaDoobieTranchesResource.TrancheId]] =
    H2ViaDoobieResource.transactorResource.map(
      transactor =>
        new H2ViaDoobieTranches(transactor)
          with TranchesContracts[H2ViaDoobieTranchesResource.TrancheId])
}

class H2ViaDoobieTranchesSpec
  extends TranchesBehaviours[H2ViaDoobieTranchesResource.TrancheId]
    with H2ViaDoobieTranchesResource {
  "H2 tranches" should behave like tranchesBehaviour
}

object H2ViaScalikeJdbcTranchesResource {
  type TrancheId = H2ViaScalikeJdbcTranches#TrancheId
}

trait H2ViaScalikeJdbcTranchesResource
  extends TranchesResource[H2ViaScalikeJdbcTranchesResource.TrancheId]
    with H2ViaScalikeJdbcDatabaseSetupResource {
  override val tranchesResource
  : Resource[IO, Tranches[H2ViaScalikeJdbcTranchesResource.TrancheId]] =
    connectionPoolResource.map(
      connectionPool =>
        new H2ViaScalikeJdbcTranches(connectionPool)
          with TranchesContracts[H2ViaDoobieTranchesResource.TrancheId])
}

class H2ViaScalikeJdbcTranchesSpec
  extends TranchesBehaviours[H2ViaScalikeJdbcTranchesResource.TrancheId]
    with H2ViaScalikeJdbcTranchesResource {
  "H2 tranches" should behave like tranchesBehaviour
}

object RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId
}

trait RocksDbTranchesResource extends TranchesResource[RocksDbTranchesResource.TrancheId] with RocksDbResource {
  override val tranchesResource: Resource[IO, Tranches[RocksDbTranchesResource.TrancheId]] = for {
    rocksDb <- rocksDbResource
  } yield new RocksDbTranches(rocksDb)
}

class RocksDbTranchesSpec extends TranchesBehaviours[RocksDbTranchesResource.TrancheId] with RocksDbTranchesResource {
  "RocksDB tranches" should behave like tranchesBehaviour
}