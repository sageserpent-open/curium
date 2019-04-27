package com.sageserpent.plutonium.curium

import ImmutableObjectStorage.{
  ObjectReferenceId,
  TrancheOfData,
  Tranches,
  TranchesContracts
}
import cats.effect.{Resource, IO}
import com.sageserpent.plutonium.curium.ImmutableObjectStorageSpec.FakeTranches
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

  val objectReferenceIdOffsetsGenerator: Gen[Set[Int]] =
    Gen.containerOf[Set, ObjectReferenceId](Gen.posNum[Int].map(_ - 1))

  val fakePayloadAndObjectReferenceIdOffsetsPairsGenerator
    : Gen[(Array[Byte], Set[ObjectReferenceId])] =
    Gen.zip(payloadGenerator, objectReferenceIdOffsetsGenerator)
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
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdOffsetsPairsGenerator)) {
      payloadAndOffsetsPairs =>
        tranchesResource
          .use(tranches =>
            IO {
              val numberOfPayloads = payloadAndOffsetsPairs.size

              val trancheIds = MutableSet.empty[TrancheId]

              for ((payload, offsets) <- payloadAndOffsetsPairs) {
                val Right(trancheId) =
                  for {
                    objectReferenceIdOffset <- tranches.objectReferenceIdOffsetForNewTranche
                    objectReferenceIds = offsets map (_ + objectReferenceIdOffset)
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
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdOffsetsPairsGenerator)) {
      payloadAndOffsetsPairs =>
        tranchesResource
          .use(tranches =>
            IO {
              val objectReferenceIdsByTrancheId =
                MutableMap.empty[TrancheId, Set[ObjectReferenceId]]

              for ((payload, offsets) <- payloadAndOffsetsPairs) {
                val Right((trancheId, objectReferenceIds)) =
                  for {
                    objectReferenceIdOffset <- tranches.objectReferenceIdOffsetForNewTranche
                    objectReferenceIds = offsets map (_ + objectReferenceIdOffset)
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
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdOffsetsPairsGenerator)) {
      payloadAndOffsetsPairs =>
        tranchesResource
          .use(tranches =>
            IO {
              val trancheIdToExpectedTrancheMapping =
                MutableMap.empty[TrancheId, TrancheOfData]

              for ((payload, offsets) <- payloadAndOffsetsPairs) {
                val Right((trancheId, tranche)) =
                  for {
                    objectReferenceIdOffset <- tranches.objectReferenceIdOffsetForNewTranche
                    objectReferenceIds = offsets map (_ + objectReferenceIdOffset)
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

object H2TranchesResource {
  type TrancheId = H2ViaDoobieTranches#TrancheId
}

trait H2TranchesResource
    extends TranchesResource[H2TranchesResource.TrancheId] {
  override val tranchesResource
    : Resource[IO, Tranches[H2TranchesResource.TrancheId]] =
    H2ViaDoobieResource.transactorResource.map(
      transactor =>
        new H2ViaDoobieTranches(transactor)
        with TranchesContracts[H2TranchesResource.TrancheId])
}

class H2ViaDoobieTranchesSpec
    extends TranchesBehaviours[H2TranchesResource.TrancheId]
    with H2TranchesResource {
  "H2 tranches" should behave like tranchesBehaviour
}

object H2AlternativeTranchesResource {
  type TrancheId = H2ViaScalikeJdbcTranches#TrancheId
}

trait H2AlternativeTranchesResource
    extends TranchesResource[H2AlternativeTranchesResource.TrancheId] {
  override val tranchesResource
    : Resource[IO, Tranches[H2AlternativeTranchesResource.TrancheId]] =
    H2ViaScalikeJdbcResource.connectionPoolResource.map(
      connectionPool =>
        new H2ViaScalikeJdbcTranches(connectionPool)
        with TranchesContracts[H2TranchesResource.TrancheId])
}

class H2ViaScalikeJdbcTranchesSpec
    extends TranchesBehaviours[H2AlternativeTranchesResource.TrancheId]
    with H2AlternativeTranchesResource {
  "H2 tranches" should behave like tranchesBehaviour
}
