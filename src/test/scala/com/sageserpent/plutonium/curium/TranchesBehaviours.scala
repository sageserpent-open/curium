package com.sageserpent.plutonium.curium

import ImmutableObjectStorage.{
  ObjectReferenceId,
  TrancheOfData,
  Tranches,
  TranchesContracts
}
import cats.effect.{Resource, IO}
import com.sageserpent.plutonium.curium.ImmutableObjectStorageSpec.FakeTranches
import com.sageserpent.plutonium.curium.TranchesBehaviours.FakePayload
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

trait TranchesResource[TrancheId] {
  val tranchesResourceGenerator: Resource[
    IO,
    Tranches[TrancheId, TranchesBehaviours.FakePayload]]
}

object TranchesBehaviours {
  type FakePayload = Int

  val fakePayloadGenerator: Gen[FakePayload] = Arbitrary.arbInt.arbitrary

  val objectReferenceIdOffsetsGenerator: Gen[Set[Int]] =
    Gen.containerOf[Set, Int](Gen.posNum[Int].map(_ - 1))

  val fakePayloadAndObjectReferenceIdOffsetsPairsGenerator
    : Gen[(FakePayload, Set[FakePayload])] =
    Gen.zip(fakePayloadGenerator, objectReferenceIdOffsetsGenerator)
}

trait TranchesBehaviours[TrancheId]
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  this: TranchesResource[TrancheId] =>

  import TranchesBehaviours._

  def tranchesBehaviour: Unit = {

    "creating a tranche" should "yield a unique tranche id" in forAll(
      tranchesResourceGenerator,
      Gen
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdOffsetsPairsGenerator)) {
      (tranchesResource, payloadAndOffsetsPairs) =>
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
      tranchesResourceGenerator,
      Gen
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdOffsetsPairsGenerator)) {
      (tranchesResource, payloadAndOffsetsPairs) =>
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
      tranchesResourceGenerator,
      Gen
        .nonEmptyListOf(fakePayloadAndObjectReferenceIdOffsetsPairsGenerator)) {
      (tranchesResource, payloadAndOffsetsPairs) =>
        tranchesResource
          .use(tranches =>
            IO {
              val trancheIdToExpectedTrancheMapping =
                MutableMap.empty[TrancheId, TrancheOfData[FakePayload]]

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
  type TrancheId = FakeTranches[FakePayload]#TrancheId
}

trait FakeTranchesResource
    extends TranchesResource[FakeTranchesResource.TrancheId] {

  override val tranchesResourceGenerator
    : Resource[IO, Tranches[FakeTranchesResource.TrancheId, FakePayload]] =
    Resource.liftF(IO {
      new FakeTranches[FakePayload]
      with TranchesContracts[FakeTranchesResource.TrancheId, FakePayload]
    })
}

class FakeTranchesSpec
    extends TranchesBehaviours[FakeTranchesResource.TrancheId]
    with FakeTranchesResource {
  "Fake tranches" should behave like tranchesBehaviour
}
