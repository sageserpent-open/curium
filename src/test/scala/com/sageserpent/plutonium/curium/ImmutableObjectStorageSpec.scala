package com.sageserpent.plutonium.curium

import cats.implicits._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.curium.ImmutableObjectStorage._
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.collection.mutable.{
  Map => MutableMap,
  SortedMap => MutableSortedMap
}
import scala.util.{Random, Try}

object ImmutableObjectStorageSpec {
  sealed trait Part

  case class Leaf(id: Int) extends Part

  case class Fork(left: Part, id: Int, right: Part) extends Part

  case object alien

  val seedGenerator: Gen[Int] = Arbitrary.arbInt.arbitrary

  type PartGrowthStep = Vector[Part] => Part

  def partGrowthStepsLeadingToRootForkGenerator(
      allowDuplicates: Boolean): Gen[Seq[PartGrowthStep]] =
    for {
      maximumNumberOfLeaves <- Gen.posNum[Int]
      seed                  <- seedGenerator
    } yield {
      require(0 < maximumNumberOfLeaves)

      val randomBehaviour = new Random(seed)

      def growthSteps(partIdSetsCoveredBySubparts: Vector[Set[Int]],
                      numberOfLeaves: Int): Stream[PartGrowthStep] = {
        val numberOfSubparts          = partIdSetsCoveredBySubparts.size
        val partId                    = numberOfSubparts // Makes it easier to read the test cases when debugging; the ids label the growth steps in ascending order.
        val collectingStrandsTogether = numberOfLeaves == maximumNumberOfLeaves

        require(
          numberOfLeaves < maximumNumberOfLeaves ||
            0 < numberOfSubparts && numberOfLeaves == maximumNumberOfLeaves)

        val chooseALeaf = numberOfLeaves < maximumNumberOfLeaves &&
          (0 == numberOfSubparts || randomBehaviour.nextBoolean())

        if (chooseALeaf) {
          def leaf(subparts: Vector[Part]): Part = {
            require(numberOfSubparts == subparts.size)

            Leaf(numberOfSubparts)
          }
          leaf _ #:: growthSteps(partIdSetsCoveredBySubparts :+ Set(partId),
                                 1 + numberOfLeaves)
        } else if (allowDuplicates && 0 < numberOfSubparts && randomBehaviour
                     .nextBoolean())
          ((_: Vector[Part]).last) #:: growthSteps(
            partIdSetsCoveredBySubparts :+ partIdSetsCoveredBySubparts.last,
            numberOfLeaves)
        else {
          val indexOfLeftSubpart =
            if (collectingStrandsTogether) numberOfSubparts - 1
            else
              randomBehaviour
                .chooseAnyNumberFromZeroToOneLessThan(numberOfSubparts)
          val indexOfRightSubpart = if (collectingStrandsTogether) {
            val partIdsCoveredByLeftSubpart = partIdSetsCoveredBySubparts.last
            val indicesOfPartsNotCoveredByLeftSubpart = 0 until numberOfSubparts filterNot {
              index =>
                val partIdsCoveredByIndex = partIdSetsCoveredBySubparts(index)
                partIdsCoveredByIndex.subsetOf(partIdsCoveredByLeftSubpart)
            }
            if (indicesOfPartsNotCoveredByLeftSubpart.nonEmpty)
              indicesOfPartsNotCoveredByLeftSubpart.maxBy(index =>
                partIdSetsCoveredBySubparts(index).size)
            else
              randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
                numberOfSubparts)
          } else
            randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(numberOfSubparts)
          val partIdsCoveredByThisFork: Set[Int] = partIdSetsCoveredBySubparts(
            indexOfLeftSubpart) ++ Set(partId) ++ partIdSetsCoveredBySubparts(
            indexOfRightSubpart)
          val allSubpartsIncludedInThisFork =
            partIdSetsCoveredBySubparts.forall(
              _.subsetOf(partIdsCoveredByThisFork))

          def fork(subparts: Vector[Part]): Fork = {
            require(numberOfSubparts == subparts.size)

            Fork(subparts(indexOfLeftSubpart),
                 partId,
                 subparts(indexOfRightSubpart))
          }
          fork _ #:: (if (allSubpartsIncludedInThisFork &&
                          (collectingStrandsTogether ||
                          randomBehaviour.nextBoolean())) Stream.empty
                      else
                        growthSteps(
                          partIdSetsCoveredBySubparts :+ partIdsCoveredByThisFork,
                          numberOfLeaves))
        }
      }

      val result = growthSteps(Vector.empty, numberOfLeaves = 0).force

      assert(result.nonEmpty)

      result
    }

  def storeViaMultipleSessions(
      partGrowthStepsLeadingToRootFork: Seq[PartGrowthStep],
      tranches: Tranches,
      randomBehaviour: Random): Vector[TrancheId] = {
    val partGrowthStepsLeadingToRootForkInChunks
      : Stream[Vector[PartGrowthStep]] =
      randomBehaviour
        .splitIntoNonEmptyPieces(partGrowthStepsLeadingToRootFork)
        .map(_.toVector)
        .force

    var trancheIdsSoFar: Vector[TrancheId] = Vector.empty

    for (partGrowthSteps <- partGrowthStepsLeadingToRootForkInChunks) {
      val retrievalAndStorageSession: Session[Vector[TrancheId]] = for {
        retrievedPartsFromPreviousSessions <- trancheIdsSoFar.traverse(
          ImmutableObjectStorage.retrieve[Part])
        newPartsFromThisSession: Vector[Part] = (retrievedPartsFromPreviousSessions /: partGrowthSteps) {
          case (existingSubparts, partGrowthStep) =>
            existingSubparts :+ partGrowthStep(existingSubparts)
        } drop retrievedPartsFromPreviousSessions.size
        trancheIds <- newPartsFromThisSession.traverse(
          ImmutableObjectStorage.store)
      } yield trancheIds

      val Right(trancheIdsForChunk) =
        ImmutableObjectStorage
          .runToYieldTrancheIds(retrievalAndStorageSession)(tranches)

      trancheIdsSoFar ++= trancheIdsForChunk
    }

    trancheIdsSoFar
  }

  class FakeTranches extends Tranches {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] = MutableMap.empty
    val objectReferenceIdsToAssociatedTrancheIdMap
      : MutableSortedMap[ObjectReferenceId, TrancheId] = MutableSortedMap.empty

    def purgeTranche(trancheId: TrancheId): Unit = {
      val objectReferenceIdsToRemove =
        objectReferenceIdsToAssociatedTrancheIdMap.collect {
          case (objectReferenceId, keyTrancheId) if trancheId == keyTrancheId =>
            objectReferenceId
        }

      objectReferenceIdsToAssociatedTrancheIdMap --= objectReferenceIdsToRemove

      tranchesById -= trancheId
    }

    override protected def storeTrancheAndAssociatedObjectReferenceIds(
        trancheId: TrancheId,
        tranche: TrancheOfData,
        objectReferenceIds: Seq[ObjectReferenceId]): EitherThrowableOr[Unit] = {
      Try {
        tranchesById(trancheId) = tranche
        for (objectReferenceId <- objectReferenceIds) {
          objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) =
            trancheId
        }
      }.toEither
    }
    override def retrieveTranche(
        trancheId: TrancheId): scala.Either[scala.Throwable, TrancheOfData] =
      Try { tranchesById(trancheId) }.toEither
    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
      : scala.Either[scala.Throwable, TrancheId] =
      Try { objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) }.toEither
    override def objectReferenceIdOffsetForNewTranche
      : EitherThrowableOr[ObjectReferenceId] =
      Try {
        val maximumObjectReferenceId =
          objectReferenceIdsToAssociatedTrancheIdMap.keys
            .reduceOption((leftObjectReferenceId, rightObjectReferenceId) =>
              leftObjectReferenceId max rightObjectReferenceId)
        val alignmentMultipleForObjectReferenceIdsInSeparateTranches = 100
        maximumObjectReferenceId.fold(0)(
          1 + _ / alignmentMultipleForObjectReferenceIdsInSeparateTranches) * alignmentMultipleForObjectReferenceIdsInSeparateTranches
      }.toEither
  }
}

class ImmutableObjectStorageSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import ImmutableObjectStorageSpec._

  implicit def shrinkAny[Seq[PartGrowthStep]]: Shrink[Seq[PartGrowthStep]] =
    Shrink(_ => Stream.empty)

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in forAll(
    partGrowthStepsLeadingToRootForkGenerator(allowDuplicates = true),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthSteps, seed) =>
    whenever(partGrowthSteps.nonEmpty) {
      val randomBehaviour = new Random(seed)

      val tranches = new FakeTranches

      val trancheIds =
        storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

      trancheIds should contain theSameElementsAs trancheIds.toSet

      tranches.tranchesById.keys should contain theSameElementsAs trancheIds
    }
  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    partGrowthStepsLeadingToRootForkGenerator(allowDuplicates = true),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthSteps, seed) =>
    whenever(partGrowthSteps.nonEmpty) {
      val randomBehaviour = new Random(seed)

      val expectedParts = (Vector.empty[Part] /: partGrowthSteps) {
        case (existingSubparts, partGrowthStep) =>
          existingSubparts :+ partGrowthStep(existingSubparts)
      }

      val tranches = new FakeTranches

      val trancheIds =
        storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

      val forwardPermutation: Map[Int, Int] = randomBehaviour
        .shuffle(Vector.tabulate(trancheIds.size)(identity))
        .zipWithIndex
        .toMap

      val backwardsPermutation = forwardPermutation.map(_.swap)

      // NOTE: as long as we have a complete chain of tranches, it shouldn't matter
      // in what order tranche ids are submitted for retrieval.
      val permutedTrancheIds = Vector(trancheIds.indices map (index =>
        trancheIds(forwardPermutation(index))): _*)

      val retrievalSession: Session[Unit] =
        for {
          retrievedParts <- permutedTrancheIds.traverse(
            ImmutableObjectStorage.retrieve[Part])

        } yield {
          val unpermutedRetrievedParts = retrievedParts.indices map (index =>
            retrievedParts(backwardsPermutation(index)))

          unpermutedRetrievedParts should contain theSameElementsInOrderAs expectedParts

          Inspectors.forAll(retrievedParts)(retrievedPart =>
            Inspectors.forAll(expectedParts)(expectedPart =>
              retrievedPart should not be theSameInstanceAs(expectedPart)))
        }

      ImmutableObjectStorage.runForEffectsOnly(retrievalSession)(tranches) shouldBe a[
        Right[_, _]]
    }
  }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in forAll(
    partGrowthStepsLeadingToRootForkGenerator(allowDuplicates = true),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthSteps, seed) =>
    whenever(partGrowthSteps.nonEmpty) {
      val randomBehaviour = new Random(seed)

      val tranches = new FakeTranches

      val trancheIds =
        storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

      val referenceParts = (Vector.empty[Part] /: partGrowthSteps) {
        case (existingSubparts, partGrowthStep) =>
          existingSubparts :+ partGrowthStep(existingSubparts)
      }

      val referencePartsByTrancheId = (trancheIds zip referenceParts).toMap

      val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

      val samplingSession: Session[Unit] = for {
        _ <- referencePartsByTrancheId(sampleTrancheId) match {
          case _: Fork => ImmutableObjectStorage.retrieve[Leaf](sampleTrancheId)
          case _: Leaf => ImmutableObjectStorage.retrieve[Fork](sampleTrancheId)
        }
      } yield ()

      ImmutableObjectStorage.runForEffectsOnly(samplingSession)(tranches) shouldBe a[
        Left[_, _]]
    }
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in forAll(
    partGrowthStepsLeadingToRootForkGenerator(allowDuplicates = false),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthSteps, seed) =>
    whenever(partGrowthSteps.nonEmpty) {
      val randomBehaviour = new Random(seed)

      val tranches = new FakeTranches

      val trancheIds =
        storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

      val idOfCorruptedTranche = randomBehaviour.chooseOneOf(trancheIds)

      tranches.tranchesById(idOfCorruptedTranche) = {
        val trancheToCorrupt = tranches.tranchesById(idOfCorruptedTranche)
        trancheToCorrupt.copy(
          serializedRepresentation = "*** CORRUPTION! ***"
            .map(_.toByte)
            .toArray ++ trancheToCorrupt.serializedRepresentation)
      }

      val rootTrancheId = trancheIds.last

      val samplingSessionWithCorruptedTranche: Session[Unit] = for {
        _ <- ImmutableObjectStorage.retrieve[Fork](rootTrancheId)
      } yield ()

      ImmutableObjectStorage.runForEffectsOnly(
        samplingSessionWithCorruptedTranche)(tranches) shouldBe a[Left[_, _]]
    }
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in forAll(
    partGrowthStepsLeadingToRootForkGenerator(allowDuplicates = false),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthSteps, seed) =>
    whenever(partGrowthSteps.nonEmpty) {
      val randomBehaviour = new Random(seed)

      val tranches = new FakeTranches

      val trancheIds =
        storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

      val idOfMissingTranche =
        randomBehaviour.chooseOneOf(trancheIds)

      tranches.purgeTranche(idOfMissingTranche)

      val rootTrancheId = trancheIds.last

      val samplingSessionWithMissingTranche: Session[Unit] = for {
        _ <- ImmutableObjectStorage.retrieve[Fork](rootTrancheId)
      } yield ()

      ImmutableObjectStorage.runForEffectsOnly(
        samplingSessionWithMissingTranche)(tranches) shouldBe a[Left[_, _]]
    }
  }

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in forAll(
    partGrowthStepsLeadingToRootForkGenerator(allowDuplicates = false),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthSteps, seed) =>
    whenever(partGrowthSteps.nonEmpty) {
      val randomBehaviour = new Random(seed)

      val tranches = new FakeTranches

      val Right(alienTrancheId) = ImmutableObjectStorage.runToYieldTrancheId(
        ImmutableObjectStorage.store(alien))(tranches)

      val nonAlienTrancheIds =
        storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

      val idOfIncorrectlyTypedTranche =
        randomBehaviour.chooseOneOf(nonAlienTrancheIds)

      tranches.tranchesById(idOfIncorrectlyTypedTranche) =
        tranches.tranchesById(alienTrancheId)

      val rootTrancheId = nonAlienTrancheIds.last

      val samplingSessionWithTrancheForIncompatibleType: Session[Unit] = for {
        _ <- ImmutableObjectStorage.retrieve[Fork](rootTrancheId)
      } yield ()

      ImmutableObjectStorage.runForEffectsOnly(
        samplingSessionWithTrancheForIncompatibleType)(tranches) shouldBe a[
        Left[_, _]]
    }
  }

  it should "result in a smaller tranche when there is a tranche chain covering some of its substructure" in forAll(
    partGrowthStepsLeadingToRootForkGenerator(allowDuplicates = true),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthSteps, seed) =>
    whenever(partGrowthSteps.nonEmpty) {
      val randomBehaviour = new Random(seed)

      val isolatedSpokeTranche = {
        val isolatedSpokeTranches = new FakeTranches

        val root = (Vector.empty[Part] /: partGrowthSteps) {
          case (existingSubparts, partGrowthStep) =>
            existingSubparts :+ partGrowthStep(existingSubparts)
        }.last

        val isolatedSpokeStorageSession: Session[TrancheId] =
          ImmutableObjectStorage.store(root)

        val Right(isolatedTrancheId) =
          ImmutableObjectStorage.runToYieldTrancheId(
            isolatedSpokeStorageSession)(isolatedSpokeTranches)

        isolatedSpokeTranches.tranchesById(isolatedTrancheId)
      }

      val tranches = new FakeTranches

      val trancheIds =
        storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

      val rootTrancheId = trancheIds.last

      val rootTranche = tranches.tranchesById(rootTrancheId)

      rootTranche.serializedRepresentation.length should be < isolatedSpokeTranche.serializedRepresentation.length
    }
  }

  it should "be idempotent in terms of object identity when retrieving using the same tranche id" in forAll(
    partGrowthStepsLeadingToRootForkGenerator(allowDuplicates = true),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthSteps, seed) =>
    whenever(partGrowthSteps.nonEmpty) {
      val randomBehaviour = new Random(seed)

      val tranches = new FakeTranches

      val trancheIds =
        storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

      val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

      val samplingSession: Session[Unit] = for {
        retrievedPartTakeOne <- ImmutableObjectStorage.retrieve[Part](
          sampleTrancheId)
        retrievedPartTakeTwo <- ImmutableObjectStorage.retrieve[Part](
          sampleTrancheId)
      } yield {
        retrievedPartTakeTwo should be theSameInstanceAs retrievedPartTakeOne
      }

      ImmutableObjectStorage.runForEffectsOnly(samplingSession)(tranches) shouldBe a[
        Right[_, _]]
    }
  }
}
