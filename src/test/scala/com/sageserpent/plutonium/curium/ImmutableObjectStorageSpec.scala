package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.free.FreeT
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
  class FakeTranches extends Tranches[UUID] {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] =
      MutableMap.empty
    val objectReferenceIdsToAssociatedTrancheIdMap
      : MutableSortedMap[ObjectReferenceId, TrancheId]           = MutableSortedMap.empty
    var _objectReferenceIdOffsetForNewTranche: ObjectReferenceId = 0

    def purgeTranche(trancheId: TrancheId): Unit = {
      val objectReferenceIdsToRemove =
        objectReferenceIdsToAssociatedTrancheIdMap.collect {
          case (objectReferenceId, associatedTrancheId)
              if trancheId == associatedTrancheId =>
            objectReferenceId
        }

      objectReferenceIdsToAssociatedTrancheIdMap --= objectReferenceIdsToRemove

      tranchesById -= trancheId
    }

    override def createTrancheInStorage(
        payload: Array[Byte],
        objectReferenceIdOffset: ObjectReferenceId,
        objectReferenceIds: Set[ObjectReferenceId])
      : EitherThrowableOr[TrancheId] =
      Try {
        val trancheId = UUID.randomUUID()

        tranchesById(trancheId) =
          TrancheOfData(payload, objectReferenceIdOffset)

        for (objectReferenceId <- objectReferenceIds) {
          objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) =
            trancheId
        }

        val alignmentMultipleForObjectReferenceIdsInSeparateTranches = 100

        objectReferenceIds.reduceOption(_ max _).foreach {
          maximumObjectReferenceId =>
            _objectReferenceIdOffsetForNewTranche =
              (1 + maximumObjectReferenceId / alignmentMultipleForObjectReferenceIdsInSeparateTranches) *
                alignmentMultipleForObjectReferenceIdsInSeparateTranches
        }

        trancheId
      }.toEither

    override def retrieveTranche(
        trancheId: TrancheId): scala.Either[scala.Throwable, TrancheOfData] =
      Try { tranchesById(trancheId) }.toEither

    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
      : scala.Either[scala.Throwable, TrancheId] =
      Try { objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) }.toEither

    override def objectReferenceIdOffsetForNewTranche
      : EitherThrowableOr[ObjectReferenceId] =
      _objectReferenceIdOffsetForNewTranche.pure[EitherThrowableOr]
  }

  type TrancheId = FakeTranches#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[FakeTranches].getSimpleName
  }

  val aThing = "Foo"

  sealed trait Part {
    def useProblematicClosure: String
  }

  val labelStrings: Set[String] = Set("Huey", "Duey", "Louie")

  case class Leaf(id: Int, labelString: String) extends Part {
    val problematicClosure: Any => String      = (_: Any) => aThing + aThing
    override def useProblematicClosure: String = problematicClosure()
  }

  case class Fork(left: Part, id: Int, right: Part, labelString: String)
      extends Part {
    val problematicClosure: Any => String      = (_: Any) => s"$aThing"
    override def useProblematicClosure: String = problematicClosure()
  }

  case object alien

  val seedGenerator: Gen[Int] = Arbitrary.arbInt.arbitrary

  type PartGrowthStep = Vector[Part] => Part

  object PartGrowth {
    def apply(steps: Seq[PartGrowthStep], chunkingSeed: Int): PartGrowth = {
      val chunkEndIndices =
        if (1 < steps.size) {
          val randomBehaviour = new Random(chunkingSeed)

          val numberOfRandomIndices =
            randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(steps.size - 2)

          0 +: randomBehaviour
            .buildRandomSequenceOfDistinctIntegersFromZeroToOneLessThan(
              steps.size - 2)
            .map(1 + _)
            .sorted
            .take(numberOfRandomIndices) :+ steps.size
        } else 0 to steps.size

      val chunkSizes = chunkEndIndices.zip(chunkEndIndices.tail).map {
        case (lower, higher) => higher - lower
      }

      PartGrowth(steps, chunkSizes)
    }
  }

  case class PartGrowth(steps: Seq[PartGrowthStep], chunkSizes: Seq[Int]) {
    require(steps.nonEmpty)
    require(steps.size == chunkSizes.sum)

    private def thingsInChunks[Thing](chunkSizes: Seq[Int],
                                      things: Seq[Thing]): Seq[Seq[Thing]] =
      if (chunkSizes.nonEmpty) {
        val (leadingChunk, remainder) = things.splitAt(chunkSizes.head)

        leadingChunk +: thingsInChunks(chunkSizes.tail, remainder)
      } else Seq.empty

    override def toString(): String = {
      val partsInChunks = thingsInChunks(chunkSizes, parts())

      partsInChunks
        .map(chunk => s"[${chunk.mkString(", ")} ]")
        .mkString(", ")
    }

    def parts(): Vector[Part] =
      (Vector.empty[Part] /: steps) {
        case (parts, partGrowthStep) =>
          parts :+ partGrowthStep(parts)
      }

    def storeViaMultipleSessions(tranches: FakeTranches): Vector[TrancheId] = {
      val chunks: Seq[Vector[PartGrowthStep]] =
        thingsInChunks(chunkSizes, steps)
          .map(_.toVector)

      var trancheIdsSoFar: Vector[TrancheId] = Vector.empty

      for (partGrowthSteps <- chunks) {
        val retrievalAndStorageSession: Session[Vector[TrancheId]] = for {
          retrievedPartsFromPreviousSessions <- trancheIdsSoFar.traverse(
            immutableObjectStorage.retrieve[Part])
          newPartsFromThisSession: Vector[Part] = (retrievedPartsFromPreviousSessions /: partGrowthSteps) {
            case (existingSubparts, partGrowthStep) =>
              existingSubparts :+ partGrowthStep(existingSubparts)
          } drop retrievedPartsFromPreviousSessions.size
          trancheIds <- newPartsFromThisSession.traverse(
            immutableObjectStorage.store)
        } yield trancheIds

        val Right(trancheIdsForChunk) =
          immutableObjectStorage
            .runToYieldTrancheIds(retrievalAndStorageSession)(tranches)

        trancheIdsSoFar ++= trancheIdsForChunk
      }

      trancheIdsSoFar
    }
  }

  def partGrowthLeadingToRootForkGenerator(
      allowDuplicates: Boolean): Gen[PartGrowth] =
    for {
      numberOfLeavesRequired <- Gen.posNum[ObjectReferenceId]
      seed                   <- seedGenerator
    } yield
      partGrowthLeadingToRootFork(allowDuplicates, numberOfLeavesRequired, seed)

  def partGrowthLeadingToRootFork(allowDuplicates: Boolean,
                                  numberOfLeavesRequired: Int,
                                  seed: Int): PartGrowth = {
    require(0 < numberOfLeavesRequired)

    val randomBehaviour = new Random(seed)

    def growthSteps(
        partIdSetsCoveredBySubparts: Vector[Set[ObjectReferenceId]],
        numberOfLeaves: ObjectReferenceId): Stream[PartGrowthStep] = {
      val numberOfSubparts                            = partIdSetsCoveredBySubparts.size
      val partId                                      = numberOfSubparts // Makes it easier to read the test cases when debugging; the ids label the growth steps in ascending order.
      val collectingStrandsTogetherAsHaveEnoughLeaves = numberOfLeaves == numberOfLeavesRequired

      require(
        numberOfLeaves < numberOfLeavesRequired ||
          0 < numberOfSubparts && numberOfLeaves == numberOfLeavesRequired)

      val chooseALeaf = numberOfLeaves < numberOfLeavesRequired &&
        (0 == numberOfSubparts || randomBehaviour.nextBoolean())

      val label = randomBehaviour.chooseOneOf(labelStrings)

      if (chooseALeaf) {
        def leaf(subparts: Vector[Part]): Part = {
          require(numberOfSubparts == subparts.size)

          Leaf(numberOfSubparts, label)
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
          if (collectingStrandsTogetherAsHaveEnoughLeaves)
            numberOfSubparts - 1
          else
            randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(numberOfSubparts)
        val indexOfRightSubpart =
          if (collectingStrandsTogetherAsHaveEnoughLeaves) {
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
        val partIdsCoveredByThisFork
          : Set[ObjectReferenceId] = partIdSetsCoveredBySubparts(
          indexOfLeftSubpart) ++ Set(partId) ++ partIdSetsCoveredBySubparts(
          indexOfRightSubpart)
        val allSubpartsIncludedInThisFork =
          partIdSetsCoveredBySubparts.forall(
            _.subsetOf(partIdsCoveredByThisFork))

        def fork(subparts: Vector[Part]): Fork = {
          require(numberOfSubparts == subparts.size)

          Fork(subparts(indexOfLeftSubpart),
               partId,
               subparts(indexOfRightSubpart),
               label)
        }

        val thisCouldBeTheLastStep = allSubpartsIncludedInThisFork &&
          collectingStrandsTogetherAsHaveEnoughLeaves

        fork _ #:: (if (thisCouldBeTheLastStep &&
                        randomBehaviour.nextBoolean()) Stream.empty
                    else
                      growthSteps(
                        partIdSetsCoveredBySubparts :+ partIdsCoveredByThisFork,
                        numberOfLeaves))
      }
    }

    val steps: Stream[PartGrowthStep] =
      growthSteps(Vector.empty, numberOfLeaves = 0).force

    PartGrowth(steps, seed)
  }

  def shrink(partGrowth: PartGrowth): Stream[PartGrowth] = {
    val PartGrowth(steps, chunkSizes) = partGrowth

    if (1 < chunkSizes.size) {
      val randomBehaviour = new Random(partGrowth.hashCode())

      def mergesOfChunkSizes(chunkSizes: Seq[Int]): Stream[Seq[Int]] = {
        val mergedChunkSizes =
          randomBehaviour.splitIntoNonEmptyPieces(chunkSizes).map(_.sum)

        if (mergedChunkSizes == chunkSizes)
          mergesOfChunkSizes(chunkSizes) // This can happen if the chunks are all of size 1.
        else
          mergedChunkSizes #:: (if (1 < mergedChunkSizes.size)
                                  mergesOfChunkSizes(mergedChunkSizes)
                                else Stream.empty)
      }

      val shrunkViaMergingChunks =
        mergesOfChunkSizes(chunkSizes).map(mergedChunkSizes =>
          partGrowth.copy(chunkSizes = mergedChunkSizes))

      val chunkSizesWithoutTheFinalGrowthStep =
        if (chunkSizes.last > 1)
          chunkSizes.init :+ chunkSizes.last - 1
        else chunkSizes.init
      val shrunkViaDroppingAGrowthStep = shrink(
        PartGrowth(steps.init, chunkSizesWithoutTheFinalGrowthStep))

      shrunkViaMergingChunks ++
        shrunkViaDroppingAGrowthStep
    } else
      steps.inits.toStream
        .drop(1)
        .init
        .map(steps => PartGrowth(steps, Seq(steps.size)))
  }
}

class ImmutableObjectStorageSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import ImmutableObjectStorageSpec._

  implicit val shrinker: Shrink[PartGrowth] = Shrink(shrink)

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in forAll(
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true),
    MinSuccessful(100)) { (partGrowth) =>
    val tranches = new FakeTranches with TranchesContracts[TrancheId]

    val trancheIds =
      partGrowth.storeViaMultipleSessions(tranches)

    trancheIds should contain theSameElementsAs trancheIds.toSet

    tranches.tranchesById.keys should contain theSameElementsAs trancheIds

  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true),
    MinSuccessful(100)) { partGrowth =>
    val expectedParts = partGrowth.parts()

    val tranches = new FakeTranches with TranchesContracts[TrancheId]

    val trancheIds =
      partGrowth.storeViaMultipleSessions(tranches)

    val randomBehaviour = new Random(partGrowth.hashCode())

    val forwardPermutation: Map[Int, Int] = randomBehaviour
      .shuffle(Vector.tabulate(trancheIds.size)(identity))
      .zipWithIndex
      .toMap

    val backwardsPermutation = forwardPermutation.map(_.swap)

    // NOTE: as long as we have a complete chain of tranches, it shouldn't matter
    // in what order tranche ids are submitted for retrieval.
    val permutedTrancheIds = Vector(trancheIds.indices map (index =>
      trancheIds(forwardPermutation(index))): _*)

    val retrievalSession: Session[IndexedSeq[Part]] =
      for {
        permutedRetrievedParts <- permutedTrancheIds.traverse(
          immutableObjectStorage.retrieve[Part])

      } yield
        permutedRetrievedParts.indices map (index =>
          permutedRetrievedParts(backwardsPermutation(index)))

    val Right(retrievedParts) =
      immutableObjectStorage.unsafeRun(retrievalSession)(tranches)

    retrievedParts should contain theSameElementsInOrderAs expectedParts

    retrievedParts.map(_.useProblematicClosure) should equal(
      expectedParts.map(_.useProblematicClosure))

    Inspectors.forAll(retrievedParts)(retrievedPart =>
      Inspectors.forAll(expectedParts)(expectedPart =>
        retrievedPart should not be theSameInstanceAs(expectedPart)))

  }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in forAll(
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true),
    MinSuccessful(100)) { partGrowth =>
    val tranches = new FakeTranches with TranchesContracts[TrancheId]

    val trancheIds =
      partGrowth.storeViaMultipleSessions(tranches)

    val referenceParts = partGrowth.parts()

    val referencePartsByTrancheId = (trancheIds zip referenceParts).toMap

    val randomBehaviour = new Random(partGrowth.hashCode())

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val samplingSession: Session[Unit] = for {
      _ <- (referencePartsByTrancheId(sampleTrancheId) match {
        case _: Fork => immutableObjectStorage.retrieve[Leaf](sampleTrancheId)
        case _: Leaf => immutableObjectStorage.retrieve[Fork](sampleTrancheId)
      }) flatMap (part => FreeT.liftT(Try { part.hashCode }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
    } yield ()

    immutableObjectStorage.runForEffectsOnly(samplingSession)(tranches) shouldBe a[
      Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in forAll(
    partGrowthLeadingToRootForkGenerator(allowDuplicates = false),
    MinSuccessful(100)) { partGrowth =>
    val tranches = new FakeTranches with TranchesContracts[TrancheId]

    val trancheIds =
      partGrowth.storeViaMultipleSessions(tranches)

    val randomBehaviour = new Random(partGrowth.hashCode())

    val idOfCorruptedTranche = randomBehaviour.chooseOneOf(trancheIds)

    tranches.tranchesById(idOfCorruptedTranche) = {
      val trancheToCorrupt = tranches.tranchesById(idOfCorruptedTranche)
      trancheToCorrupt.copy(
        payload = "*** CORRUPTION! ***"
          .map(_.toByte)
          .toArray ++ trancheToCorrupt.payload)
    }

    val rootTrancheId = trancheIds.last

    val samplingSessionWithCorruptedTranche: Session[Unit] = for {
      _ <- immutableObjectStorage.retrieve[Fork](rootTrancheId) flatMap (part =>
        FreeT.liftT(Try { part.hashCode }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
    } yield ()

    immutableObjectStorage.runForEffectsOnly(
      samplingSessionWithCorruptedTranche)(tranches) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in forAll(
    partGrowthLeadingToRootForkGenerator(allowDuplicates = false),
    MinSuccessful(100)) { partGrowth =>
    val tranches = new FakeTranches with TranchesContracts[TrancheId]

    val trancheIds =
      partGrowth.storeViaMultipleSessions(tranches)

    val randomBehaviour = new Random(partGrowth.hashCode())

    val idOfMissingTranche =
      randomBehaviour.chooseOneOf(trancheIds)

    tranches.purgeTranche(idOfMissingTranche)

    val rootTrancheId = trancheIds.last

    val samplingSessionWithMissingTranche: Session[Unit] = for {
      _ <- immutableObjectStorage.retrieve[Fork](rootTrancheId) flatMap (part =>
        FreeT.liftT(Try { part.hashCode }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
    } yield ()

    immutableObjectStorage.runForEffectsOnly(samplingSessionWithMissingTranche)(
      tranches) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in forAll(
    partGrowthLeadingToRootForkGenerator(allowDuplicates = false),
    MinSuccessful(100)) { partGrowth =>
    val tranches = new FakeTranches with TranchesContracts[TrancheId]

    val Right(alienTrancheId) = immutableObjectStorage.runToYieldTrancheId(
      immutableObjectStorage.store(alien))(tranches)

    val nonAlienTrancheIds =
      partGrowth.storeViaMultipleSessions(tranches)

    val randomBehaviour = new Random(partGrowth.hashCode())

    val idOfIncorrectlyTypedTranche =
      randomBehaviour.chooseOneOf(nonAlienTrancheIds)

    tranches.tranchesById(idOfIncorrectlyTypedTranche) =
      tranches.tranchesById(alienTrancheId)

    val rootTrancheId = nonAlienTrancheIds.last

    val samplingSessionWithTrancheForIncompatibleType: Session[Unit] = for {
      _ <- immutableObjectStorage.retrieve[Fork](rootTrancheId) flatMap (part =>
        FreeT.liftT(Try { part.hashCode }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
    } yield ()

    immutableObjectStorage.runForEffectsOnly(
      samplingSessionWithTrancheForIncompatibleType)(tranches) shouldBe a[
      Left[_, _]]
  }

  it should "result in a smaller tranche when there is a tranche chain covering some of its substructure" in forAll(
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true),
    MinSuccessful(100)) { partGrowth =>
    val isolatedSpokeTranche = {
      val isolatedSpokeTranches = new FakeTranches
      with TranchesContracts[TrancheId]

      val root = partGrowth.parts().last

      val isolatedSpokeStorageSession: Session[TrancheId] =
        immutableObjectStorage.store(root)

      val Right(isolatedTrancheId) =
        immutableObjectStorage.runToYieldTrancheId(isolatedSpokeStorageSession)(
          isolatedSpokeTranches)

      isolatedSpokeTranches.tranchesById(isolatedTrancheId)
    }

    val tranches = new FakeTranches with TranchesContracts[TrancheId]

    val trancheIds =
      partGrowth.storeViaMultipleSessions(tranches)

    val rootTrancheId = trancheIds.last

    val rootTranche = tranches.tranchesById(rootTrancheId)

    rootTranche.payload.length should be < isolatedSpokeTranche.payload.length
  }

  it should "be idempotent in terms of object identity when retrieving using the same tranche id" in forAll(
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true),
    MinSuccessful(100)) { partGrowth =>
    val tranches = new FakeTranches with TranchesContracts[TrancheId]

    val trancheIds =
      partGrowth.storeViaMultipleSessions(tranches)

    val randomBehaviour = new Random(partGrowth.hashCode())

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val samplingSession: Session[Unit] = for {
      retrievedPartTakeOne <- immutableObjectStorage.retrieve[Part](
        sampleTrancheId)
      retrievedPartTakeTwo <- immutableObjectStorage.retrieve[Part](
        sampleTrancheId)
    } yield {
      retrievedPartTakeTwo should be theSameInstanceAs retrievedPartTakeOne
    }

    immutableObjectStorage.runForEffectsOnly(samplingSession)(tranches) shouldBe a[
      Right[_, _]]
  }
}
