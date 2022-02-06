package com.sageserpent.curium

import cats.free.FreeT
import cats.implicits._
import com.sageserpent.americium.Trials
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.curium.ImmutableObjectStorage._
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.mutable.{Map => MutableMap, SortedMap => MutableSortedMap}
import scala.util.{Random, Try}

object ImmutableObjectStorageSpec {

  class FakeTranches extends Tranches[UUID] {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] =
      MutableMap.empty
    val objectReferenceIdsToAssociatedTrancheIdMap
    : MutableSortedMap[ObjectReferenceId, TrancheId] = MutableSortedMap.empty
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
                                         objectReferenceIds: Range)
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
      Try {
        tranchesById(trancheId)
      }.toEither

    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
    : scala.Either[scala.Throwable, TrancheId] =
      Try {
        objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId)
      }.toEither

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
    val problematicClosure: Any => String = (_: Any) => aThing + aThing

    override def useProblematicClosure: String = problematicClosure()
  }

  case class Fork(left: Part, id: Int, right: Part, labelString: String)
    extends Part {
    val problematicClosure: Any => String = (_: Any) => s"$aThing"

    override def useProblematicClosure: String = problematicClosure()
  }

  case object alien

  val api = Trials.api

  val seedTrials: Trials[Int] = api.integers

  type PartGrowthStep = Vector[Part] => Part

  def partGrowthLeadingToRootForkGenerator(allowDuplicates: Boolean): Trials[PartGrowth] =
    for {
      numberOfLeavesRequired <- api.integers(lowerBound = 1, upperBound = 300, shrinkageTarget = 1)
      seed <- seedTrials
      result <- partGrowthLeadingToRootFork(allowDuplicates, numberOfLeavesRequired)
    } yield result

  def partGrowthLeadingToRootFork(allowDuplicates: Boolean,
                                  numberOfLeavesRequired: Int): Trials[PartGrowth] = {
    require(0 < numberOfLeavesRequired)

    def growthSteps(
                     partIdSetsCoveredBySubparts: Vector[Set[ObjectReferenceId]],
                     numberOfLeaves: ObjectReferenceId): Trials[LazyList[PartGrowthStep]] = {
      val numberOfSubparts = partIdSetsCoveredBySubparts.size
      val partId = numberOfSubparts // Makes it easier to read the test cases when debugging; the ids label the growth steps in ascending order.
      val collectingStrandsTogetherAsHaveEnoughLeaves = numberOfLeaves == numberOfLeavesRequired

      require(
        numberOfLeaves < numberOfLeavesRequired ||
          0 < numberOfSubparts && numberOfLeaves == numberOfLeavesRequired)

      for {
        chooseALeaf <- api.booleans.map(coinFlip =>
          numberOfLeaves < numberOfLeavesRequired && (0 == numberOfSubparts || coinFlip))

        label <- api.choose(labelStrings)

        result <- if (chooseALeaf) {
          def leaf(subparts: Vector[Part]): Part = {
            require(numberOfSubparts == subparts.size)

            Leaf(numberOfSubparts, label)
          }

          growthSteps(partIdSetsCoveredBySubparts :+ Set(partId),
            1 + numberOfLeaves).map(leaf _ #:: _)
        } else api.booleans.flatMap(anotherCoinFlip =>
          if (allowDuplicates && 0 < numberOfSubparts && anotherCoinFlip)
            growthSteps(
              partIdSetsCoveredBySubparts :+ partIdSetsCoveredBySubparts.last,
              numberOfLeaves).map(((_: Vector[Part]).last) #:: _)
          else for {
            foo <- api.integers(lowerBound = 0, upperBound = numberOfSubparts - 1, 0)
            bar <- api.integers(lowerBound = 0, upperBound = numberOfSubparts - 1, 0)
            baz <- api.integers(lowerBound = 0, upperBound = numberOfSubparts - 1, 0)
            qux <- {
              val indexOfLeftSubpart =
                if (collectingStrandsTogetherAsHaveEnoughLeaves)
                  numberOfSubparts - 1
                else
                  foo

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
                    bar
                } else
                  baz

              val partIdsCoveredByThisFork
              : Set[ObjectReferenceId] = partIdSetsCoveredBySubparts(
                indexOfLeftSubpart) ++ Set(partId) ++ partIdSetsCoveredBySubparts(
                indexOfRightSubpart)

              val allSubpartsIncludedInThisFork =
                partIdSetsCoveredBySubparts.forall(
                  _.subsetOf(partIdsCoveredByThisFork))

              val thisCouldBeTheLastStep = allSubpartsIncludedInThisFork &&
                collectingStrandsTogetherAsHaveEnoughLeaves

              {
                def fork(subparts: Vector[Part]): Fork = {
                  require(numberOfSubparts == subparts.size)

                  Fork(subparts(indexOfLeftSubpart),
                    partId,
                    subparts(indexOfRightSubpart),
                    label)
                }

                api.booleans.flatMap(coinFlip => if (thisCouldBeTheLastStep &&
                  coinFlip) api.only(LazyList.empty)
                else
                  growthSteps(
                    partIdSetsCoveredBySubparts :+ partIdsCoveredByThisFork,
                    numberOfLeaves)).map(fork _ #:: _)
              }
            }
          } yield qux
        )
      } yield result
    }

    growthSteps(Vector.empty, numberOfLeaves = 0).map(_.force).flatMap(steps => PartGrowth(steps))
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

    def storeViaMultipleSessions(
                                  intersessionState: IntersessionState[TrancheId],
                                  tranches: FakeTranches): Vector[TrancheId] = {
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
          _ = {
            println(newPartsFromThisSession)
          }
          trancheIds <- newPartsFromThisSession.traverse(
            immutableObjectStorage.store)
        } yield trancheIds

        val Right(trancheIdsForChunk) =
          immutableObjectStorage
            .runToYieldTrancheIds(retrievalAndStorageSession,
              intersessionState)(tranches)

        trancheIdsSoFar ++= trancheIdsForChunk
      }

      println("-------------")

      trancheIdsSoFar
    }
  }

  object TrialsHelpers {
    def sequencesOfDistinctIntegersFromZeroToOneLessThan(exclusiveLimit: Int): Trials[LazyList[Int]] = {
      // NOTE: need that mapping of `distinct` as the lazy lists yielded by `several`
      // can (and will because it does not terminate) contain duplicates even though
      // the base choices are unique.
      api.choose(0 until exclusiveLimit)
        .several[LazyList[Int]]
        .map(_.distinct)
        .map(_.take(exclusiveLimit))
    }
  }

  object PartGrowth {

    def apply(steps: Seq[PartGrowthStep]): Trials[PartGrowth] = for {
      chunkEndIndices <- {
        val numberOfSteps = steps.size
        val oneLessThanNumberOfSteps = numberOfSteps - 1
        if (0 < oneLessThanNumberOfSteps) api
          .integers(0, oneLessThanNumberOfSteps - 1)
          .flatMap(numberOfIndices =>
            TrialsHelpers.sequencesOfDistinctIntegersFromZeroToOneLessThan(oneLessThanNumberOfSteps)
              .map { sequence =>
                val innerIndices = sequence
                  .take(numberOfIndices)
                  .map(1 + _)
                  .sorted

                0 +: innerIndices :+ numberOfSteps
              }
          ) else api.only(0 to numberOfSteps)
      }

      chunkSizes = chunkEndIndices.zip(chunkEndIndices.tail).map {
        case (lower, higher) => higher - lower
      }
    } yield PartGrowth(steps, chunkSizes)
  }
}

class ImmutableObjectStorageSpec
  extends AnyFlatSpec
    with Matchers {

  import ImmutableObjectStorageSpec._


  private val maximumNumberOfCases = 100

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true).withLimit(maximumNumberOfCases).supplyTo { partGrowth =>
      val intersessionState = new IntersessionState[TrancheId]

      val tranches = new FakeTranches with TranchesContracts[TrancheId]

      val trancheIds =
        partGrowth.storeViaMultipleSessions(intersessionState, tranches)

      trancheIds should contain theSameElementsAs trancheIds.toSet

      tranches.tranchesById.keys should contain theSameElementsAs trancheIds

    }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true).withLimit(maximumNumberOfCases).supplyTo { partGrowth =>
      val expectedParts = partGrowth.parts()

      val intersessionState = new IntersessionState[TrancheId]

      val tranches = new FakeTranches with TranchesContracts[TrancheId]

      val trancheIds =
        partGrowth.storeViaMultipleSessions(intersessionState, tranches)

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
        immutableObjectStorage.unsafeRun(retrievalSession, intersessionState)(
          tranches)

      retrievedParts should contain theSameElementsInOrderAs expectedParts

      retrievedParts.map(_.useProblematicClosure) should equal(
        expectedParts.map(_.useProblematicClosure))

      Inspectors.forAll(retrievedParts)(retrievedPart =>
        Inspectors.forAll(expectedParts)(expectedPart =>
          retrievedPart should not be theSameInstanceAs(expectedPart)))

    }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true).withLimit(maximumNumberOfCases).supplyTo { partGrowth =>
      val intersessionState = new IntersessionState[TrancheId]

      val tranches = new FakeTranches with TranchesContracts[TrancheId]

      val trancheIds =
        partGrowth.storeViaMultipleSessions(intersessionState, tranches)

      val referenceParts = partGrowth.parts()

      val referencePartsByTrancheId = (trancheIds zip referenceParts).toMap

      val randomBehaviour = new Random(partGrowth.hashCode())

      val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

      val samplingSession: Session[Unit] = for {
        _ <- (referencePartsByTrancheId(sampleTrancheId) match {
          case _: Fork => immutableObjectStorage.retrieve[Leaf](sampleTrancheId)
          case _: Leaf => immutableObjectStorage.retrieve[Fork](sampleTrancheId)
        }) flatMap (part => FreeT.liftT(Try {
          part.hashCode
        }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
      } yield ()

      immutableObjectStorage.runForEffectsOnly(
        samplingSession,
        intersessionState)(tranches) shouldBe a[Left[_, _]]
    }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in
    partGrowthLeadingToRootForkGenerator(allowDuplicates = false).withLimit(maximumNumberOfCases).supplyTo { partGrowth =>
      val intersessionState = new IntersessionState[TrancheId]

      val tranches = new FakeTranches with TranchesContracts[TrancheId]

      val trancheIds =
        partGrowth.storeViaMultipleSessions(intersessionState, tranches)

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
          FreeT.liftT(Try {
            part.hashCode
          }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
      } yield ()

      val freshIntersessionState = new IntersessionState[TrancheId]

      immutableObjectStorage.runForEffectsOnly(
        samplingSessionWithCorruptedTranche,
        freshIntersessionState)(tranches) shouldBe a[Left[_, _]]
    }

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in
    partGrowthLeadingToRootForkGenerator(allowDuplicates = false).withLimit(maximumNumberOfCases).supplyTo { partGrowth =>
      val intersessionState = new IntersessionState[TrancheId]

      val tranches = new FakeTranches with TranchesContracts[TrancheId]

      val trancheIds =
        partGrowth.storeViaMultipleSessions(intersessionState, tranches)

      val randomBehaviour = new Random(partGrowth.hashCode())

      val idOfMissingTranche =
        randomBehaviour.chooseOneOf(trancheIds)

      tranches.purgeTranche(idOfMissingTranche)

      val rootTrancheId = trancheIds.last

      val samplingSessionWithMissingTranche: Session[Unit] = for {
        _ <- immutableObjectStorage.retrieve[Fork](rootTrancheId) flatMap (part =>
          FreeT.liftT(Try {
            part.hashCode
          }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
      } yield ()

      val freshIntersessionState = new IntersessionState[TrancheId]

      immutableObjectStorage.runForEffectsOnly(
        samplingSessionWithMissingTranche,
        freshIntersessionState)(tranches) shouldBe a[Left[_, _]]
    }

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in
    partGrowthLeadingToRootForkGenerator(allowDuplicates = false).withLimit(maximumNumberOfCases).supplyTo { partGrowth =>
      val intersessionState = new IntersessionState[TrancheId]

      val tranches = new FakeTranches with TranchesContracts[TrancheId]

      val Right(alienTrancheId) = immutableObjectStorage.runToYieldTrancheId(
        immutableObjectStorage.store(alien),
        intersessionState)(tranches)

      val nonAlienTrancheIds =
        partGrowth.storeViaMultipleSessions(intersessionState, tranches)

      val randomBehaviour = new Random(partGrowth.hashCode())

      val idOfIncorrectlyTypedTranche =
        randomBehaviour.chooseOneOf(nonAlienTrancheIds)

      tranches.tranchesById(idOfIncorrectlyTypedTranche) =
        tranches.tranchesById(alienTrancheId)

      val rootTrancheId = nonAlienTrancheIds.last

      val samplingSessionWithTrancheForIncompatibleType: Session[Unit] = for {
        _ <- immutableObjectStorage.retrieve[Fork](rootTrancheId) flatMap (part =>
          FreeT.liftT(Try {
            part.hashCode
          }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
      } yield ()

      val freshIntersessionState = new IntersessionState[TrancheId]

      immutableObjectStorage.runForEffectsOnly(
        samplingSessionWithTrancheForIncompatibleType,
        freshIntersessionState)(tranches) shouldBe a[Left[_, _]]
    }

  it should "result in a smaller tranche when there is a tranche chain covering some of its substructure" in
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true).withLimit(maximumNumberOfCases).supplyTo { partGrowth =>
      val isolatedSpokeTranche = {
        val intersessionState = new IntersessionState[TrancheId]

        val isolatedSpokeTranches = new FakeTranches
          with TranchesContracts[TrancheId]

        val root = partGrowth.parts().last

        val isolatedSpokeStorageSession: Session[TrancheId] =
          immutableObjectStorage.store(root)

        val Right(isolatedTrancheId) =
          immutableObjectStorage.runToYieldTrancheId(
            isolatedSpokeStorageSession,
            intersessionState)(isolatedSpokeTranches)

        isolatedSpokeTranches.tranchesById(isolatedTrancheId)
      }

      val intersessionState = new IntersessionState[TrancheId]

      val tranches = new FakeTranches with TranchesContracts[TrancheId]

      val trancheIds =
        partGrowth.storeViaMultipleSessions(intersessionState, tranches)

      val rootTrancheId = trancheIds.last

      val rootTranche = tranches.tranchesById(rootTrancheId)

      rootTranche.payload.length should be < isolatedSpokeTranche.payload.length
    }

  it should "be idempotent in terms of object identity when retrieving using the same tranche id" in
    partGrowthLeadingToRootForkGenerator(allowDuplicates = true).withLimit(maximumNumberOfCases).supplyTo { partGrowth =>
      val intersessionState = new IntersessionState[TrancheId]

      val tranches = new FakeTranches with TranchesContracts[TrancheId]

      val trancheIds =
        partGrowth.storeViaMultipleSessions(intersessionState, tranches)

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

      immutableObjectStorage.runForEffectsOnly(
        samplingSession,
        intersessionState)(tranches) shouldBe a[Right[_, _]]
    }
}
