package com.sageserpent.plutonium.curium

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

  case class PartGrowthStepsInChunks(chunks: Seq[Vector[PartGrowthStep]]) {
    require(chunks.nonEmpty)
    require(chunks.forall(_.nonEmpty))

    override def toString(): String = {

      def partsInChunks(chunkSizes: Seq[Int],
                        parts: Vector[Part]): Seq[Vector[Part]] =
        if (chunkSizes.nonEmpty) {
          val (leadingChunk, remainder) = parts.splitAt(chunkSizes.head)

          leadingChunk +: partsInChunks(chunkSizes.tail, remainder)
        } else Seq.empty

      partsInChunks(chunks.map(_.size), parts())
        .map((chunk: Vector[Part]) => s"[ ${chunk.mkString(", ")} ]")
        .mkString(", ")
    }

    def parts(): Vector[Part] =
      (Vector.empty[Part] /: chunks.flatten) {
        case (parts, partGrowthStep) =>
          parts :+ partGrowthStep(parts)
      }

    def storeViaMultipleSessions(tranches: Tranches): Vector[TrancheId] = {

      var trancheIdsSoFar: Vector[TrancheId] = Vector.empty

      for (partGrowthSteps <- chunks) {
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
  }

  def partGrowthStepsInChunksLeadingToRootForkGenerator(
      allowDuplicates: Boolean): Gen[PartGrowthStepsInChunks] =
    for {
      numberOfLeavesRequired <- Gen.posNum[ObjectReferenceId]
      seed                   <- seedGenerator
    } yield
      partGrowthStepsInChunksLeadingToRootFork(allowDuplicates,
                                               numberOfLeavesRequired,
                                               seed)

  def partGrowthStepsInChunksLeadingToRootFork(
      allowDuplicates: Boolean,
      numberOfLeavesRequired: Int,
      seed: Int): PartGrowthStepsInChunks = {
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

    val chunks: Stream[Vector[PartGrowthStep]] =
      randomBehaviour
        .splitIntoNonEmptyPieces(steps)
        .map(_.toVector)
        .force

    PartGrowthStepsInChunks(chunks)
  }

  def shrink(partGrowthSteps: PartGrowthStepsInChunks)
    : Stream[PartGrowthStepsInChunks] = {
    partGrowthSteps.chunks.inits.toStream
      .drop(1)
      .init
      .map(PartGrowthStepsInChunks.apply)
  }

  class FakeTranches extends Tranches {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] = MutableMap.empty
    val objectReferenceIdsToAssociatedTrancheIdMap
      : MutableSortedMap[ObjectReferenceId, TrancheId] = MutableSortedMap.empty

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

    override protected def storeTrancheAndAssociatedObjectReferenceIds(
        trancheId: TrancheId,
        tranche: TrancheOfData,
        objectReferenceIds: Seq[ObjectReferenceId]): EitherThrowableOr[Unit] = {
      for {
        objectReferenceIdOffsetForNewTranche <- this.objectReferenceIdOffsetForNewTranche
        _ <- Try {
          tranchesById(trancheId) = tranche
          for (objectReferenceId <- objectReferenceIds) {
            require(objectReferenceIdOffsetForNewTranche <= objectReferenceId)
            objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) =
              trancheId
          }
        }.toEither
      } yield ()
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

  // TODO - reinstate shrinkage and see that it works.
  implicit val shrinker: Shrink[PartGrowthStepsInChunks] = Shrink(shrink)

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in forAll(
    partGrowthStepsInChunksLeadingToRootForkGenerator(allowDuplicates = true),
    MinSuccessful(100)) { (partGrowthStepsInChunks) =>
    val tranches = new FakeTranches

    val trancheIds =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

    trancheIds should contain theSameElementsAs trancheIds.toSet

    tranches.tranchesById.keys should contain theSameElementsAs trancheIds

  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    partGrowthStepsInChunksLeadingToRootForkGenerator(allowDuplicates = true),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthStepsInChunks, seed) =>
    val randomBehaviour = new Random(seed)

    val expectedParts = partGrowthStepsInChunks.parts()

    val tranches = new FakeTranches

    val trancheIds =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

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
          ImmutableObjectStorage.retrieve[Part])

      } yield
        permutedRetrievedParts.indices map (index =>
          permutedRetrievedParts(backwardsPermutation(index)))

    val Right(retrievedParts) =
      ImmutableObjectStorage.unsafeRun(retrievalSession)(tranches)

    retrievedParts should contain theSameElementsInOrderAs expectedParts

    retrievedParts.map(_.useProblematicClosure) should equal(
      expectedParts.map(_.useProblematicClosure))

    Inspectors.forAll(retrievedParts)(retrievedPart =>
      Inspectors.forAll(expectedParts)(expectedPart =>
        retrievedPart should not be theSameInstanceAs(expectedPart)))

  }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in forAll(
    partGrowthStepsInChunksLeadingToRootForkGenerator(allowDuplicates = true),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthStepsInChunks, seed) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val trancheIds =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

    val referenceParts = partGrowthStepsInChunks.parts()

    val referencePartsByTrancheId = (trancheIds zip referenceParts).toMap

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val samplingSession: Session[Unit] = for {
      _ <- (referencePartsByTrancheId(sampleTrancheId) match {
        case _: Fork => ImmutableObjectStorage.retrieve[Leaf](sampleTrancheId)
        case _: Leaf => ImmutableObjectStorage.retrieve[Fork](sampleTrancheId)
      }) flatMap (part => FreeT.liftT(Try { part.hashCode }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
    } yield ()

    ImmutableObjectStorage.runForEffectsOnly(samplingSession)(tranches) shouldBe a[
      Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in forAll(
    partGrowthStepsInChunksLeadingToRootForkGenerator(allowDuplicates = false),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthStepsInChunks, seed) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val trancheIds =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

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
      _ <- ImmutableObjectStorage.retrieve[Fork](rootTrancheId) flatMap (part =>
        FreeT.liftT(Try { part.hashCode }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
    } yield ()

    ImmutableObjectStorage.runForEffectsOnly(
      samplingSessionWithCorruptedTranche)(tranches) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in forAll(
    partGrowthStepsInChunksLeadingToRootForkGenerator(allowDuplicates = false),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthStepsInChunks, seed) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val trancheIds =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

    val idOfMissingTranche =
      randomBehaviour.chooseOneOf(trancheIds)

    tranches.purgeTranche(idOfMissingTranche)

    val rootTrancheId = trancheIds.last

    val samplingSessionWithMissingTranche: Session[Unit] = for {
      _ <- ImmutableObjectStorage.retrieve[Fork](rootTrancheId) flatMap (part =>
        FreeT.liftT(Try { part.hashCode }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
    } yield ()

    ImmutableObjectStorage.runForEffectsOnly(samplingSessionWithMissingTranche)(
      tranches) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in forAll(
    partGrowthStepsInChunksLeadingToRootForkGenerator(allowDuplicates = false),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthStepsInChunks, seed) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val Right(alienTrancheId) = ImmutableObjectStorage.runToYieldTrancheId(
      ImmutableObjectStorage.store(alien))(tranches)

    val nonAlienTrancheIds =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

    val idOfIncorrectlyTypedTranche =
      randomBehaviour.chooseOneOf(nonAlienTrancheIds)

    tranches.tranchesById(idOfIncorrectlyTypedTranche) =
      tranches.tranchesById(alienTrancheId)

    val rootTrancheId = nonAlienTrancheIds.last

    val samplingSessionWithTrancheForIncompatibleType: Session[Unit] = for {
      _ <- ImmutableObjectStorage.retrieve[Fork](rootTrancheId) flatMap (part =>
        FreeT.liftT(Try { part.hashCode }.toEither)) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
    } yield ()

    ImmutableObjectStorage.runForEffectsOnly(
      samplingSessionWithTrancheForIncompatibleType)(tranches) shouldBe a[
      Left[_, _]]
  }

  it should "result in a smaller tranche when there is a tranche chain covering some of its substructure" in forAll(
    partGrowthStepsInChunksLeadingToRootForkGenerator(allowDuplicates = true),
    MinSuccessful(100)) { (partGrowthStepsInChunks) =>
    val isolatedSpokeTranche = {
      val isolatedSpokeTranches = new FakeTranches

      val root = partGrowthStepsInChunks.parts().last

      val isolatedSpokeStorageSession: Session[TrancheId] =
        ImmutableObjectStorage.store(root)

      val Right(isolatedTrancheId) =
        ImmutableObjectStorage.runToYieldTrancheId(isolatedSpokeStorageSession)(
          isolatedSpokeTranches)

      isolatedSpokeTranches.tranchesById(isolatedTrancheId)
    }

    val tranches = new FakeTranches

    val trancheIds =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

    val rootTrancheId = trancheIds.last

    val rootTranche = tranches.tranchesById(rootTrancheId)

    rootTranche.serializedRepresentation.length should be < isolatedSpokeTranche.serializedRepresentation.length
  }

  it should "be idempotent in terms of object identity when retrieving using the same tranche id" in forAll(
    partGrowthStepsInChunksLeadingToRootForkGenerator(allowDuplicates = true),
    seedGenerator,
    MinSuccessful(100)) { (partGrowthStepsInChunks, seed) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val trancheIds =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

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
