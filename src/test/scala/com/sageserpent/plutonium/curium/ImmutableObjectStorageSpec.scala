package com.sageserpent.plutonium.curium

import cats.implicits._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.curium.ImmutableObjectStorage._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.collection.mutable.{
  Map => MutableMap,
  SortedMap => MutableSortedMap
}
import scala.util.{Random, Try}

object ImmutableObjectStorageSpec {
  sealed trait Part {
    def subpart(randomBehaviour: Random): Part = this match {
      case leaf: Leaf => leaf
      case fork: Fork =>
        randomBehaviour.chooseAnyNumberFromOneTo(2) match {
          case 1 => fork.distinctSubpart(randomBehaviour)
          case 2 => fork
        }
    }

    def shareSubstructureWithOneOf(parts: Set[Part]): Part =
      parts.find(_ == this).getOrElse {
        this match {
          case Leaf(_) => this
          case Fork(left, id, right) =>
            Fork(left.shareSubstructureWithOneOf(parts),
                 id,
                 right.shareSubstructureWithOneOf(parts))
        }
      }
  }

  case class Leaf(id: Int) extends Part

  case class Fork(left: Part, id: Int, right: Part) extends Part {
    def distinctSubpart(randomBehaviour: Random): Part =
      randomBehaviour.chooseAnyNumberFromOneTo(2) match {
        case 1 => left.subpart(randomBehaviour)
        case 2 => right.subpart(randomBehaviour)
      }
  }

  case object alien

  val seedGenerator: Gen[Int] = Arbitrary.arbInt.arbitrary

  val rootGenerator: Gen[Fork] = for {
    size <- Gen
      .posNum[Int]
      .map(1 + _) // Need at least two parts - a leaf and an overall fork.
    seed <- seedGenerator
    forkChoices <- Gen
      .listOfN(size - 2, Arbitrary.arbBool.arbitrary)
      .map(false +: _ :+ true) // Start with a leaf, end with a fork.
  } yield {
    val randomBehaviour = new Random(seed)
    val parts = (Vector.empty[Part] /: forkChoices) {
      case (subparts, chooseFork) =>
        val numberOfSubparts = subparts.size
        subparts :+ (if (chooseFork) {
                       val indexOfLeftSubpart = randomBehaviour
                         .chooseAnyNumberFromZeroToOneLessThan(numberOfSubparts)
                       val indexOfRightSubpart = randomBehaviour
                         .chooseAnyNumberFromZeroToOneLessThan(numberOfSubparts)
                       Fork(subparts(indexOfLeftSubpart),
                            numberOfSubparts,
                            subparts(indexOfRightSubpart))
                     } else Leaf(numberOfSubparts))
    }

    parts.last.asInstanceOf[Fork]
  }

  import ImmutableObjectStorage._

  class FakeTranches extends Tranches {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] = MutableMap.empty
    val objectReferenceIdsToAssociatedTrancheIdMap
      : MutableSortedMap[ObjectReferenceId, TrancheId] = MutableSortedMap.empty

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

  def chainOfNestedSubparts(root: Fork,
                            randomBehaviour: Random,
                            allowDuplicates: Boolean): Vector[Part] = {
    val subpart =
      if (allowDuplicates) root.subpart(randomBehaviour)
      else root.distinctSubpart(randomBehaviour)
    subpart match {
      case fork: Fork =>
        chainOfNestedSubparts(fork, randomBehaviour, allowDuplicates) :+ subpart
      case leaf: Leaf => Vector(leaf)
    }
  }

  def storeViaMultipleSessions(things: Vector[Part],
                               tranches: Tranches,
                               randomBehaviour: Random): Vector[TrancheId] = {
    var trancheIdsSoFar: Vector[TrancheId] = Vector.empty

    val thingsInChunks: Stream[Vector[Part]] =
      randomBehaviour.splitIntoNonEmptyPieces(things)

    for (chunk <- thingsInChunks) {
      val retrievalAndStorageSession: Session[Vector[TrancheId]] = for {
        retrievedPartsToShareStructureWith <- trancheIdsSoFar.traverse(
          ImmutableObjectStorage.retrieve[Part])
        trancheIds <- chunk
          .map(
            _.shareSubstructureWithOneOf(
              retrievedPartsToShareStructureWith.toSet))
          .traverse(ImmutableObjectStorage.store)
      } yield trancheIds

      val Right(trancheIdsForChunk) =
        ImmutableObjectStorage
          .runToYieldTrancheIds(retrievalAndStorageSession)(tranches)

      trancheIdsSoFar ++= trancheIdsForChunk
    }

    trancheIdsSoFar
  }
}

class ImmutableObjectStorageSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import ImmutableObjectStorageSpec._

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in forAll(
    rootGenerator,
    seedGenerator,
    MinSuccessful(50)) { (root, seed) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val originalParts = chainOfNestedSubparts(root,
                                              randomBehaviour,
                                              allowDuplicates = true) :+ root

    val trancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

    trancheIds should contain theSameElementsAs trancheIds.toSet

    tranches.tranchesById.keys should contain theSameElementsAs trancheIds
  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    rootGenerator,
    seedGenerator,
    MinSuccessful(50)) { (root, seed) =>
    val randomBehaviour = new Random(seed)

    val originalParts = chainOfNestedSubparts(root,
                                              randomBehaviour,
                                              allowDuplicates = true) :+ root

    val tranches = new FakeTranches

    val trancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

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

        unpermutedRetrievedParts should contain theSameElementsInOrderAs originalParts

        Inspectors.forAll(retrievedParts)(retrievedPart =>
          Inspectors.forAll(originalParts)(originalPart =>
            retrievedPart should not be theSameInstanceAs(originalPart)))
      }

    ImmutableObjectStorage.runForEffectsOnly(retrievalSession)(tranches) shouldBe a[
      Right[_, _]]
  }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in forAll(
    rootGenerator,
    seedGenerator,
    MinSuccessful(50)) { (root, seed) =>
    val randomBehaviour = new Random(seed)

    val originalParts = chainOfNestedSubparts(root,
                                              randomBehaviour,
                                              allowDuplicates = true) :+ root

    val tranches = new FakeTranches

    val trancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

    val originalPartsByTrancheId = (trancheIds zip originalParts).toMap

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val samplingSession: Session[Unit] = for {
      bogus <- originalPartsByTrancheId(sampleTrancheId) match {
        case _: Fork => ImmutableObjectStorage.retrieve[Leaf](sampleTrancheId)
        case _: Leaf => ImmutableObjectStorage.retrieve[Fork](sampleTrancheId)
      }
    } yield ()

    ImmutableObjectStorage.runForEffectsOnly(samplingSession)(tranches) shouldBe a[
      Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in forAll(
    rootGenerator,
    seedGenerator,
    MinSuccessful(50)) { (root, seed) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val originalParts = chainOfNestedSubparts(root,
                                              randomBehaviour,
                                              allowDuplicates = false) :+ root

    val trancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

    assert(
      originalParts.size == tranches.tranchesById.size && originalParts.size == trancheIds.size)

    val idOfCorruptedTranche = randomBehaviour.chooseOneOf(trancheIds)

    tranches.tranchesById(idOfCorruptedTranche) = {
      val trancheToCorrupt = tranches.tranchesById(idOfCorruptedTranche)
      val (firstHalf, secondHalf) =
        trancheToCorrupt.serializedRepresentation.splitAt(
          randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
            1 + trancheToCorrupt.serializedRepresentation.length))
      trancheToCorrupt.copy(
        firstHalf ++ "*** CORRUPTION! ***".map(_.toByte) ++ secondHalf)
    }

    val rootTrancheId = trancheIds.last

    val samplingSessionWithCorruptedTranche: Session[Unit] = for {
      _ <- ImmutableObjectStorage.retrieve[Fork](rootTrancheId)
    } yield ()

    ImmutableObjectStorage.runForEffectsOnly(
      samplingSessionWithCorruptedTranche)(tranches) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in forAll(
    rootGenerator,
    seedGenerator,
    MinSuccessful(50)) { (root, seed) =>
    val randomBehaviour = new Random(seed)

    val originalParts = chainOfNestedSubparts(root,
                                              randomBehaviour,
                                              allowDuplicates = false) :+ root

    val tranches = new FakeTranches

    val trancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

    assert(
      originalParts.size == tranches.tranchesById.size && originalParts.size == trancheIds.size)

    val idOfMissingTranche =
      randomBehaviour.chooseOneOf(trancheIds)

    tranches.tranchesById -= idOfMissingTranche

    val rootTrancheId = trancheIds.last

    val samplingSessionWithMissingTranche: Session[Unit] = for {
      _ <- ImmutableObjectStorage.retrieve[Fork](rootTrancheId)
    } yield ()

    ImmutableObjectStorage.runForEffectsOnly(samplingSessionWithMissingTranche)(
      tranches) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in forAll(
    rootGenerator,
    seedGenerator,
    MinSuccessful(50)) { (root, seed) =>
    val randomBehaviour = new Random(seed)

    val originalParts = chainOfNestedSubparts(root,
                                              randomBehaviour,
                                              allowDuplicates = false) :+ root

    val tranches = new FakeTranches

    val Right(alienTrancheId) = ImmutableObjectStorage.runToYieldTrancheId(
      ImmutableObjectStorage.store(alien))(tranches)

    val nonAlienTrancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

    assert(
      1 + originalParts.size == tranches.tranchesById.size && originalParts.size == nonAlienTrancheIds.size)

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

  it should "result in a smaller tranche when there is a tranche chain covering some of its substructure" in forAll(
    rootGenerator,
    seedGenerator,
    MinSuccessful(50)) { (root, seed) =>
    val randomBehaviour = new Random(seed)

    val originalParts = chainOfNestedSubparts(root,
                                              randomBehaviour,
                                              allowDuplicates = true) :+ root

    val isolatedSpokeTranche = {
      val isolatedSpokeTranches = new FakeTranches

      val isolatedSpokeStorageSession: Session[TrancheId] =
        ImmutableObjectStorage.store(root)

      val Right(isolatedTrancheId) =
        ImmutableObjectStorage.runToYieldTrancheId(isolatedSpokeStorageSession)(
          isolatedSpokeTranches)

      isolatedSpokeTranches.tranchesById(isolatedTrancheId)
    }

    val tranches = new FakeTranches

    val trancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

    val rootTrancheId = trancheIds.last

    val rootTranche = tranches.tranchesById(rootTrancheId)

    rootTranche.serializedRepresentation.length should be < isolatedSpokeTranche.serializedRepresentation.length
  }

  it should "be idempotent when retrieving using the same tranche id" in forAll(
    rootGenerator,
    seedGenerator,
    MinSuccessful(50)) { (root, seed) =>
    val randomBehaviour = new Random(seed)

    val originalParts = chainOfNestedSubparts(root,
                                              randomBehaviour,
                                              allowDuplicates = true) :+ root

    val tranches = new FakeTranches

    val trancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val samplingSession: Session[Unit] = for {
      retrievedPartTakeOne <- ImmutableObjectStorage.retrieve[Part](
        sampleTrancheId)
      retrievedPartTakeTwo <- ImmutableObjectStorage.retrieve[Part](
        sampleTrancheId)
    } yield {
      retrievedPartTakeTwo should be(retrievedPartTakeTwo)
    }

    ImmutableObjectStorage.runForEffectsOnly(samplingSession)(tranches) shouldBe a[
      Right[_, _]]
  }
}
