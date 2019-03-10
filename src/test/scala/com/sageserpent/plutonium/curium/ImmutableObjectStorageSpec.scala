package com.sageserpent.plutonium.curium

import cats.implicits._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.curium.ImmutableObjectStorage._

import org.scalacheck.{Arbitrary, Gen, ScalacheckShapeless, Shrink}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.collection.mutable.{Map => MutableMap}
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Random, Try}

object ImmutableObjectStorageSpec {
  sealed trait Part {
    def subPart(randomBehaviour: Random): Part = this match {
      case Leaf(_) =>
        this
      case Fork(left, _, right) =>
        randomBehaviour.chooseAnyNumberFromOneTo(3) match {
          case 1 => left.subPart(randomBehaviour)
          case 2 => right.subPart(randomBehaviour)
          case 3 => this
        }
    }

    def shareSubstructureWithOneOf(parts: Seq[Part]): Part = ???
  }

  case class Leaf(id: Int) extends Part

  case class Fork(left: Part, id: Int, right: Part) extends Part

  case object alien

  val rootGenerator: Gen[Fork] = {
    import org.scalacheck.ScalacheckShapeless._

    val _ = ScalacheckShapeless // HACK: prevent IntelliJ from removing the
    // import, as it doesn't spot the implicit macro usage.

    implicitly[Arbitrary[Fork]].arbitrary
  }

  val seedGenerator: Gen[Int] = Arbitrary.arbInt.arbitrary

  val numberOfReachablePartsGenerator: Gen[Int] = Gen.posNum[Int] map (_ - 1)

  import ImmutableObjectStorage._

  class FakeTranches extends Tranches {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] = MutableMap.empty

    override protected def storeTrancheAndAssociatedObjectReferenceIds(
        trancheId: TrancheId,
        tranche: TrancheOfData,
        objectReferenceIds: Seq[ObjectReferenceId]): EitherThrowableOr[Unit] = {
      Try {
        tranchesById(trancheId) = tranche
      }.toEither
    }
    override def retrieveTranche(
        id: TrancheId): scala.Either[scala.Throwable, TrancheOfData] =
      Try { tranchesById(id) }.toEither
    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
      : scala.Either[scala.Throwable, TrancheId] =
      ???
  }

  def storeViaMultipleSessions[X: TypeTag](
      things: Vector[X],
      tranches: Tranches,
      randomBehaviour: Random): Vector[TrancheId] = {
    var trancheIdsSoFar: Vector[TrancheId] = Vector.empty

    val thingsInChunks: Stream[Vector[X]] =
      randomBehaviour.splitIntoNonEmptyPieces(things)

    for (chunk <- thingsInChunks) {
      val retrievalAndStorageSession: Session[Vector[TrancheId]] = for {
        _          <- trancheIdsSoFar.traverse(ImmutableObjectStorage.retrieve[X])
        trancheIds <- chunk.traverse(ImmutableObjectStorage.store)
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

  implicit def shrinkAny[T]: Shrink[T] = Shrink(_ => Stream.empty)

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in forAll(
    rootGenerator,
    seedGenerator,
    numberOfReachablePartsGenerator,
    MinSuccessful(20)) { (root, seed, numberOfReachableParts) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    // NOTE: there may indeed be duplicate parts - but we still expect
    // unique tranche ids when the same part is stored several times.
    val originalParts = Vector.fill(numberOfReachableParts) {
      root.subPart(randomBehaviour)
    } :+ root

    val trancheIds =
      storeViaMultipleSessions(originalParts, tranches, randomBehaviour)

    trancheIds should contain theSameElementsAs trancheIds.toSet

    tranches.tranchesById.keys should contain theSameElementsAs trancheIds
  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    rootGenerator,
    seedGenerator,
    numberOfReachablePartsGenerator,
    MinSuccessful(20)) { (root, seed, numberOfReachableParts) =>
    val randomBehaviour = new Random(seed)

    val originalParts = Vector.fill(numberOfReachableParts) {
      root.subPart(randomBehaviour)
    } :+ root

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
    numberOfReachablePartsGenerator,
    MinSuccessful(20)) { (root, seed, numberOfReachableParts) =>
    val randomBehaviour = new Random(seed)

    val originalParts = Vector.fill(numberOfReachableParts) {
      root.subPart(randomBehaviour)
    } :+ root

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
    numberOfReachablePartsGenerator,
    MinSuccessful(20)) { (root, seed, numberOfReachableParts) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val originalParts = Vector.fill(numberOfReachableParts) {
      root.subPart(randomBehaviour)
    } :+ root

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
    numberOfReachablePartsGenerator,
    MinSuccessful(20)) { (root, seed, numberOfReachableParts) =>
    val randomBehaviour = new Random(seed)

    val originalParts = Vector.fill(numberOfReachableParts) {
      root.subPart(randomBehaviour)
    } :+ root

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
    numberOfReachablePartsGenerator,
    MinSuccessful(20)) { (root, seed, numberOfReachableParts) =>
    val randomBehaviour = new Random(seed)

    val originalParts = Vector.fill(numberOfReachableParts) {
      root.subPart(randomBehaviour)
    } :+ root

    val tranches = new FakeTranches

    val trancheIds = storeViaMultipleSessions(alien +: originalParts,
                                              tranches,
                                              randomBehaviour)

    assert(
      1 + originalParts.size == tranches.tranchesById.size && 1 + originalParts.size == trancheIds.size)

    val (Vector(alienTrancheId), nonAlienTrancheIds) = trancheIds.splitAt(1)

    val idOfIncorrectlyTypedTranche =
      randomBehaviour.chooseOneOf(nonAlienTrancheIds)

    tranches.tranchesById(idOfIncorrectlyTypedTranche) =
      tranches.tranchesById(alienTrancheId)

    val rootTrancheId = trancheIds.last

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
    numberOfReachablePartsGenerator,
    MinSuccessful(20)) { (root, seed, oneLessThanNumberOfReachableParts) =>
    val randomBehaviour = new Random(seed)

    val numberOfReachableParts = 1 + oneLessThanNumberOfReachableParts // Have to have at least one reachable part in addition to the root to force sharing of substructure.

    val originalParts = Vector.fill(numberOfReachableParts) {
      root.subPart(randomBehaviour)
    } :+ root

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
    numberOfReachablePartsGenerator,
    MinSuccessful(20)) { (root, seed, numberOfReachableParts) =>
    val randomBehaviour = new Random(seed)

    val originalParts = Vector.fill(numberOfReachableParts) {
      root.subPart(randomBehaviour)
    } :+ root

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
