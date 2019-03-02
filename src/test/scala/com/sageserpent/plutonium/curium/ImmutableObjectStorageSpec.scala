package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.Id
import cats.data._
import cats.implicits._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  ObjectReferenceId,
  TrancheId,
  TrancheOfData
}
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen, ScalacheckShapeless}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inspectors, Matchers}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.util.{Random, Try}

object ImmutableObjectStorageSpec {
  sealed trait Part

  case class Hub(id: Int, parent: Option[Hub]) extends Part

  case class Spoke(id: Int, hub: Hub) extends Part

  case object alien extends Part

  val _ = ScalacheckShapeless // HACK: prevent IntelliJ from removing the
  // import, as it doesn't spot the implicit macro usage.

  implicit val arbitraryName: Arbitrary[String] = Arbitrary(
    Arbitrary.arbInt.arbitrary.map(_.toString))

  val spokeGenerator: Gen[Spoke] = {
    implicitly[Arbitrary[Spoke]].arbitrary
  }

  val seedGenerator: Gen[Int] = Arbitrary.arbInt.arbitrary

  val oneLessThanNumberOfPartsGenerator: Gen[Int] = Gen.posNum[Int]

  private def somethingReachableFrom(randomBehaviour: Random)(
      part: Part): Part = {
    def somethingReachableFrom(part: Part): Part = part match {
      case hub @ Hub(_, Some(parent)) =>
        if (randomBehaviour.nextBoolean()) hub
        else somethingReachableFrom(parent)
      case hub @ Hub(_, None) => hub
      case spoke @ Spoke(_, hub) =>
        if (randomBehaviour.nextBoolean()) spoke
        else somethingReachableFrom(hub)
    }

    somethingReachableFrom(part)
  }

  type TrancheWriter[X] =
    Writer[Vector[(TrancheId, TrancheOfData)], X]

  type TrancheReader[X] =
    Reader[Map[TrancheId, TrancheOfData], X]

  trait TranchesUsingWriter extends Tranches[TrancheWriter] {
    override def createTrancheInStorage(
        serializedRepresentation: Array[Byte],
        objectReferenceIds: Seq[ObjectReferenceId])
      : EitherT[TrancheWriter, Throwable, TrancheId] = {
      val id = UUID.randomUUID()
      EitherT.right(
        id.pure[TrancheWriter]
          .tell(Vector(id -> TrancheOfData(serializedRepresentation, ???))))
    }

    override def retrieveTranche(
        id: TrancheId): EitherT[TrancheWriter, Throwable, TrancheOfData] =
      EitherT.leftT(new NotImplementedException())

    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
      : EitherT[TrancheWriter, Throwable, TrancheId] = ???
  }

  trait TranchesUsingReader extends Tranches[TrancheReader] {
    override def createTrancheInStorage(
        serializedRepresentation: Array[Byte],
        objectReferenceIds: Seq[ObjectReferenceId])
      : EitherT[TrancheReader, Throwable, TrancheId] =
      EitherT.leftT[TrancheReader, TrancheId][Throwable](
        new NotImplementedException())

    override def retrieveTranche(
        id: TrancheId): EitherT[TrancheReader, Throwable, TrancheOfData] =
      EitherT(
        Kleisli
          .ask[Id, Map[TrancheId, TrancheOfData]]
          .flatMap(tranches =>
            Try { tranches(id) }.toEither.pure[TrancheReader]))

    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
      : EitherT[TrancheReader, Throwable, TrancheId] = ???
  }
}

class ImmutableObjectStorageSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import ImmutableObjectStorageSpec._

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    // NOTE: there may indeed be duplicate parts - but we still expect
    // unique tranche ids when the same part is stored several times.
    val originalParts = Vector.fill(1 + oneLessThanNumberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession: EitherT[TrancheWriter, Throwable, Vector[TrancheId]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(TrancheId, TrancheOfData)],
         Right(trancheIds: Vector[TrancheId])) =
      storageSession.value.run

    trancheIds should contain theSameElementsAs trancheIds.toSet

    tranches.map(_._1) should contain theSameElementsAs trancheIds
  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    val originalParts = Vector.fill(1 + oneLessThanNumberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession: EitherT[TrancheWriter, Throwable, Vector[TrancheId]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(TrancheId, TrancheOfData)],
         Right(trancheIds: Vector[TrancheId])) =
      storageSession.value.run

    val forwardPermutation: Map[Int, Int] = randomBehaviour
      .shuffle(Vector.tabulate(trancheIds.size)(identity))
      .zipWithIndex
      .toMap

    val backwardsPermutation = forwardPermutation.map(_.swap)

    // NOTE: as long as we have a complete chain of tranches, it shouldn't matter
    // in what order tranche ids are submitted for retrieval.
    val permutedTrancheIds = Vector(trancheIds.indices map (index =>
      trancheIds(forwardPermutation(index))): _*)

    val storageUsingTheSameTrancheChain: ImmutableObjectStorage[TrancheReader] =
      new ImmutableObjectStorageImplementation[TrancheReader]
      with TranchesUsingReader

    val retrievalSession: EitherT[TrancheReader, Throwable, Vector[Part]] =
      permutedTrancheIds.traverse(
        storageUsingTheSameTrancheChain.retrieve[Part])

    val Right(retrievedParts: Vector[Part]) =
      retrievalSession.value.run(tranches.toMap)

    val unpermutedRetrievedParts = retrievedParts.indices map (index =>
      retrievedParts(backwardsPermutation(index)))

    unpermutedRetrievedParts should contain theSameElementsInOrderAs originalParts

    Inspectors.forAll(retrievedParts)(retrievedPart =>
      Inspectors.forAll(originalParts)(originalPart =>
        retrievedPart should not be theSameInstanceAs(originalPart)))
  }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession: EitherT[TrancheWriter, Throwable, Vector[TrancheId]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(TrancheId, TrancheOfData)],
         Right(trancheIds: Vector[TrancheId])) =
      storageSession.value.run

    val originalPartsByTrancheId = (trancheIds zip originalParts).toMap

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val storageUsingTheSameTrancheChain: ImmutableObjectStorage[TrancheReader] =
      new ImmutableObjectStorageImplementation[TrancheReader]
      with TranchesUsingReader

    val tranchesMap = tranches.toMap

    originalPartsByTrancheId(sampleTrancheId) match {
      case _: Spoke =>
        val retrievalSession: EitherT[TrancheReader, Throwable, Hub] =
          storageUsingTheSameTrancheChain.retrieve[Hub](sampleTrancheId)

        retrievalSession.value
          .run(tranchesMap) shouldBe a[Left[_, _]]
      case _: Hub =>
        val retrievalSession: EitherT[TrancheReader, Throwable, Spoke] =
          storageUsingTheSameTrancheChain.retrieve[Spoke](sampleTrancheId)

        retrievalSession.value
          .run(tranchesMap) shouldBe a[Left[_, _]]
    }
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession: EitherT[TrancheWriter, Throwable, Vector[TrancheId]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(TrancheId, TrancheOfData)],
         Right(trancheIds: Vector[TrancheId])) =
      storageSession.value.run

    assert(
      originalParts.size == tranches.size && originalParts.size == trancheIds.size)

    val idOfCorruptedTranche =
      randomBehaviour.chooseOneOf(trancheIds)

    val storageUsingTheSameTrancheChain: ImmutableObjectStorage[TrancheReader] =
      new ImmutableObjectStorageImplementation[TrancheReader]
      with TranchesUsingReader

    val spokeTrancheId = trancheIds.last

    val retrievalSession: EitherT[TrancheReader, Throwable, Spoke] =
      storageUsingTheSameTrancheChain.retrieve[Spoke](spokeTrancheId)

    val tranchesMap = tranches.toMap

    retrievalSession.value
      .run(
        tranchesMap.updated(
          idOfCorruptedTranche, {
            val trancheToCorrupt = tranchesMap(idOfCorruptedTranche)
            val (firstHalf, secondHalf) =
              trancheToCorrupt.serializedRepresentation.splitAt(
                randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
                  1 + trancheToCorrupt.serializedRepresentation.length))
            trancheToCorrupt.copy(
              firstHalf ++ "*** CORRUPTION! ***".map(_.toByte) ++ secondHalf)
          }
        )) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession: EitherT[TrancheWriter, Throwable, Vector[TrancheId]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(TrancheId, TrancheOfData)],
         Right(trancheIds: Vector[TrancheId])) =
      storageSession.value.run

    assert(
      originalParts.size == tranches.size && originalParts.size == trancheIds.size)

    val idOfMissingTranche =
      randomBehaviour.chooseOneOf(trancheIds)

    val storageUsingTheSameTrancheChain: ImmutableObjectStorage[TrancheReader] =
      new ImmutableObjectStorageImplementation[TrancheReader]
      with TranchesUsingReader

    val spokeTrancheId = trancheIds.last

    val retrievalSession: EitherT[TrancheReader, Throwable, Spoke] =
      storageUsingTheSameTrancheChain.retrieve[Spoke](spokeTrancheId)

    val tranchesMap = tranches.toMap

    retrievalSession.value
      .run(tranchesMap - idOfMissingTranche) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession: EitherT[TrancheWriter, Throwable, Vector[TrancheId]] =
      (alien +: originalParts).traverse(storage.store)

    val (tranches: Vector[(TrancheId, TrancheOfData)],
         Right(trancheIds: Vector[TrancheId])) =
      storageSession.value.run

    assert(
      1 + originalParts.size == tranches.size && 1 + originalParts.size == trancheIds.size)

    val nonAlienTrancheIds = trancheIds.drop(1)

    val idOfIncorrectlyTypedTranche =
      randomBehaviour.chooseOneOf(nonAlienTrancheIds)

    val storageUsingTheSameTrancheChain: ImmutableObjectStorage[TrancheReader] =
      new ImmutableObjectStorageImplementation[TrancheReader]
      with TranchesUsingReader

    val spokeTrancheId = trancheIds.last

    val retrievalSession: EitherT[TrancheReader, Throwable, Spoke] =
      storageUsingTheSameTrancheChain.retrieve[Spoke](spokeTrancheId)

    val tranchesMap = tranches.toMap

    retrievalSession.value
      .run(
        tranchesMap.updated(
          idOfIncorrectlyTypedTranche,
          tranchesMap(trancheIds.head))) shouldBe a[Left[_, _]]
  }

  it should "result in a smaller tranche when there is a tranche chain covering some of its substructure" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, twoLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    // NOTE: there may indeed be duplicate parts - but we still expect
    // unique tranche ids when the same part is stored several times.
    val originalParts = Vector.fill(2 + twoLessThanNumberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession: EitherT[TrancheWriter, Throwable, Vector[TrancheId]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(TrancheId, TrancheOfData)], _) =
      storageSession.value.run

    val isolatedSpokeStorage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    val isolatedSpokeStorageSession
      : EitherT[TrancheWriter, Throwable, TrancheId] =
      storage.store(spoke)

    val (Vector((_, isolatedSpokeTranche: TrancheOfData)), _) =
      isolatedSpokeStorageSession.value.run

    val (_, spokeTranche) = tranches.last

    spokeTranche.serializedRepresentation.length should be < isolatedSpokeTranche.serializedRepresentation.length
  }

  it should "be idempotent when retrieving using the same tranche id" in {}
}
