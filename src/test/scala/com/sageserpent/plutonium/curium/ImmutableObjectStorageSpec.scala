package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.data.{EitherT, Kleisli, ReaderT, WriterT}
import cats.effect.IO
import cats.implicits._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.Id
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen, ScalacheckShapeless}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inspectors, Matchers}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.util.{Random, Try}

object ImmutableObjectStorageSpec {
  sealed trait Part

  case class Hub(name: String, parent: Option[Hub]) extends Part

  case class Spoke(name: String, hub: Hub) extends Part

  val _ = ScalacheckShapeless // HACK: prevent IntelliJ from removing the
  // import, as it doesn't spot the implicit macro usage.

  implicit val arbitraryName: Arbitrary[String] = Arbitrary(
    Arbitrary.arbInt.arbitrary.map(_.toString))

  val spokeGenerator: Gen[Spoke] = {
    implicitly[Arbitrary[Spoke]].arbitrary
  }

  val seedGenerator: Gen[Int] = Arbitrary.arbInt.arbitrary

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
    WriterT[IO, Vector[(ImmutableObjectStorage.Id, TrancheOfData)], X]

  type TrancheReader[X] =
    ReaderT[IO, Map[ImmutableObjectStorage.Id, TrancheOfData], X]

  trait TranchesUsingWriter extends Tranches[TrancheWriter] {
    override def storeTranche(
        tranche: TrancheOfData): EitherT[TrancheWriter, Throwable, Id] = {
      val id = UUID.randomUUID()
      EitherT.right(id.pure[TrancheWriter].tell(Vector(id -> tranche)))
    }

    override def retrieveTranche(
        id: Id): EitherT[TrancheWriter, Throwable, TrancheOfData] =
      EitherT.leftT(new NotImplementedException())
  }

  trait TranchesUsingReader extends Tranches[TrancheReader] {
    override def storeTranche(
        tranche: TrancheOfData): EitherT[TrancheReader, Throwable, Id] =
      EitherT.leftT[TrancheReader, Id][Throwable](new NotImplementedException())

    override def retrieveTranche(
        id: Id): EitherT[TrancheReader, Throwable, TrancheOfData] =
      EitherT(
        Kleisli
          .ask[IO, Map[ImmutableObjectStorage.Id, TrancheOfData]]
          .flatMap(tranches =>
            Try { tranches(id) }.toEither.pure[TrancheReader]))
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
    MinSuccessful(20)) { (spoke, seed) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    // NOTE: there may indeed be duplicate parts - but we still expect
    // unique tranche ids when the same part is stored several times.
    val originalParts = Vector.fill(10) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession
      : EitherT[TrancheWriter, Throwable, Vector[ImmutableObjectStorage.Id]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(ImmutableObjectStorage.Id, TrancheOfData)],
         Right(trancheIds: Vector[ImmutableObjectStorage.Id])) =
      storageSession.value.run.unsafeRunSync

    trancheIds should contain theSameElementsAs trancheIds.toSet

    tranches.map(_._1) should contain theSameElementsAs trancheIds
  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    spokeGenerator,
    seedGenerator,
    MinSuccessful(20)) { (spoke, seed) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    val originalParts = Vector.fill(10) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession
      : EitherT[TrancheWriter, Throwable, Vector[ImmutableObjectStorage.Id]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(ImmutableObjectStorage.Id, TrancheOfData)],
         Right(trancheIds: Vector[ImmutableObjectStorage.Id])) =
      storageSession.value.run.unsafeRunSync

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
      retrievalSession.value.run(tranches.toMap).unsafeRunSync

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
    MinSuccessful(20)) { (spoke, seed) =>
    val randomBehaviour = new Random(seed)

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter

    val originalParts = Vector.fill(10) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val storageSession
      : EitherT[TrancheWriter, Throwable, Vector[ImmutableObjectStorage.Id]] =
      originalParts.traverse(storage.store)

    val (tranches: Vector[(ImmutableObjectStorage.Id, TrancheOfData)],
         Right(trancheIds: Vector[ImmutableObjectStorage.Id])) =
      storageSession.value.run.unsafeRunSync

    val originalPartsByTrancheId = (trancheIds zip originalParts).toMap

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val storageUsingTheSameTrancheChain: ImmutableObjectStorage[TrancheReader] =
      new ImmutableObjectStorageImplementation[TrancheReader]
      with TranchesUsingReader

    originalPartsByTrancheId(sampleTrancheId) match {
      case _: Spoke =>
        val retrievalSession: EitherT[TrancheReader, Throwable, Hub] =
          storageUsingTheSameTrancheChain.retrieve[Hub](sampleTrancheId)

        retrievalSession.value
          .run(tranches.toMap)
          .unsafeRunSync shouldBe a[Left[_, _]]
      case _: Hub =>
        val retrievalSession: EitherT[TrancheReader, Throwable, Spoke] =
          storageUsingTheSameTrancheChain.retrieve[Spoke](sampleTrancheId)

        retrievalSession.value
          .run(tranches.toMap)
          .unsafeRunSync shouldBe a[Left[_, _]]
    }
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in {}

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in {}

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in {}
}
