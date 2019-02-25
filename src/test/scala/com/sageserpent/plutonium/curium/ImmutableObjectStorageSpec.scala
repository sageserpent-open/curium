package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.Monad
import cats.data.{Kleisli, ReaderT, WriterT}
import cats.effect.IO
import cats.implicits._
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.Id
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, ScalacheckShapeless}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.util.{Failure, Random, Try}

object ImmutableObjectStorageSpec {
  sealed trait Part

  case class Hub(name: String, parent: Option[Hub]) extends Part

  case class Spoke(name: String, hub: Hub) extends Part

  val _ = ScalacheckShapeless // HACK: prevent IntelliJ from removing the
  // import, as it doesn't spot the implicit macro usage.

  implicit val arbitraryName = Arbitrary(
    Arbitrary.arbInt.arbitrary.map(_.toString))

  val spokeGenerator = {
    implicitly[Arbitrary[Spoke]].arbitrary
  }

  val seedGenerator = Arbitrary.arbInt.arbitrary

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
    WriterT[IO, List[(ImmutableObjectStorage.Id, TrancheOfData)], X]

  type TrancheReader[X] =
    ReaderT[IO, Map[ImmutableObjectStorage.Id, TrancheOfData], X]

  trait TranchesUsingWriter extends Tranches[TrancheWriter] {
    override def store(tranche: TrancheOfData): TrancheWriter[Try[Id]] = {
      val id = UUID.randomUUID()
      Try(id).pure[TrancheWriter].tell(List(id -> tranche))
    }

    override def retrieve(id: Id): TrancheWriter[Try[TrancheOfData]] =
      (Failure(new NotImplementedException()): Try[TrancheOfData])
        .pure[TrancheWriter]
  }

  trait TranchesUsingReader extends Tranches[TrancheReader] {
    override def store(tranche: TrancheOfData): TrancheReader[Try[Id]] =
      (Failure(new NotImplementedException()): Try[Id]).pure[TrancheReader]

    override def retrieve(id: Id): TrancheReader[Try[TrancheOfData]] =
      Kleisli
        .ask[IO, Map[ImmutableObjectStorage.Id, TrancheOfData]]
        .flatMap(tranches => Try(tranches.apply(id)).pure[TrancheReader])
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

    // NOTE: there may indeed be duplicate parts - but we still expect
    // unique tranche ids when the same part is stored several times.
    val parts = List.fill(10) { somethingReachableFrom(randomBehaviour)(spoke) }

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter {
        override implicit val monadEvidence: Monad[TrancheWriter] =
          implicitly[Monad[TrancheWriter]]
      }

    val storageSession: TrancheWriter[List[ImmutableObjectStorage.Id]] =
      (parts :+ spoke).traverse(storage.store)

    val (tranches: List[(ImmutableObjectStorage.Id, TrancheOfData)],
         trancheIds: List[ImmutableObjectStorage.Id]) =
      storageSession.run.unsafeRunSync

    trancheIds should contain(theSameElementsAs(trancheIds.toSet))

    tranches.map(_._1) should contain(theSameElementsAs(trancheIds))
  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    spokeGenerator,
    seedGenerator,
    MinSuccessful(20)) { (spoke, seed) =>
    val randomBehaviour = new Random(seed)

    val parts = List.fill(10) { somethingReachableFrom(randomBehaviour)(spoke) }

    val storage: ImmutableObjectStorage[TrancheWriter] =
      new ImmutableObjectStorageImplementation[TrancheWriter]
      with TranchesUsingWriter {
        override implicit val monadEvidence: Monad[TrancheWriter] =
          implicitly[Monad[TrancheWriter]]
      }

    val storageSession: TrancheWriter[List[ImmutableObjectStorage.Id]] =
      (parts :+ spoke).traverse(storage.store)

    val (tranches: List[(ImmutableObjectStorage.Id, TrancheOfData)],
         trancheIds: List[ImmutableObjectStorage.Id]) =
      storageSession.run.unsafeRunSync

    // NOTE: as long as we have a complete chain of tranches, it shouldn't matter
    // in what order tranche ids are submitted for retrieval.
    val permutedTrancheIds: List[ImmutableObjectStorage.Id] =
      randomBehaviour.shuffle(trancheIds)

    val storageUsingTheSameTrancheChain: ImmutableObjectStorage[TrancheReader] =
      new ImmutableObjectStorageImplementation[TrancheReader]
      with TranchesUsingReader {
        override implicit val monadEvidence: Monad[TrancheReader] =
          implicitly[Monad[TrancheReader]]
      }

    val retrievalSession: TrancheReader[List[Part]] =
      permutedTrancheIds.traverse(
        storageUsingTheSameTrancheChain.retrieve[Part])

    val retrievedParts: List[Part] =
      retrievalSession.run(tranches.toMap).unsafeRunSync

    retrievedParts should contain(theSameElementsAs(parts :+ spoke))
  }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in {}

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in {}

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in {}

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in {}
}
