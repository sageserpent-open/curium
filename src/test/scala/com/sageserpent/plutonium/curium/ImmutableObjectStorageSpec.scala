package com.sageserpent.plutonium.curium

import cats.data.WriterT
import cats.effect.IO
import cats.implicits._
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, ScalacheckShapeless}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

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
    println(spoke)

    val randomBehaviour = new Random(seed)

    def somethingReachableFrom(part: Part): Part = part match {
      case hub @ Hub(_, Some(parent)) =>
        if (randomBehaviour.nextBoolean()) hub
        else somethingReachableFrom(parent)
      case hub @ Hub(_, None) => hub
      case spoke @ Spoke(_, hub) =>
        if (randomBehaviour.nextBoolean()) spoke
        else somethingReachableFrom(hub)
    }

    // NOTE: there may indeed be duplicate parts - but we still expect
    // unique tranche ids when the same part is stored several times.
    val parts = List.fill(10) { somethingReachableFrom(spoke) }

    type TrancheWriter[X] = WriterT[IO, List[TrancheOfData], X]

    val storage: ImmutableObjectStorage[TrancheWriter] = ???

    val session: TrancheWriter[List[ImmutableObjectStorage.Id]] =
      (parts :+ spoke).traverse(storage.store)

    val (tranches: List[TrancheOfData],
         trancheIds: List[ImmutableObjectStorage.Id]) =
      session.run.unsafeRunSync

    trancheIds should contain(theSameElementsAs(trancheIds.toSet))

    tranches should have size trancheIds.size
  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in {}

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in {}

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in {}

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in {}

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in {}
}
