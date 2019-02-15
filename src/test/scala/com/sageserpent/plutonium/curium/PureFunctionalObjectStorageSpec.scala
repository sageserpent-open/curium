package com.sageserpent.plutonium.curium
import java.util.UUID

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

object TrancheOfData {
  type ObjectReferenceId = Int

  type Id = UUID

  // TODO - need to be able to create a tranche from an object.
}

trait TrancheOfData // TODO - add operations, somehow we need to be able to reconstitute an object conforming to a type.

trait Tranches {
  // Imperative...
  def tranchFor(referenceId: TrancheOfData.Id): Option[TrancheOfData]

  // Imperative...
  def persist(tranche: TrancheOfData): TrancheOfData.Id
}

class PureFunctionalObjectStorageSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  "storing a pure functional object" should "yield a tranche id and a corresponding tranche of data" in {}

  "reconstituting a pure functional object via a tranche id" should "yield an object that is equal to what was stored" in {}

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in {}

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in {}

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in {}

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in {}
}
