package com.sageserpent.plutonium.curium

import org.scalameter.api.{Bench, Gen, exec}
import cats.implicits._

import scala.util.Random

object ImmutableObjectStorageBenchmark extends Bench.ForkedTime {
  import ImmutableObjectStorage._
  import ImmutableObjectStorageSpec._

  val numberOfLeaves = Gen.range("Number of leaves")(10, 200, 5)

  performance of "Bookings" in {
    measure method ("storeAndRetrieve") in {
      using(numberOfLeaves) config (exec.benchRuns -> 5, exec.jvmflags -> List(
        "-Xmx3G")) in activity
    }
  }

  def activity(numberOfLeaves: Int): Unit = {
    val seed = numberOfLeaves

    val partGrowthSteps = partGrowthStepsLeadingToRootFork(
      allowDuplicates = true,
      numberOfLeavesRequired = numberOfLeaves,
      seed = seed)

    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    storeAndRetrieve(partGrowthSteps, randomBehaviour, tranches)
  }

  def storeAndRetrieve(partGrowthSteps: Stream[PartGrowthStep],
                       randomBehaviour: Random,
                       tranches: FakeTranches) = {
    val trancheIds: Vector[TrancheId] =
      storeViaMultipleSessions(partGrowthSteps, tranches, randomBehaviour)

    val retrievalSession: Session[Unit] =
      for (_ <- trancheIds.traverse(ImmutableObjectStorage.retrieve[Part]))
        yield ()

    val Right(()) =
      ImmutableObjectStorage.runForEffectsOnly(retrievalSession)(tranches)
  }
}
