package com.sageserpent.plutonium.curium

import cats.implicits._
import org.scalameter.api.{Bench, Gen, exec}
import org.scalameter.picklers.noPickler._
import org.scalameter.{Parameter, Parameters}

object ImmutableObjectStorageBenchmark extends Bench.ForkedTime {
  import ImmutableObjectStorage._
  import ImmutableObjectStorageSpec._

  val numberOfLeavesGenerator: Gen[ObjectReferenceId] =
    Gen.range("Number of leaves")(10, 300, 5)

  val partGrowthGenerator: Gen[PartGrowth] =
    numberOfLeavesGenerator.flatMap { numberOfLeaves =>
      val seed = numberOfLeaves

      val partGrowthSteps: PartGrowth =
        partGrowthLeadingToRootFork(allowDuplicates = true,
                                    numberOfLeavesRequired = numberOfLeaves,
                                    seed = seed)

      val axisName = "Number of steps"

      new Gen[PartGrowth] {
        override def warmupset: Iterator[PartGrowth] =
          Iterator(partGrowthSteps)
        override def dataset: Iterator[Parameters] =
          Iterator(Parameters(
            Parameter[PartGrowthStep](axisName) -> partGrowthSteps.steps.size))
        override def generate(params: Parameters): PartGrowth =
          partGrowthSteps // HACK: ignore the supplied parameters, as we are generating only one possible value.
      }
    }

  performance of "Bookings" in {
    measure method "storeAndRetrieve" in {
      using(partGrowthGenerator) config (exec.benchRuns -> 5, exec.jvmflags -> List(
        "-Xmx3G")) in activity
    }
  }

  def activity(partGrowth: PartGrowth): Unit = {
    storeAndRetrieve(partGrowth, new IntersessionState, new FakeTranches)
  }

  def storeAndRetrieve(partGrowth: PartGrowth,
                       intersessionState: IntersessionState[TrancheId],
                       tranches: FakeTranches): Unit = {
    val trancheIds: Vector[TrancheId] =
      partGrowth.storeViaMultipleSessions(intersessionState, tranches)

    val retrievalSession: Session[Unit] =
      for (_ <- trancheIds.traverse(immutableObjectStorage.retrieve[Part]))
        yield ()

    val Right(()) =
      immutableObjectStorage.runForEffectsOnly(retrievalSession,
                                               intersessionState)(tranches)
  }
}
