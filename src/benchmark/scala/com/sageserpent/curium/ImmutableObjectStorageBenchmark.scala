package com.sageserpent.curium

import cats.implicits._
import org.scalameter.api.{Bench, Gen, exec}
import org.scalameter.picklers.noPickler._
import org.scalameter.{Parameter, Parameters}

object ImmutableObjectStorageBenchmark extends Bench.ForkedTime {

  import ImmutableObjectStorage._
  import ImmutableObjectStorageSpec._

  val numberOfLeavesGenerator: Gen[TrancheLocalObjectReferenceId] =
    Gen.range("Number of leaves")(10, 300, 5)

  val partGrowthGenerator: Gen[PartGrowth] =
    numberOfLeavesGenerator.flatMap { numberOfLeaves =>
      val seed = numberOfLeaves

      // NASTY HACK: `Trials` doesn't currently support sampling of a single
      // case, so we have to switch to the Java form and dig one out via the
      // iterator support for JUnit.
      val partGrowthSteps: PartGrowth =
        partGrowthLeadingToRootFork(
          allowDuplicates = true,
          numberOfLeavesRequired = numberOfLeaves
        ).javaTrials.withLimit(1).asIterator().next()

      val axisName = "Number of steps"

      new Gen[PartGrowth] {
        override def warmupset: Iterator[PartGrowth] =
          Iterator(partGrowthSteps)

        override def dataset: Iterator[Parameters] =
          Iterator(
            Parameters(
              Parameter[PartGrowthStep](axisName) -> partGrowthSteps.steps.size
            )
          )

        override def generate(params: Parameters): PartGrowth =
          partGrowthSteps // HACK: ignore the supplied parameters, as we are generating only one possible value.

        override def cardinality: Int = 1
      }
    }

  performance of "Bookings" in {
    measure method "storeAndRetrieve" in {
      using(
        partGrowthGenerator
      ) config (exec.benchRuns := 5, exec.jvmflags := List(
        "-Xmx3G"
      )) in activity
    }
  }

  def activity(partGrowth: PartGrowth): Unit = {
    storeAndRetrieve(
      partGrowth,
      configuration(_sessionCycleCountWhenStoredTranchesAreNotRecycled = 1)
        .build(new FakeTranches)
    )
  }

  def storeAndRetrieve(
      partGrowth: PartGrowth,
      immutableObjectStorage: ImmutableObjectStorage[TrancheId]
  ): Unit = {
    val trancheIds: Vector[TrancheId] =
      partGrowth.storeViaMultipleSessions(immutableObjectStorage)

    val retrievalSession: Session[Unit] =
      for (_ <- trancheIds.traverse(immutableObjectStorage.retrieve[Part]))
        yield ()

    val Right(()) =
      immutableObjectStorage.runForEffectsOnly(retrievalSession)
  }
}
