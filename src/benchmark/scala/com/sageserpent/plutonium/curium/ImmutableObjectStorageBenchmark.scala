package com.sageserpent.plutonium.curium

import cats.implicits._
import org.scalameter.api.{Bench, Gen, exec}
import org.scalameter.picklers.noPickler._
import org.scalameter.{Parameter, Parameters}

import scala.util.Random

object ImmutableObjectStorageBenchmark extends Bench.ForkedTime {
  import ImmutableObjectStorage._
  import ImmutableObjectStorageSpec._

  val numberOfLeavesGenerator: Gen[ObjectReferenceId] =
    Gen.range("Number of leaves")(10, 200, 5)

  val partGrowthStepsInChunksGenerator: Gen[PartGrowthStepsInChunks] =
    numberOfLeavesGenerator.flatMap { numberOfLeaves =>
      val seed = numberOfLeaves

      val partGrowthSteps: PartGrowthStepsInChunks =
        partGrowthStepsInChunksLeadingToRootFork(allowDuplicates = true,
                                                 numberOfLeavesRequired =
                                                   numberOfLeaves,
                                                 seed = seed)

      val axisName = "Number of steps"

      new Gen[PartGrowthStepsInChunks] {
        override def warmupset: Iterator[PartGrowthStepsInChunks] =
          Iterator(partGrowthSteps)
        override def dataset: Iterator[Parameters] =
          Iterator(Parameters(
            Parameter[PartGrowthStep](axisName) -> partGrowthSteps.chunks.size))
        override def generate(params: Parameters): PartGrowthStepsInChunks =
          partGrowthSteps // HACK: ignore the supplied parameters, as we are generating only one possible value.
      }
    }

  performance of "Bookings" in {
    measure method "storeAndRetrieve" in {
      using(partGrowthStepsInChunksGenerator) config (exec.benchRuns -> 5, exec.jvmflags -> List(
        "-Xmx3G")) in activity
    }
  }

  def activity(partGrowthStepsInChunks: PartGrowthStepsInChunks): Unit = {
    val seed = partGrowthStepsInChunks.hashCode()

    val tranches = new FakeTranches

    storeAndRetrieve(partGrowthStepsInChunks, tranches)
  }

  def storeAndRetrieve(partGrowthStepsInChunks: PartGrowthStepsInChunks,
                       tranches: FakeTranches): Unit = {
    val trancheIds: Vector[TrancheId] =
      partGrowthStepsInChunks.storeViaMultipleSessions(tranches)

    val retrievalSession: Session[Unit] =
      for (_ <- trancheIds.traverse(ImmutableObjectStorage.retrieve[Part]))
        yield ()

    val Right(()) =
      ImmutableObjectStorage.runForEffectsOnly(retrievalSession)(tranches)
  }
}
