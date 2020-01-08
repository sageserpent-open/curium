package com.sageserpent.curium

import cats.effect.IO
import cats.implicits._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.curium.ImmutableObjectStorage.{IntersessionState, Session}

import scala.util.Random

object ImmutableObjectStorageMeetsSet extends RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[RocksDbTranches].getSimpleName
  }

  def main(args: Array[String]): Unit = {
    tranchesResource
      .use(
        tranches =>
          IO {
            val batchSize = 100

            val intersessionState = new IntersessionState[TrancheId](trancheIdCacheMaximumSize = 100 * batchSize)

            val Right(initialTrancheId: TrancheId) = {
              val session: Session[TrancheId] =
                immutableObjectStorage.store(Set.empty[Int])

              immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
            }

            val randomBehaviour = new Random(53278953)

            var trancheId: TrancheId = initialTrancheId

            for (step <- 0 until 40000000 by batchSize) {
              val session: Session[TrancheId] = for {
                set <- immutableObjectStorage.retrieve[Set[Int]](trancheId)
                mutatedSet = Timer.timed("Folding over steps")((set /: (step until (batchSize + step)))
                  ((set2: Set[Int], step2: Int) => (if (1 == step2 % 5) {
                    val elementToRemove = randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(step2)
                    set2 - elementToRemove
                  } else set2) + step2))
                newTrancheId <- immutableObjectStorage.store(mutatedSet)
              } yield newTrancheId

              trancheId = immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
                .right
                .get

              if (step % 5000 == 0) {
                Timer.sampleAndPrintResults(s"$step")
                intersessionState.dumpStatistics()
              }
            }
          }
      )
      .unsafeRunSync()
  }
}
