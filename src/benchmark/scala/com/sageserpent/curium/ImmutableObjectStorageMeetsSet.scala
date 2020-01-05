package com.sageserpent.curium

import cats.effect.IO
import cats.implicits._
import com.sageserpent.curium.ImmutableObjectStorage.{IntersessionState, Session}

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
            val intersessionState = new IntersessionState[TrancheId](trancheIdCacheMaximumSize = 20000)

            val Right(initialTrancheId: TrancheId) = {
              val session: Session[TrancheId] =
                immutableObjectStorage.store(Set.empty[Int])

              immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
            }

            var trancheId: TrancheId = initialTrancheId

            val batchSize = 100

            for (step <- 0 until 40000000 by batchSize) {
              val session: Session[TrancheId] = for {
                set <- immutableObjectStorage.retrieve[Set[Int]](trancheId)
                mutatedSet = Timer.timed("Folding over steps")((set /: (step until (batchSize + step)))(_ + _))
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
