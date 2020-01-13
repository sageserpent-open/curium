package com.sageserpent.curium

import cats.effect.IO
import cats.implicits._
import com.sageserpent.curium.ImmutableObjectStorage.{IntersessionState, Session}

import scala.concurrent.duration.Deadline

object ImmutableObjectStorageMeetsList extends RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[RocksDbTranches].getSimpleName

    override protected def canBeProxiedViaSuperTypes(clazz: Class[_]): Boolean =
      classOf[List[_]].isAssignableFrom(clazz) ||
        super.canBeProxiedViaSuperTypes(clazz)
  }

  def main(args: Array[String]): Unit = {
    tranchesResource
      .use(
        tranches =>
          IO {
            val intersessionState = new IntersessionState[TrancheId](trancheIdCacheMaximumSize = 1000)

            val Right(initialTrancheId: TrancheId) = {
              val session: Session[TrancheId] =
                immutableObjectStorage.store(List.empty[Int])

              immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
            }

            var trancheId: TrancheId = initialTrancheId

            val startTime = Deadline.now

            for (step <- 0 until 40000000) {
              val session: Session[TrancheId] = for {
                list <- immutableObjectStorage.retrieve[List[Int]](trancheId)
                mutatedSet = step :: (if (1 == step % 5) list.tail else list)
                newTrancheId <- immutableObjectStorage.store(mutatedSet)
              } yield newTrancheId

              trancheId = immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
                .right
                .get

              if (step % 5000 == 0) {
                val currentTime = Deadline.now

                val duration = currentTime - startTime

                println(
                  s"Step: $step, duration: ${duration.toMillis} milliseconds"
                )
                println(s"trancheIdToCompletedOperationCache: ${intersessionState.trancheIdToCompletedOperationCache.stats()}, ${intersessionState.trancheIdToCompletedOperationCache.estimatedSize()}")
                println(s"secondChanceTrancheIdToCompletedOperationCache: ${intersessionState.secondChanceTrancheIdToCompletedOperationCache.stats()}, ${intersessionState.secondChanceTrancheIdToCompletedOperationCache.estimatedSize()}")
              }
            }
          }
      )
      .unsafeRunSync()
  }
}
