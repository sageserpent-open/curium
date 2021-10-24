package com.sageserpent.curium

import cats.collections.{AvlMap, AvlSet}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.curium.ImmutableObjectStorage.{IntersessionState, Session}

import scala.concurrent.duration.Deadline
import scala.util.Random

object ImmutableObjectStorageMeetsMap extends RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[RocksDbTranches].getSimpleName

    override protected def isExcludedFromBeingProxied(clazz: Class[_]): Boolean = clazz.getName.contains("BTNil")
  }

  def main(args: Array[String]): Unit = {
    tranchesResource
      .use(
        tranches =>
          IO {
            val intersessionState = new IntersessionState[TrancheId]
            val Right(initialTrancheId: TrancheId) = {
              val session: Session[TrancheId] =
                immutableObjectStorage.store(AvlMap.empty[Int, AvlSet[String]])

              immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
            }

            val randomBehaviour = new Random(53278953)

            var trancheId: TrancheId = initialTrancheId

            val startTime = Deadline.now

            val lookbackLimit = 1000000

            for (step <- 0 until 100000000) {
              val session: Session[TrancheId] = for {
                map <- immutableObjectStorage.retrieve[AvlMap[Int, AvlSet[String]]](trancheId)
                mutatedMap = (if (1 == step % 5) {
                  val elementToRemove = step - randomBehaviour.chooseAnyNumberFromOneTo(lookbackLimit min step)
                  map.remove(elementToRemove)
                } else map) + (step -> {
                  val set = map.get(step - randomBehaviour.chooseAnyNumberFromOneTo(lookbackLimit min step)).getOrElse(AvlSet.empty)
                  if (!set.isEmpty && 1 == step % 11) set.remove(set.min.get) else set + step.toString
                })
                newTrancheId <- immutableObjectStorage.store(mutatedMap)
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
              }
            }
          }
      )
      .unsafeRunSync()
  }
}
