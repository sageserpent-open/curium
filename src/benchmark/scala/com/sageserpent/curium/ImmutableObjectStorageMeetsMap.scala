package com.sageserpent.curium

import cats.collections.{AvlMap, AvlSet}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.curium.ImmutableObjectStorage.{
  CompletedOperation,
  IntersessionState,
  Session
}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, Duration}
import scala.util.Random

object ImmutableObjectStorageMeetsMap extends RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId

  def main(args: Array[String]): Unit = {
    tranchesResource
      .use(tranches =>
        IO {
          val intersessionState = new IntersessionState[TrancheId] {
            protected override def finalCustomisationForTrancheCaching(
                caffeine: Caffeine[Any, Any]
            ): Cache[TrancheId, CompletedOperation[TrancheId]] = {
              caffeine
                .maximumSize(10000L)
                .build[TrancheId, CompletedOperation[TrancheId]]
            }
          }

          val intersessionStateForQueries = new IntersessionState[TrancheId] {
            protected override def finalCustomisationForTrancheCaching(
                caffeine: Caffeine[Any, Any]
            ): Cache[TrancheId, CompletedOperation[TrancheId]] = {
              caffeine
                .maximumSize(100L)
                .build[TrancheId, CompletedOperation[TrancheId]]
            }
          }

          val Right(initialTrancheId: TrancheId) = {
            val session: Session[TrancheId] =
              immutableObjectStorage.store(AvlMap.empty[Int, AvlSet[String]])

            immutableObjectStorage
              .runToYieldTrancheId(session, intersessionState)(tranches)
          }

          val randomBehaviour = new Random(53278953)

          var trancheId: TrancheId = initialTrancheId

          val startTime = Deadline.now

          var maximumUpdateDuration: Duration =
            Duration.Zero

          var minimumUpdateDuration: Duration = Duration.Inf

          val updateDurations: mutable.ListBuffer[Duration] =
            mutable.ListBuffer.empty

          val lookbackLimit = 10000000

          val batchSize = 100

          for (step <- 0 until (1000000000, batchSize)) {
            if (0 < step && step % 5000 == 0) {
              val postUpdatesTime = Deadline.now

              val Right(contentsAtPreviousStep) =
                immutableObjectStorage.runToYieldResult(
                  immutableObjectStorage
                    .retrieve[AvlMap[Int, AvlSet[String]]](trancheId)
                    .map(_.get(step - 1).getOrElse(AvlSet.empty).toScalaSet),
                  intersessionStateForQueries
                )(tranches)

              val duration = postUpdatesTime - startTime

              val queryDuration = Deadline.now - postUpdatesTime

              println(
                s"Step: $step, duration to before query: ${duration.toMillis} milliseconds, query duration: ${queryDuration.toMillis} milliseconds, minimum update duration: ${minimumUpdateDuration.toMillis} milliseconds, maximum update duration: ${maximumUpdateDuration.toMillis} milliseconds, average update duration: ${updateDurations
                    .map(_.toMillis)
                    .sum / updateDurations.size}, update durations: ${updateDurations
                    .map(_.toMillis)
                    .groupBy(identity)
                    .toSeq
                    .map { case (duration, group) =>
                      group.size -> duration
                    }
                    .sortBy(_._1)} ,contents at previous step: $contentsAtPreviousStep"
              )

              maximumUpdateDuration = Duration.Zero
              minimumUpdateDuration = Duration.Inf
              updateDurations.clear()
            }

            val session: Session[TrancheId] = for {
              retrievedMap <- immutableObjectStorage
                .retrieve[AvlMap[Int, AvlSet[String]]](trancheId)
              mutatedMap = iterate {
                (count, originalMap: AvlMap[Int, AvlSet[String]]) =>
                  val microStep = step + count

                  val mapWithPossibleRemoval =
                    if (1 == microStep % 5) {
                      val elementToRemove =
                        microStep - randomBehaviour.chooseAnyNumberFromOneTo(
                          lookbackLimit min microStep
                        )
                      originalMap.remove(elementToRemove)
                    } else originalMap

                  mapWithPossibleRemoval + (microStep -> {
                    val set = originalMap
                      .get(
                        if (0 < microStep)
                          microStep - randomBehaviour
                            .chooseAnyNumberFromOneTo(
                              lookbackLimit min microStep
                            )
                        else 0
                      )
                      .getOrElse(AvlSet.empty)
                    if (!set.isEmpty && 1 == microStep % 11)
                      set.remove(set.min.get)
                    else set + microStep.toString
                  })
              }(seed = retrievedMap, numberOfIterations = batchSize)

              newTrancheId <- immutableObjectStorage.store(mutatedMap)
            } yield newTrancheId

            {

              val updateStartTime = Deadline.now

              trancheId = immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
                .right
                .get

              val updateDuration = Deadline.now - updateStartTime

              maximumUpdateDuration = maximumUpdateDuration max updateDuration

              minimumUpdateDuration = minimumUpdateDuration min updateDuration

              updateDurations += updateDuration
            }
          }
        }
      )
      .unsafeRunSync()
  }

  def iterate[X](step: (Int, X) => X)(seed: X, numberOfIterations: Int): X = {
    require(0 <= numberOfIterations)

    @tailrec
    def evaluate(seed: X, count: Int): X =
      if (numberOfIterations > count) evaluate(step(count, seed), 1 + count)
      else seed

    evaluate(seed, 0)
  }

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[RocksDbTranches].getSimpleName
  }
}
