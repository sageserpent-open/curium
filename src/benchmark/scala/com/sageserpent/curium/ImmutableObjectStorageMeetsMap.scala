package com.sageserpent.curium

import cats.collections.{AvlMap, AvlSet}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.curium.ImmutableObjectStorage.{
  IntersessionState,
  Session
}

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.Random

object ImmutableObjectStorageMeetsMap extends RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId

  def main(args: Array[String]): Unit = {
    tranchesResource
      .use(tranches =>
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

          var maximumUpdateDuration: FiniteDuration =
            FiniteDuration(0L, TimeUnit.MILLISECONDS)

          val lookbackLimit = 10000000

          val batchSize = 20

          for (step <- 0 until (1000000000, batchSize)) {
            if (step % 5000 == 0) {
              val currentTime = Deadline.now

              val duration = currentTime - startTime

              immutableObjectStorage.runForEffectsOnly(
                for {
                  retrievedMap <- immutableObjectStorage
                    .retrieve[AvlMap[Int, AvlSet[String]]](trancheId)
                } yield {
                  println(
                    s"Step: $step, duration: ${duration.toMillis} milliseconds, maximum update duration: ${maximumUpdateDuration.toMillis} milliseconds, contents at previous step: ${retrievedMap.get(step - 1).getOrElse(AvlSet.empty).toScalaSet}"
                  )
                },
                intersessionState
              )(tranches)

              maximumUpdateDuration = FiniteDuration(0L, TimeUnit.MILLISECONDS)
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

              maximumUpdateDuration =
                maximumUpdateDuration max (Deadline.now - updateStartTime)
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

    override protected def isExcludedFromBeingProxied(
        clazz: Class[_]
    ): Boolean = clazz.getName.contains("BTNil")
  }
}
