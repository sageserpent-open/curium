package com.sageserpent.curium

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.curium.ImmutableObjectStorage.Session
import com.sageserpent.curium.caffeineBuilder.CaffeineArchetype

import scala.annotation.tailrec
import scala.collection.immutable.{AbstractMap, AbstractSet}
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, Duration}
import scala.util.Random

object ImmutableObjectStorageMeetsMap extends RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId

  private val lookbackLimit = 10000000

  private val batchSize = 100

  def main(args: Array[String]): Unit = {
    tranchesResource
      .use(tranches =>
        IO {
          val immutableObjectStorage =
            configuration.build(tranches)

          val Right(initialTrancheId: TrancheId) = {
            val session: Session[TrancheId] =
              immutableObjectStorage.store(Map.empty[Int, Set[String]])

            immutableObjectStorage
              .runToYieldTrancheId(session)
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
                    .retrieve[Map[Int, Set[String]]](trancheId)
                    .map(_.get(step - 1).getOrElse(Set.empty))
                )

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
                    .sortBy { case (count, duration) => count -> -duration }} ,contents at previous step: $contentsAtPreviousStep"
              )

              maximumUpdateDuration = Duration.Zero
              minimumUpdateDuration = Duration.Inf
              updateDurations.clear()
            }

            val session: Session[TrancheId] = for {
              retrievedMap <- immutableObjectStorage
                .retrieve[Map[Int, Set[String]]](trancheId)
              mutatedMap = iterate {
                (count, originalMap: Map[Int, Set[String]]) =>
                  val microStep = step + count

                  val mapWithPossibleRemoval =
                    if (1 == microStep % 5) {
                      val elementToRemove =
                        microStep - randomBehaviour.chooseAnyNumberFromOneTo(
                          lookbackLimit min microStep
                        )
                      originalMap.removed(elementToRemove)
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
                      .getOrElse(Set.empty)
                    if (!set.isEmpty && 1 == microStep % 11)
                      set.excl(set.min)
                    else set + microStep.toString
                  })
              }(seed = retrievedMap, numberOfIterations = batchSize)

              trimmedMap =
                if (mutatedMap.size > lookbackLimit)
                  mutatedMap.removedAll(
                    step + batchSize - mutatedMap.size until step + batchSize - lookbackLimit
                  )
                else mutatedMap

              newTrancheId <- immutableObjectStorage.store(trimmedMap)
            } yield newTrancheId

            {

              val updateStartTime = Deadline.now

              trancheId = immutableObjectStorage
                .runToYieldTrancheId(session)
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

  object configuration extends ImmutableObjectStorage.Configuration {
    override val tranchesImplementationName: String =
      classOf[RocksDbTranches].getSimpleName

    override val sessionCycleCountWhenStoredTranchesAreNotRecycled: Int =
      0 // Disabled!

    override def canBeProxiedViaSuperTypes(clazz: Class[_]): Boolean =
      // What goes on behind the scenes for the `HashSet` and `HashMap`
      // implementations.
      (clazz.getName contains "BitmapIndexed") || (clazz.getName contains "HashCollision") ||
        (classOf[AbstractSet[_]] isAssignableFrom clazz) || (classOf[
          AbstractMap[_, _]
        ] isAssignableFrom clazz)

    override def trancheCacheCustomisation(
        caffeine: CaffeineArchetype
    ): CaffeineArchetype =
      super
        .trancheCacheCustomisation(caffeine)
        .maximumSize(
          1
        ) // Only need a single tranche as recycling on permanently, and only one object as updated in a session.

  }
}
