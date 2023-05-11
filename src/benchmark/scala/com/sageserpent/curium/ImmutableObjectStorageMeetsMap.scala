package com.sageserpent.curium

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.curium.ImmutableObjectStorage.Session
import com.sageserpent.curium.caffeineBuilder.CaffeineArchetype

import scala.annotation.tailrec
import scala.collection.immutable.{AbstractMap, AbstractSet}
import scala.collection.mutable
import scala.util.Random

object ImmutableObjectStorageMeetsMap extends RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId

  private val lookbackLimit = 10000

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

          var maximumUpdateTrancheLoads: Double =
            0

          var minimumUpdateTrancheLoads: Double = Double.MaxValue

          val trancheLoadsSamples: mutable.ListBuffer[Double] =
            mutable.ListBuffer.empty

          for (step <- 0 until (1000000000, batchSize)) {
            if (0 < step && step % 5000 == 0) {
              val postUpdatesTrancheLoads =
                immutableObjectStorage.resetMeanNumberOfTrancheLoadsInASession

              val Right(contentsAtPreviousStep) =
                immutableObjectStorage.runToYieldResult(
                  immutableObjectStorage
                    .retrieve[Map[Int, Set[String]]](trancheId)
                    .map(_.get(step - 1).getOrElse(Set.empty))
                )

              val queryTrancheLoads =
                immutableObjectStorage.resetMeanNumberOfTrancheLoadsInASession - postUpdatesTrancheLoads

              println(
                s"Step: $step, query tranche loads: ${queryTrancheLoads}, minimum update tranche loads: ${minimumUpdateTrancheLoads}, maximum update tranche loads: ${maximumUpdateTrancheLoads}, average update tranche loads: ${trancheLoadsSamples.sum / trancheLoadsSamples.size}, update tranche loads: ${trancheLoadsSamples
                    .groupBy(identity)
                    .toSeq
                    .map { case (duration, group) =>
                      group.size -> duration
                    }
                    .sortBy { case (count, duration) => count -> -duration }} ,contents at previous step: $contentsAtPreviousStep"
              )

              maximumUpdateTrancheLoads = 0L
              minimumUpdateTrancheLoads = Long.MaxValue
              trancheLoadsSamples.clear()
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

              val preUpdateTrancheLoads =
                immutableObjectStorage.resetMeanNumberOfTrancheLoadsInASession

              trancheId = immutableObjectStorage
                .runToYieldTrancheId(session)
                .right
                .get

              val updateTrancheLoads =
                immutableObjectStorage.resetMeanNumberOfTrancheLoadsInASession - preUpdateTrancheLoads

              maximumUpdateTrancheLoads =
                maximumUpdateTrancheLoads max updateTrancheLoads

              minimumUpdateTrancheLoads =
                minimumUpdateTrancheLoads min updateTrancheLoads

              trancheLoadsSamples += updateTrancheLoads
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

    override val recycleStoredObjectsInSubsequentSessions: Boolean = false

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
        .maximumSize(lookbackLimit / batchSize)

  }
}
