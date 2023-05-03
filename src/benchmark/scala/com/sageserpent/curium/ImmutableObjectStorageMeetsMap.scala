package com.sageserpent.curium

import com.sageserpent.americium.randomEnrichment._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, Duration}
import scala.util.Random

object ImmutableObjectStorageMeetsMap {

  def main(args: Array[String]): Unit = {
    val randomBehaviour = new Random(53278953)

    var theMap: Map[Int, Set[String]] = Map.empty

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

        val contentsAtPreviousStep =
          theMap.get(step - 1).getOrElse(Set.empty)

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

      {

        val updateStartTime = Deadline.now

        {
          val mutatedMap = iterate {
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
          }(seed = theMap, numberOfIterations = batchSize)

          val trimmedMap =
            if (mutatedMap.size > lookbackLimit)
              mutatedMap.removedAll(
                step + batchSize - mutatedMap.size until step + batchSize - lookbackLimit
              )
            else mutatedMap

          theMap = trimmedMap
        }

        val updateDuration = Deadline.now - updateStartTime

        maximumUpdateDuration = maximumUpdateDuration max updateDuration

        minimumUpdateDuration = minimumUpdateDuration min updateDuration

        updateDurations += updateDuration
      }
    }

  }

  def iterate[X](step: (Int, X) => X)(seed: X, numberOfIterations: Int): X = {
    require(0 <= numberOfIterations)

    @tailrec
    def evaluate(seed: X, count: Int): X =
      if (numberOfIterations > count) evaluate(step(count, seed), 1 + count)
      else seed

    evaluate(seed, 0)
  }
}
