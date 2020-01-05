package com.sageserpent.curium

import scala.collection.mutable
import scala.concurrent.duration.Deadline
import scala.util.DynamicVariable

object Timer {
  val cumulativeDurations = mutable.Map.empty[Seq[String], (Int, Long)]

  val categories: DynamicVariable[Seq[String]] = new DynamicVariable(Seq.empty)

  def withCategory[X](category: String)(block: => X): X = {
    categories.withValue(categories.value :+ category)(block)
  }

  def sampleAndPrintResults(prefix: String): Unit = {
    for ((description, (count, cumulativeDuration)) <- cumulativeDurations
      .map {
        case (categories, value) => categories.mkString(">") -> value
      }
      .toSeq
      .sortBy(_._1)) {
      val meanDuration = cumulativeDuration / count

      println(s"$prefix $description $cumulativeDuration $count $meanDuration")
    }

    cumulativeDurations.clear()
  }

  def timed[X](category: String)(block: => X): X = withCategory(category) {
    val startTime = Deadline.now

    try {
      block
    } finally {
      val duration = (Deadline.now - startTime).toMillis
      cumulativeDurations.getOrElse(categories.value, 0 -> 0L) match {
        case (count, cumulativeDuration) =>
          cumulativeDurations(categories.value) = (1 + count) -> (duration + cumulativeDuration)
      }
    }
  }
}