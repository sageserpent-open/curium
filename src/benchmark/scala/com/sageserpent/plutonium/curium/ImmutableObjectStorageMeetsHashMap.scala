package com.sageserpent.plutonium.curium

import cats.effect.IO
import cats.implicits._
import com.sageserpent.plutonium.WorldH2StorageImplementation
import com.sageserpent.plutonium.curium.H2ViaScalikeJdbcTranchesResource.TrancheId
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  IntersessionState,
  Session
}

import scala.collection.immutable.HashMap
import scala.concurrent.duration.Deadline

object ImmutableObjectStorageMeetsHashMap
    extends H2ViaScalikeJdbcTranchesResource {
  def main(args: Array[String]): Unit = {
    tranchesResource
      .use(tranches =>
        IO {
          val intersessionState = new IntersessionState[TrancheId]

          val immutableObjectStorage =
            WorldH2StorageImplementation.immutableObjectStorage

          val Right(initialTrancheId: TrancheId) = {
            val session: Session[TrancheId] =
              immutableObjectStorage.store(HashMap.empty[Int, String])

            immutableObjectStorage
              .runToYieldTrancheId(session, intersessionState)(tranches)
          }

          var trancheId: TrancheId = initialTrancheId

          val startTime = Deadline.now

          for (step <- 0 until 1000000) {
            val session: Session[TrancheId] = for {
              map <- immutableObjectStorage.retrieve[HashMap[Int, String]](
                trancheId)
              mutatedHashMap = (if (0 == step % 2) map - (step / 2) else map) + (step -> step.toString)
              newTrancheId <- immutableObjectStorage.store(mutatedHashMap)
            } yield newTrancheId

            trancheId = immutableObjectStorage
              .runToYieldTrancheId(session, intersessionState)(tranches)
              .right
              .get

            if (step % 50 == 0) {
              val currentTime = Deadline.now

              val duration = currentTime - startTime

              println(
                s"Step: $step, duration: ${duration.toMillis} milliseconds, referenceIdToProxyCache: ${intersessionState.referenceIdToProxyCache.estimatedSize}, objectToReferenceIdCache: ${intersessionState.objectToReferenceIdCache.estimatedSize}")
            }
          }
      })
      .unsafeRunSync()
  }
}
