package com.sageserpent.curium

import cats.effect.IO
import cats.implicits._
import com.sageserpent.curium.ImmutableObjectStorage.{
  IntersessionState,
  Session
}

import scala.concurrent.duration.Deadline

object ImmutableObjectStorageMeetsSet extends H2ViaScalikeJdbcTranchesResource {
  type TrancheId = H2ViaScalikeJdbcTranches#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected def isExcludedFromBeingProxied(
        clazz: Class[_]): Boolean = false

    override protected def canBeProxiedViaSuperTypes(clazz: Class[_]): Boolean =
      !(Nil.getClass.isAssignableFrom(clazz) || Map
        .empty[Any, Nothing]
        .getClass
        .isAssignableFrom(clazz) || Set
        .empty[Any]
        .getClass
        .isAssignableFrom(clazz) || clazz.getName.contains("Empty")) &&
        classOf[Traversable[_]]
          .isAssignableFrom(clazz)

    override protected val tranchesImplementationName: String =
      classOf[H2ViaScalikeJdbcTranches].getSimpleName
  }

  def main(args: Array[String]): Unit = {
    tranchesResource
      .use(tranches =>
        IO {
          val intersessionState = new IntersessionState[TrancheId]

          val Right(initialTrancheId: TrancheId) = {
            val session: Session[TrancheId] =
              immutableObjectStorage.store(Set.empty[Int])

            immutableObjectStorage
              .runToYieldTrancheId(session, intersessionState)(tranches)
          }

          var trancheId: TrancheId = initialTrancheId

          val startTime = Deadline.now

          for (step <- 0 until 10000000) {
            val session: Session[TrancheId] = for {
              set <- immutableObjectStorage.retrieve[Set[Int]](trancheId)
              mutatedSet = (if (0 == step % 2) set - (step / 2) else set) + step
              newTrancheId <- immutableObjectStorage.store(mutatedSet)
            } yield newTrancheId

            trancheId = immutableObjectStorage
              .runToYieldTrancheId(session, intersessionState)(tranches)
              .right
              .get

            if (step % 50 == 0) {
              val currentTime = Deadline.now

              val duration = currentTime - startTime

              println(
                s"Step: $step, duration: ${duration.toMillis} milliseconds")
            }
          }
      })
      .unsafeRunSync()
  }
}
