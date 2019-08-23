package com.sageserpent.curium

import cats.effect.IO
import cats.implicits._
import com.sageserpent.curium.ImmutableObjectStorage.IntersessionState
import org.scalatest.{FlatSpec, Matchers}

object ExampleSpec {
  type TrancheId = H2ViaScalikeJdbcTranches#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[H2ViaScalikeJdbcTranches].getSimpleName
  }
}

class ExampleSpec
    extends FlatSpec
    with Matchers
    with H2ViaScalikeJdbcTranchesResource {
  import ExampleSpec._

  "storing and retrieving a series immutable objects that share structure" should "work" in {
    // Start by obtaining a tranches object that provides backend storage and retrieval of tranche data.
    // Use the one provided by the testing support - it spins up a temporary H2 database and uses that
    // for its implementation; the database is torn down when the resource is relinquished.
    tranchesResource
      .use(
        tranches =>
          IO {
            // Next we'll have some intersession state, which is a memento object
            // used by the immutable object storage to cache data between sessions.

            val intersessionState = new IntersessionState[TrancheId]

            // Let's begin .. we shall store something and get back a tranche id
            // that we hold on to.
            val Right(firstTrancheId: TrancheId) = {
              // A session in which an initial immutable map with some substructure is stored...
              val session = for {
                trancheId <- immutableObjectStorage
                  .store[Map[Set[String], List[Int]]](
                    Map(Set("Huey, Duey, Louie") -> List(1, 2, 3, -1, -2, -3))
                  )
              } yield trancheId

              // Nothing has actually taken place yet - we have to run the session to
              // make imperative changes, in this case storing the initial map. This
              // gives us back the tranche id.
              immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
            }

            // OK, so let's do some pure functional work on our initial map and
            // store the result. We get back another tranche id that we hold on to.
            val Right(secondTrancheId: TrancheId) = {
              // A session in which we retrieve the initial map, make a bigger one and store it...
              val session = for {
                initialMap <- immutableObjectStorage
                  .retrieve[Map[Set[String], List[Int]]](firstTrancheId)
                biggerMap = initialMap + (Set("Bystander") -> List.empty)
                trancheId <- immutableObjectStorage.store(biggerMap)
              } yield trancheId

              // Again, nothing has actually taken place yet - so run the session, etc.
              immutableObjectStorage
                .runToYieldTrancheId(session, intersessionState)(tranches)
            }

            // Let's verify what was stored.
            {
              // A session in which we retrieve the two objects from the previous sessions
              // and verify that they contain the correct data...
              val session = for {
                initialMap <- immutableObjectStorage
                  .retrieve[Map[Set[String], List[Int]]](firstTrancheId)
                biggerMap <- immutableObjectStorage
                  .retrieve[Map[Set[String], List[Int]]](secondTrancheId)
              } yield {
                initialMap should be(
                  Map(Set("Huey, Duey, Louie") -> List(1, 2, 3, -1, -2, -3))
                )
                biggerMap should be(
                  Map(
                    Set("Huey, Duey, Louie") -> List(1, 2, 3, -1, -2, -3),
                    Set("Bystander") -> List.empty
                  )
                )
              }: Unit

              immutableObjectStorage
                .runForEffectsOnly(session, intersessionState)(tranches)
            }
        }
      )
      .unsafeRunSync()
  }
}
