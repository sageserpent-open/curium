package com.sageserpent.curium

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object ExampleSpec {
  type TrancheId = H2ViaScalikeJdbcTranches#TrancheId

  object configuration extends ImmutableObjectStorage.Configuration {
    override val tranchesImplementationName: String =
      classOf[H2ViaScalikeJdbcTranches].getSimpleName
  }
}

class ExampleSpec
    extends AnyFlatSpec
    with Matchers
    with H2ViaScalikeJdbcTranchesResource {

  import ExampleSpec._

  "storing and retrieving a series of immutable of objects that share structure" should "work" in {
    // Start by obtaining a tranches object that provides backend storage and
    // retrieval of tranche data. Here, we use the one provided by the testing
    // support via a Cats resource - it spins up a temporary H2 database and
    // uses that for its implementation; the database is torn down when the
    // resource is relinquished.
    tranchesResource
      .use(tranches =>
        IO {
          // Build an instance of `ImmutableObjectStorage` from the underlying
          // tranches store.
          val immutableObjectStorage =
            configuration.build(tranches)

          val initialMap =
            Map("Huey" -> List(1, 2, 3, -1, -2, -3))

          // Let's begin .. we shall store something and get back a tranche id
          // that we hold on to. Think of the following block as being a
          // transaction that writes some initial data back to the tranches
          // store.
          val Right(trancheIdForInitialMap: TrancheId) = {
            // A session in which an initial immutable map with some
            // substructure is stored...
            val session = for {
              trancheId <- immutableObjectStorage
                .store[Map[String, List[Int]]](initialMap)
            } yield trancheId

            // Nothing has actually taken place yet - we have to run the session
            // to make imperative changes, in this case storing the initial map.
            // This gives us back the tranche id.
            immutableObjectStorage
              .runToYieldTrancheId(session)
          }

          // OK, so let's do some pure functional work on our initial map and
          // store the result. We get back another tranche id that we hold on
          // to. This defines another transaction that performs an update.
          val Right(trancheIdForBiggerMap: TrancheId) = {
            // A session in which we retrieve the initial map, make a bigger,
            // updated one and store it...
            val session = for {
              initialMap <- immutableObjectStorage
                .retrieve[Map[String, List[Int]]](trancheIdForInitialMap)

              mapWithUpdatedValue = initialMap.updatedWith("Huey")(
                _.map(99 :: _)
              )

              biggerMap = mapWithUpdatedValue.updated("Bystander", List.empty)

              trancheId <- immutableObjectStorage.store(biggerMap)
            } yield trancheId

            // Again, nothing has actually taken place yet - so run the session
            // to yield a new tranche id for the bigger map.
            immutableObjectStorage
              .runToYieldTrancheId(session)
          }

          // Let's query what was stored in a third transaction.
          val Right((retrievedInitialMap, retrievedBiggerMap)) = {
            // A session in which we retrieve the two objects from the previous
            // sessions, using the two tranche ids from above ...
            val session = for {
              initialMap <- immutableObjectStorage
                .retrieve[Map[String, List[Int]]](trancheIdForInitialMap)

              biggerMap <- immutableObjectStorage
                .retrieve[Map[String, List[Int]]](trancheIdForBiggerMap)
            } yield {
              initialMap -> biggerMap
            }

            immutableObjectStorage
              .runToYieldResult(session)
          }

          retrievedInitialMap should be(
            initialMap
          )

          retrievedBiggerMap should be(
            Map(
              // Note the added value of 99 nested inside the list in the map's
              // entry...
              "Huey" -> List(99, 1, 2, 3, -1, -2, -3),
              // ... along with an entirely new entry contained directly within
              // the map.
              "Bystander" -> List.empty
            )
          )
        }
      )
      .unsafeRunSync()
  }
}
