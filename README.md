# Curium - **_Storage for your immutable Scala objects_**

## Why ##

You have written some beautiful Scala that uses pure functional code to work on immutable Scala objects. You wrote
tests, they passed first time (really?), the code was easy to refactor and extend thanks to referential transparency,
lifting, types that guide your design, macros, Scalacheck, free monads, applicative functors, lenses and all those other
shiny things that make Scala a joy to write code in.

Except that when you move from running tests to actually providing an application or service that is going to be
deployed, you suddenly remembered that you have to store your data somewhere. Real applications have to park their data
on a hard drive or SSD or something; at the very least, they have to be restarted at some point and carry on from where
they left off, even if they work on the model of keeping everything in memory to run as fast possible.

Ok, so you look at Hibernate etc, but realise that the Hibernate approach is to map _mutable_ objects to storage - so
loading of data is performed by _mutating_ an object in memory. That won't work with your pristine immutable objects.

How about using Java serialization or Kryo or some such like? While Java serialization is slow, Kryo performs well, so
problem solved ... except that each serialization of the cosmic application object is going to store an application's
worth of data. So if you have a map of one million key-value pairs, creating a new map with one updated or new key-value
pair and serializing it will write all of the other pairs to storage again. That won't scale.

Perhaps a database is the way to go - we have ScalikeJDBC, Slick, Quill, Doobie etc. All good, but the onus is on you to
go through your application state and unpick it into pieces that will fit nicely into a relational database.

Have you tried doing this for a finger tree data structure, for instance? Perhaps the Quiver graph data structure?
Better yet, both of them embedded deep within some other data structure that is your application state.

Hmmm...

## What ##

Enter _Curium_ - the premise is that you write pure functional code, using an approach of keeping everything in memory
via one or a couple of application state objects. These are stored regularly as _tranches_ by Curium, which yields a
simple, storable _tranche id_ that allows easy retrieval of the application state when the application restarts.

Curium understands that your immutable objects will use sharing of state between old and new objects, and uses this to
distribute the stored state of an object across many tranches. So your storage will indeed scale.

The best thing is that you don't need a schema, macros, a special IDL compiler, aspects, a byte code rewriter or some
pesky trait that all your objects need to inherit from to make this work - just give your Scala application state
objects to Curium and you'll get back tranche ids to store via your own preferred simple storage mechanism, perhaps in a
database, perhaps as an output bookmark in whatever message bus technology you use.

Yes, that includes third party Scala and Java code too.

When your application restarts, it uses the appropriate tranche id(s) to retrieve the relevant application state, and
off it goes.

The application state loads lazily, even though it is immutable, so if you have a gigantic data structure and only wish
to deal with a piece of it, then Curium will load only as many objects as your code needs to run.

## Show me... ##

```scala
type TrancheId = H2ViaScalikeJdbcTranches#TrancheId

object configuration extends ImmutableObjectStorage.Configuration {
  override val tranchesImplementationName: String =
    classOf[H2ViaScalikeJdbcTranches].getSimpleName
}
```

...

```scala
{
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
      // ... along with an entirely new entry contained directly within the map.
      "Bystander" -> List.empty
    )
  )
}
```

## Storage Backends ##

There are two tranches implementations - one uses RocksDB and the other H2 via ScalikeJDBC, although neither technology
is visible from the point of view of the tranches API used by Curium.

The tests include Cats resource traits to provide tranches implementation instances. For `RocksDbTranches`, take a look
at `RocksDbTranchesResource` and for `H2ViaScalikeJdbcTranches`, take a look at `H2ViaScalikeJdbcTranchesResource` to
see how to configure instances.

There is also a test double implementation, `FakeTranches`, that is used by `ImmutableObjectStorageSpec` and which
serves as a reference implementation.

You are encouraged to provide your own tranches implementation to suit whatever storage technology fits your needs best.
Do please use the tranches tests (see `TranchesBehaviours`) to test your implementations, and when they pass, please
feel free to raise a pull request if you want to get them added to Curium.

## Rhetorical Questions ##

### How did this come about? ###

This was broken out of the [Plutonium](https://github.com/sageserpent-open/plutonium) project as a standalone library,
as I felt it could be useful in general.

### Does this really work with real code? ###

Plutonium drives Curium quite hard, in that it has some fairly gnarly data structures of its own to store, in addition
to its use of third party data structures such as the usual lists, tree sets, hash sets, tree maps, hash maps, vectors,
the not so usual immutable multiset and finger trees, as well as the Quiver immutable graph data structure. Several
obscure bugs in the Curium code were unearthed by running Plutonium's exhaustive Scalacheck tests; Curium's own
Scalacheck tests have been bolstered retrospectively to reproduce these bugs, which are fixed in version 0.1.0.

As usual, _caveat emptor_ - but the expectation is that any bugs you encounter should be quite obscure. Please
contribute a bug report with a bug reproduction test if you encounter one.

### What happens when my data structures evolve? ###

Curium uses Kryo 5.3.0 in its implementation, so the guarantees that Kryo makes regarding code change backward and
forward compatibility are what you get with Curium. Please consult the Kryo documentation for this, there is some
support.

### Is this stable? ###

__NO.__

1. There are outstanding performance issues to address, see
   here: [Dogfood](https://github.com/sageserpent-open/curium/issues/2). It _might_ work for your purposes, but I'd be
   careful benchmarking it first.
2. This is not, nor will it ever be your primary data storage technology for your application. For one thing, using a
   persistence approach via Kryo means that the stored tranches aren't readable in the same way that tables in a
   relational database would be. Having said that, you can just pull in tranche data into some arbitrary process that
   has the correct client code on the classpath, so it is theoretically possible to browse stored data without modifying
   the 'primary' application.
3. There are caveats galore mentioned below about what can and can't be proxied, and supported tweaks to workaround
   recalcitrant classes. Don't expect to just drop Curium in to your codebase without doing a lot of benchmarking and
   experimental configuration first. This should probably be bundled up into a default configuration in the future.
4. The Kryo configuration is not visible to client code - but if we want to support data model upgrades and reuse
   existing tranches, then we should probably open this up.

### Is there any code out there that *doesn't* work with Curium? ###

Not as far as I know, but Curium builds proxies to allow lazy loading of parts of the application state across tranches;
so if a class is say, final (`RedBlackTree`, I'm looking at you), its instances can't be proxied.

Curium can be configured to build proxies whose nominal type is that of an interface that a final class implements on a
case-by-case basis - provided the code using instances of the final class only uses the interface, this will work
nicely. An example of this can be seen
in [WorldH2StorageImplementation](https://github.com/sageserpent-open/plutonium/blob/b648dac313d3288232ac2c973a8b8d75f5469452/src/main/scala/com/sageserpent/plutonium/WorldH2StorageImplementation.scala)
, look at the code in the standalone object `immutableObjectStorage`. Nevertheless, you are in the danger zone when you
configure in proxies over interfaces.

The fallback behaviour when an object can't be proxied is to simply to use the object itself, which means that the
object will be stored locally in the same tranche as whatever object references it - so no structure sharing _across_
tranches in that particular case.

### What about object identity? Will the object graph be the same when a tranche is retrieved to what is was when the tranche was stored? ###

No, because in general, the two graphs will refer to different objects in different locations in memory, as often as not
in different processes. Furthermore, the retrieved tranche object graph will contain lots of proxies that in turn
delegate to the 'real' objects.

The point is that due to the proxies delegating en-masse to the underlying objects and referential transparency, your
pure functional code shouldn't be aware of the difference. Having said that, Curium will do its best to reproduce
structure sharing for state either within the same tranche or across tranches, it has to do this to be a scalable
storage solution. Hash maps and lists will work fine. Other things may need some special configuration, as mentioned
above.

### How does a tranches implementation relate to storage of tranche ids? ###

It doesn't. The idea is that a tranches implementation stores the heavyweight tranche data for all of a process' stored
tranches, making that data available again when the process restarts. Tranche ids on the other hand are lightweight
tokens that tell the tranches implementation what tranche data to retrieve. Tranche ids are stored independently of the
tranche data, and it is expected that completely different technologies would be used to do this; for example H2 or
Phoenix / HBase for a tranches implementation, but a Kafka table for tranche ids. The latter allows a restarted process
to quickly ascertain the tranche id of the last successfully stored tranche and to resurrect the application state based
on that.

### What does a tranches implementation need to provide? ###

Not much. Tranches are modelled as a case class containing a byte array with some extra bookkeeping data, and they are
associated with a tranche id, which is a unique identifier for the tranche. The choice of identifier type is up to the
tranches implementation, the H2 implementations use a long integer that maps to an H2 IDENTITY type. So it's a case of
storing a tranche object's byte array and some other simple data offline associated with a tranche id, and fetching them
back with a tranche id. The tranches implementation is also responsible for automatically allocating new tranche ids.

Take a look at `FakeTranches` and `TranchesContracts` for guidance.

### Kryo can only store up to `Integer.MAX_VALUE` objects at a time, so how can this scale? ###

True, but there is a notion of canonical object identity that subsumes Kryo's object identity; the canonical object
identity spans across tranches - this allows the number of objects stored over a sequence of tranches to comfortably
exceed `Integer.MAX_VALUE`. As long as the client code doesn't try to store more than that many new objects into a
single tranche at a time, then you're good.
