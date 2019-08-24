# Curium - **_Storage for your immutable Scala objects_** [![Build Status](https://travis-ci.org/sageserpent-open/curium.svg?branch=master)](https://travis-ci.org/sageserpent-open/curium)

## Why ##
You have written some beautiful Scala code that uses pure functional code to work on immutable Scala objects. You wrote tests, they passed first time (really?), the code was easy to refactor and extend thanks to referential transparency, lifting, types that guide your design, macros, Scalacheck, free monads, applicative functors, lenses and all those other shiny things that make Scala a joy to write code in.

Except that when you move from running tests to actually providing an application or service that is going to be deployed, you suddenly remembered that you have to store your data somewhere. Real applications have to park their data on a hard drive or SSD or something; at the very least, they have to be restarted at some point and carry on from where they left off, even if they work on the model of keeping everything in memory to run as fast possible.

Ok, so you look at Hibernate etc, but realise that the Hibernate approach is to map _mutable_ objects to storage - so loading of data is performed by _mutating_ an object in memory. That won't work with your pristine immutable objects.

How about using Java serialization or Kryo or some such like? While Java serialization is slow, Kryo performs well, so problem solved ... except that each serialization of the cosmic application object is going to store an application's worth of data. So if you have a map of one million key-value pairs, creating a new map with one updated or new key-value pair and serializing it will write all of the other pairs to storage again. That won't scale.

Perhaps a database is the way to go - we have ScalikeJDBC, Slick, Quill, Doobie etc. All good, but the onus is on you to go through your application state and unpick into pieces that will fit nicely into a relational database.

Have you tried doing this for a finger tree data structure, for instance? Perhaps the Quiver graph data structure? Better yet, both of them embedded deep within some other data structure that is your application state.

Hmmm...

## What ##

Enter _Curium_ - the premise is that you write pure functional code, using an approach of keeping everything in memory via one or a couple of application state objects. These are stored regularly as _tranches_ by Curium, which yields a simple, storable _tranche id_ that allows easy retrieval of the application state when the application restarts.

Curium understands that your immutable objects will use sharing of state between old and new objects, and uses this to distribute the stored state of an object across many tranches. So your storage will indeed scale.

The best thing is that you don't need a schema, macros, a special IDL compiler, aspects, a byte code rewriter or some pesky trait that all your objects need to inherit from to make this work - just give your Scala application state objects to Curium and you'll get back tranche ids to store via your own preferred simple storage mechanism, perhaps in a database, perhaps as an output bookmark in whatever message bus technology you use.

Yes, that includes third party Scala and Java code too.

When your application restarts, it uses the appropriate tranche id(s) to retrieve the relevant application state, and off it goes.

Oh, and it all fits in memory because the application state loads lazily, even though it is immutable.

Interested?

## Where? ##

It is published via Bintray. It is currently not available on JCenter.

#### SBT ####
Add this to your _build.sbt_:

    resolvers += Resolver.bintrayRepo("sageserpent-open", "maven")
    
    libraryDependencies += "com.sageserpent" %% "curium" % "0.1.0"
    
#### Gradle ####
Add this to your _build.gradle_:

    repositories {
	    maven {
		    url  "https://dl.bintray.com/sageserpent-open/maven"
	    }
    }

    dependencies {
        compile 'com.sageserpent:curium_2.12:0.1.0'
    }
    

    
## Show me... ##

```scala
object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
  override protected val tranchesImplementationName: String =
    classOf[H2ViaScalikeJdbcTranches].getSimpleName
}

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
```

## Storage Backends ##

There are two tranches implementations as of version 0.1.0 - both use an H2 database to store tranche data, one via Doobie and the other via ScalikeJDBC, although neither technology is visible from the point of view of the tranches API used by Curium.

The ScalikeJDBC implementation is currently the recommended one to use - class `H2ViaScalikeJdbcTranches`. Take a look at `H2ViaScalikeJdbcSetupResource` to see how to configure an instance of it.

There is also a test double implementation, `FakeTranches`, that is used by `ImmutableObjectStorageSpec` and which serves as a reference implementation.

You are encouraged to provide your own tranches implementation to suit whatever storage technology fits your needs best. Do please use the tranches tests (see `TranchesBehaviours`) to test your implementations, and when they pass, please feel free to raise a pull request if you want to get them added to Curium. 

## Who uses this already? ##

This was broken out of the [Plutonium](https://github.com/sageserpent-open/plutonium) project as a standalone library, as I felt it could be useful in general - this why there is quite a bit of commit history prior to the initial 0.1.0 release; it has been extracted from the Plutonium commit history.

Plutonium drives Curium quite hard, in that it has some fairly gnarly data structures of its own to store, in addition to its use of third party data structures such as the usual lists, tree sets, hash sets, tree maps, hash maps, vectors, the not so usual immutable multiset and finger trees, as well as Quiver.