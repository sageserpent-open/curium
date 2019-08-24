# Curium - **_Storage for your immutable Scala objects_** [![Build Status](https://travis-ci.org/sageserpent-open/curium.svg?branch=master)](https://travis-ci.org/sageserpent-open/curium)

## What ##
You have written some beautiful Scala code that uses pure functional code to work on immutable Scala objects. You wrote tests, they passed first time (really?), the code was easy to refactor and extend thanks to referential transparency, lifting, types that guide your design, macros, Scalacheck, free monads, applicative functors, lenses and all those other shiny things that make Scala a joy to write code in.

Except that when you move from running tests to actually providing an application or service that is going to be deployed, you suddenly remembered that you have to store your data somewhere. Real applications have to park their data on a hard drive or SSD or something; at the very least, they have to be restarted at some point and carry on from where they left off, even if they work on the model of keeping everything in memory to run as fast possible.

Ok, so you look at Hibernate etc, but realise that the Hibernate approach is to map _mutable_ objects to storage - so loading of data is performed by _mutating_ an object in memory. That won't work with your pristine immutable objects.

How about using Java serialization or Kryo or some such like? While Java serialization is slow, Kryo performs well, so problem solved ... except that each serialization of the cosmic application object is going to store an application's worth of data. So if you have a map of one million key-value pairs, creating a new map with one updated or new key-value pair and serializing it will write all of the other pairs to storage again. That won't scale.

Perhaps a database is the way to go - we have ScalikeJDBC, Slick, Quill, Doobie etc. All good, but the onus is on you to go through your application state and unpick into pieces that will fit nicely into a relational database.

Have you tried doing this for a finger tree data structure, for instance? Perhaps the Quiver graph data structure? Better yet, both of them embedded deep within some other data structure that is your application state.

Hmmm...

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

```


## How? ##


