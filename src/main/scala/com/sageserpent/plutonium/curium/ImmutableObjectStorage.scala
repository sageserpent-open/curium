package com.sageserpent.plutonium.curium
import java.util.UUID

import cats.Monad
import cats.data.EitherT
import com.sageserpent.plutonium.classFromType
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.Id
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Try => _, _}
import scala.util.Try

object ImmutableObjectStorage {
  type Id = UUID
}

trait ImmutableObjectStorage[F[_]] {
  // TODO - double-check that this is needed here. I'm presuming that 'store' and 'retrieve' will indeed be implemented to use a for-comprehension....
  implicit val monadEvidence: Monad[F]

  // Imperative...
  def store[X: TypeTag](
      immutableObject: X): EitherT[F, Throwable, ImmutableObjectStorage.Id]

  // Imperative...
  def retrieve[X: TypeTag](
      id: ImmutableObjectStorage.Id): EitherT[F, Throwable, X]
}

abstract class ImmutableObjectStorageImplementation[F[_]](
    override implicit val monadEvidence: Monad[F])
    extends ImmutableObjectStorage[F] {
  this: Tranches[F] =>

  val kryoPool: KryoPool = ScalaKryoInstantiator.defaultPool

  override def store[X: universe.TypeTag](
      immutableObject: X): EitherT[F, Throwable, Id] = {
    val serializedRepresentation: Array[Byte] =
      kryoPool.toBytesWithClass(immutableObject)

    storeTranche(TrancheOfData(serializedRepresentation))
  }

  override def retrieve[X: universe.TypeTag](
      id: Id): EitherT[F, Throwable, X] = {
    val clazz: Class[X] = classFromType(typeOf[X])
    for {
      tranche <- retrieveTranche(id)
      result <- EitherT.fromEither[F](Try {
        val deserialized = kryoPool.fromBytes(tranche.serializedRepresentation)
        clazz.cast(deserialized)
      }.toEither)
    } yield result
  }
}

object TrancheOfData {
  // DO WE NEED THIS YET?
  type ObjectReferenceId = Int
  // TODO - need to be able to create a tranche from an object.
}

case class TrancheOfData(serializedRepresentation: Array[Byte])

trait Tranches[F[_]] {
  // Imperative...
  def retrieveTranche(
      id: ImmutableObjectStorage.Id): EitherT[F, Throwable, TrancheOfData]

  // Imperative...
  def storeTranche(
      tranche: TrancheOfData): EitherT[F, Throwable, ImmutableObjectStorage.Id]
}
