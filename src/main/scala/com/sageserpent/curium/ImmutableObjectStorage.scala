package com.sageserpent.curium

import cats.free.FreeT
import cats.implicits._
import com.sageserpent.curium.caffeineBuilder.CaffeineArchetype

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.util.hashing.MurmurHash3

object ImmutableObjectStorage {
  type TrancheLocalObjectReferenceId = Int
  type CanonicalObjectReferenceId[TrancheId] =
    (TrancheId, TrancheLocalObjectReferenceId)
  type EitherThrowableOr[X] = Either[Throwable, X]
  type Session[X]           = FreeT[Operation, EitherThrowableOr, X]
  val maximumObjectReferenceId: TrancheLocalObjectReferenceId = Int.MaxValue

  trait Tranches[TrancheIdImplementation] {
    type TrancheId = TrancheIdImplementation

    def createTrancheInStorage(
        tranche: TrancheOfData[TrancheId]
    ): TrancheId

    def retrieveTranche(
        trancheId: TrancheId
    ): TrancheOfData[TrancheId]
  }

  case class TrancheOfData[TrancheId](
      payload: Array[Byte],
      interTrancheObjectReferenceIdTranslation: Map[
        TrancheLocalObjectReferenceId,
        CanonicalObjectReferenceId[TrancheId]
      ]
  ) {
    // NOTE: 'WrappedArray' isn't used here as it could require non-trivial
    // runtime conversions if the array type is cast, or needs support to work
    // with Doobie and other things that need an explicit typeclass for it.

    override def equals(another: Any): Boolean = another match {
      case TrancheOfData(
            payload,
            interTrancheObjectReferenceIdTranslation
          ) =>
        this.payload
          .sameElements(
            payload
          ) && this.interTrancheObjectReferenceIdTranslation == interTrancheObjectReferenceIdTranslation
      case _ => false
    }

    override def hashCode(): Int = MurmurHash3.productHash(
      (
        MurmurHash3.bytesHash(payload),
        interTrancheObjectReferenceIdTranslation
      )
    )

    override def toString: String =
      s"TrancheOfData(payload hash: ${MurmurHash3.bytesHash(payload)}, inter-tranche object reference id translation: $interTrancheObjectReferenceIdTranslation)"
  }

  trait Operation[Result]

  trait Configuration {
    def build[TrancheId](
        tranches: Tranches[TrancheId]
    ): ImmutableObjectStorage[TrancheId] =
      new ImmutableObjectStorageImplementation(this, tranches)

    val tranchesImplementationName: String

    val recycleStoredObjectsInSubsequentSessions: Boolean = true

    def isExcludedFromBeingProxied(clazz: Class[_]): Boolean = false

    // NOTE: this is a potential danger area when an override is defined -
    // returning true indicates that all uses of a proxied object can be
    // performed via the supertype and / or interfaces. Obvious examples where
    // this is not true would include a final class that doesn't extend an
    // interface and only has 'AnyRef' as a superclass - how would client code
    // do something useful with it? Scala case classes that are declared as
    // final and form a union type hierarchy that pattern matching is performed
    // on will also fail. The reason why this exists at all is to provide as an
    // escape hatch for the multitude of Scala case classes declared as final
    // that are actually used as part of an object-oriented interface hierarchy
    // - the collection classes being the main offenders in that regard.
    def canBeProxiedViaSuperTypes(clazz: Class[_]): Boolean = false

    def trancheCacheCustomisation(
        caffeine: CaffeineArchetype
    ): CaffeineArchetype = caffeine.softValues()
  }
}

trait ImmutableObjectStorage[TrancheId] {
  import ImmutableObjectStorage._

  case class Store[X](immutableObject: X) extends Operation[TrancheId]

  case class Retrieve[X](trancheId: TrancheId, clazz: Class[X])
      extends Operation[X]

  def clear(): Unit

  def store[X](immutableObject: X): Session[TrancheId] =
    FreeT.liftF[Operation, EitherThrowableOr, TrancheId](Store(immutableObject))

  def retrieve[X: TypeTag](id: TrancheId): Session[X] =
    FreeT.liftF[Operation, EitherThrowableOr, X](
      Retrieve(id, classFromType(typeOf[X]))
    )

  def runToYieldTrancheIds(
      session: Session[Vector[TrancheId]]
  ): EitherThrowableOr[Vector[TrancheId]]

  def runToYieldTrancheId(
      session: Session[TrancheId]
  ): EitherThrowableOr[TrancheId]

  def runForEffectsOnly(
      session: Session[Unit]
  ): EitherThrowableOr[Unit]

  def runToYieldResult[Result](
      session: Session[Result]
  ): EitherThrowableOr[Result]
}
