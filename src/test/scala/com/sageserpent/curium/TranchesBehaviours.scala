package com.sageserpent.curium

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits._
import com.sageserpent.americium.Trials
import com.sageserpent.curium.ImmutableObjectStorage._
import com.sageserpent.curium.ImmutableObjectStorageSpec.FakeTranches
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

trait TranchesResource[TrancheId] {
  val tranchesResource: Resource[IO, Tranches[TrancheId]]
}

object TranchesBehaviours {
  val api = Trials.api

  val payloads: Trials[Array[Byte]] = api.bytes.several[Array[Byte]]
}

trait TranchesBehaviours[TrancheId] extends AnyFlatSpec with Matchers {
  this: TranchesResource[TrancheId] =>

  import TranchesBehaviours._

  val interTrancheObjectReferenceIdTranslations: Trials[
    Map[TrancheLocalObjectReferenceId, CanonicalObjectReferenceId[TrancheId]]
  ] =
    api.integers
      .maps((api.longs, api.integers).mapN(_ -> _))
      .asInstanceOf[Trials[
        Map[TrancheLocalObjectReferenceId, CanonicalObjectReferenceId[
          TrancheId
        ]]
      ]]

  val cases: Trials[List[
    (
        Array[Byte],
        Map[TrancheLocalObjectReferenceId, CanonicalObjectReferenceId[
          TrancheId
        ]]
    )
  ]] =
    (
      payloads,
      interTrancheObjectReferenceIdTranslations
    ).tupled.lists.filter(_.nonEmpty)

  def tranchesBehaviour: Unit = {

    val maximumNumberOfCases = 100

    "creating a tranche" should "yield a unique tranche id" in
      cases.withLimit(maximumNumberOfCases).supplyTo { payloadAndCountPairs =>
        tranchesResource
          .use(tranches =>
            IO {
              val numberOfPayloads = payloadAndCountPairs.size

              val trancheIds = MutableSet.empty[TrancheId]

              for (
                (
                  payload,
                  interTrancheObjectReferenceIdTranslation
                ) <-
                  payloadAndCountPairs
              ) {
                val Right(trancheId) =
                  for {
                    trancheId <- tranches.createTrancheInStorage(
                      TrancheOfData(
                        payload,
                        interTrancheObjectReferenceIdTranslation
                      )
                    )
                  } yield trancheId

                trancheIds += trancheId
              }

              trancheIds should have size numberOfPayloads
            }
          )
          .unsafeRunSync
      }

    "retrieving a tranche by tranche id" should "yield a tranche that corresponds to what was used to create it" in
      cases.withLimit(maximumNumberOfCases).supplyTo { payloadAndCountPairs =>
        tranchesResource
          .use(tranches =>
            IO {
              val trancheIdToExpectedTrancheMapping =
                MutableMap.empty[TrancheId, TrancheOfData[TrancheId]]

              for (
                (
                  payload,
                  interTrancheObjectReferenceIdTranslation
                ) <-
                  payloadAndCountPairs
              ) {
                val Right((trancheId, tranche)) = {
                  val tranche = TrancheOfData(
                    payload,
                    interTrancheObjectReferenceIdTranslation
                  )

                  for {
                    trancheId <- tranches
                      .createTrancheInStorage(tranche)
                  } yield trancheId -> tranche
                }

                trancheIdToExpectedTrancheMapping += (trancheId -> tranche)
              }

              trancheIdToExpectedTrancheMapping.foreach {
                case (trancheId, expectedTranche) =>
                  val Right(tranche) = tranches.retrieveTranche(trancheId)

                  tranche shouldBe expectedTranche
              }
            }
          )
          .unsafeRunSync
      }
  }
}

object FakeTranchesResource {
  type TrancheId = FakeTranches#TrancheId
}

trait FakeTranchesResource
    extends TranchesResource[FakeTranchesResource.TrancheId] {

  override val tranchesResource
      : Resource[IO, Tranches[FakeTranchesResource.TrancheId]] =
    Resource.liftK(IO {
      new FakeTranches
    })
}

class FakeTranchesSpec
    extends TranchesBehaviours[FakeTranchesResource.TrancheId]
    with FakeTranchesResource {
  "Fake tranches" should behave like tranchesBehaviour
}

/* object H2ViaScalikeJdbcTranchesResource { type TrancheId =
 * H2ViaScalikeJdbcTranches#TrancheId }
 *
 * trait H2ViaScalikeJdbcTranchesResource extends
 * TranchesResource[H2ViaScalikeJdbcTranchesResource.TrancheId] with
 * H2ViaScalikeJdbcDatabaseSetupResource { override val tranchesResource :
 * Resource[IO, Tranches[H2ViaScalikeJdbcTranchesResource.TrancheId]] =
 * connectionPoolResource.map( connectionPool => new
 * H2ViaScalikeJdbcTranches(connectionPool) with
 * TranchesContracts[H2ViaScalikeJdbcTranchesResource.TrancheId]) }
 *
 * class H2ViaScalikeJdbcTranchesSpec extends
 * TranchesBehaviours[H2ViaScalikeJdbcTranchesResource.TrancheId] with
 * H2ViaScalikeJdbcTranchesResource { "H2 tranches" should behave like
 * tranchesBehaviour } */
object RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId
}

trait RocksDbTranchesResource
    extends TranchesResource[RocksDbTranchesResource.TrancheId]
    with RocksDbResource {
  override val tranchesResource
      : Resource[IO, Tranches[RocksDbTranchesResource.TrancheId]] = for {
    rocksDb <- rocksDbResource
  } yield new RocksDbTranches(rocksDb)
}

class RocksDbTranchesSpec
    extends TranchesBehaviours[RocksDbTranchesResource.TrancheId]
    with RocksDbTranchesResource {
  "RocksDB tranches" should behave like tranchesBehaviour
}
