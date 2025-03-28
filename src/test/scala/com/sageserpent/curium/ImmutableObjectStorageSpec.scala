package com.sageserpent.curium

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.free.FreeT
import cats.implicits._
import com.sageserpent.americium.Trials
import com.sageserpent.americium.java.CasesLimitStrategy
import com.sageserpent.curium.ImmutableObjectStorage._
import com.sageserpent.curium.caffeineBuilder.CaffeineArchetype
import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.UUID
import scala.collection.immutable.{AbstractMap, AbstractSet, HashMap}
import scala.collection.mutable.{Map => MutableMap}
import scala.util.{Random, Try}

object ImmutableObjectStorageSpec {
  type TrancheId      = FakeTranches#TrancheId
  type PartGrowthStep = Vector[Part] => Part
  val aThing = "Foo"
  val labelStrings: Set[String] = {
    // NOTE: the length of the names is significant - Kryo makes a real effort
    // to compress data, and this can lead to a legitimate case where storing a
    // tranche in isolation can result in a payload that is more compressed than
    // the corresponding one sharing structure with a previously stored tranche.
    // This only happens when the objects being stored contain little data whose
    // values are small integers, or short ASCII strings, so we defeat that
    // confusing loophole here to avoid confusing the tests.
    Set("Huey Green", "Duey Dean", "Louie Keene")
  }
  val api = Trials.api

  def partGrowthLeadingToRootForkTrials(
      allowDuplicates: Boolean
  ): Trials[PartGrowth] =
    for {
      numberOfLeavesRequired <- api.integers(
        lowerBound = 1,
        upperBound = 300,
        shrinkageTarget = 1
      )
      result <- partGrowthLeadingToRootFork(
        allowDuplicates,
        numberOfLeavesRequired
      )
    } yield result

  def partGrowthLeadingToRootFork(
      allowDuplicates: Boolean,
      numberOfLeavesRequired: Int
  ): Trials[PartGrowth] = {
    require(0 < numberOfLeavesRequired)

    def growthSteps(
        partIdSetsCoveredBySubparts: Vector[Set[TrancheLocalObjectReferenceId]],
        numberOfLeaves: TrancheLocalObjectReferenceId
    ): Trials[LazyList[PartGrowthStep]] = {
      val numberOfSubparts = partIdSetsCoveredBySubparts.size
      val partId =
        numberOfSubparts // Makes it easier to read the test cases when debugging; the ids label the growth steps in ascending order.
      val collectingStrandsTogetherAsHaveEnoughLeaves =
        numberOfLeaves == numberOfLeavesRequired

      require(
        numberOfLeaves < numberOfLeavesRequired ||
          0 < numberOfSubparts && numberOfLeaves == numberOfLeavesRequired
      )

      for {
        chooseALeaf <- api.booleans.map(coinFlip =>
          numberOfLeaves < numberOfLeavesRequired && (0 == numberOfSubparts || coinFlip)
        )

        label <- api.choose(labelStrings)

        result <-
          if (chooseALeaf) {
            def leaf(subparts: Vector[Part]): Part = {
              require(numberOfSubparts == subparts.size)

              Leaf(numberOfSubparts, label)
            }

            growthSteps(
              partIdSetsCoveredBySubparts :+ Set(partId),
              1 + numberOfLeaves
            ).map(leaf _ #:: _)
          } else
            api.booleans.flatMap(anotherCoinFlip =>
              if (allowDuplicates && 0 < numberOfSubparts && anotherCoinFlip)
                growthSteps(
                  partIdSetsCoveredBySubparts :+ partIdSetsCoveredBySubparts.last,
                  numberOfLeaves
                ).map(((_: Vector[Part]).last) #:: _)
              else
                for {
                  indexOfLeftSubpart <-
                    if (collectingStrandsTogetherAsHaveEnoughLeaves)
                      api.only(numberOfSubparts - 1)
                    else
                      api.integers(
                        lowerBound = 0,
                        upperBound = numberOfSubparts - 1,
                        0
                      )

                  indexOfRightSubpart <-
                    if (collectingStrandsTogetherAsHaveEnoughLeaves) {
                      val partIdsCoveredByLeftSubpart =
                        partIdSetsCoveredBySubparts.last
                      val indicesOfPartsNotCoveredByLeftSubpart =
                        0 until numberOfSubparts filterNot { index =>
                          val partIdsCoveredByIndex =
                            partIdSetsCoveredBySubparts(index)
                          partIdsCoveredByIndex.subsetOf(
                            partIdsCoveredByLeftSubpart
                          )
                        }
                      if (indicesOfPartsNotCoveredByLeftSubpart.nonEmpty)
                        api.only(
                          indicesOfPartsNotCoveredByLeftSubpart.maxBy(index =>
                            partIdSetsCoveredBySubparts(index).size
                          )
                        )
                      else
                        api.integers(
                          lowerBound = 0,
                          upperBound = numberOfSubparts - 1,
                          0
                        )
                    } else
                      api.integers(
                        lowerBound = 0,
                        upperBound = numberOfSubparts - 1,
                        0
                      )

                  partIdsCoveredByThisFork: Set[TrancheLocalObjectReferenceId] =
                    partIdSetsCoveredBySubparts(indexOfLeftSubpart) ++ Set(
                      partId
                    ) ++ partIdSetsCoveredBySubparts(indexOfRightSubpart)

                  allSubpartsIncludedInThisFork =
                    partIdSetsCoveredBySubparts.forall(
                      _.subsetOf(partIdsCoveredByThisFork)
                    )

                  thisCouldBeTheLastStep = allSubpartsIncludedInThisFork &&
                    collectingStrandsTogetherAsHaveEnoughLeaves

                  subResult <- {
                    def fork(subparts: Vector[Part]): Fork = {
                      require(numberOfSubparts == subparts.size)

                      Fork(
                        subparts(indexOfLeftSubpart),
                        partId,
                        subparts(indexOfRightSubpart),
                        label
                      )
                    }

                    api.booleans
                      .flatMap(coinFlip =>
                        if (
                          thisCouldBeTheLastStep &&
                          coinFlip
                        ) api.only(LazyList.empty)
                        else
                          growthSteps(
                            partIdSetsCoveredBySubparts :+ partIdsCoveredByThisFork,
                            numberOfLeaves
                          )
                      )
                      .map(fork _ #:: _)
                  }
                } yield subResult
            )
      } yield result
    }

    growthSteps(Vector.empty, numberOfLeaves = 0)
      .map(_.force)
      .flatMap(steps => PartGrowth(steps))
  }

  sealed trait Part {
    def useProblematicClosure: String
  }

  class FakeTranches extends Tranches[UUID] {
    val tranchesById: MutableMap[TrancheId, TrancheOfData[TrancheId]] =
      MutableMap.empty

    def clear(): Unit = {
      tranchesById.clear()
    }

    def purgeTranche(trancheId: TrancheId): Unit = {
      tranchesById -= trancheId
    }

    override def createTrancheInStorage(
        tranche: TrancheOfData[TrancheId]
    ): TrancheId = {
      val trancheId = UUID.randomUUID()

      tranchesById(trancheId) = tranche

      trancheId
    }

    override def retrieveTranche(
        trancheId: TrancheId
    ): TrancheOfData[TrancheId] = tranchesById(trancheId)
  }

  case class Leaf(id: Int, labelString: String) extends Part {
    val problematicClosure: Any => String = (_: Any) => aThing + aThing
    val referenceToStandalone             = theOneAndOnlyStandaloneObjectExample
    val referenceToStandaloneButInANonStaticContext =
      standaloneButInANonStaticContext

    override def useProblematicClosure: String = problematicClosure()

    object standaloneButInANonStaticContext {
      def referenceContext = id
    }
  }

  case class Fork(left: Part, id: Int, right: Part, labelString: String)
      extends Part {
    val problematicClosure: Any => String = (_: Any) => s"$aThing"

    override def useProblematicClosure: String = problematicClosure()
  }

  case class PartGrowth(steps: Seq[PartGrowthStep], chunkSizes: Seq[Int]) {
    require(steps.nonEmpty)
    require(steps.size == chunkSizes.sum)

    override def toString(): String = {
      val partsInChunks = thingsInChunks(chunkSizes, parts())

      partsInChunks
        .map(chunk => s"[${chunk.mkString(", ")} ]")
        .mkString(", ")
    }

    def parts(): Vector[Part] =
      steps.foldLeft(Vector.empty[Part]) { case (parts, partGrowthStep) =>
        parts :+ partGrowthStep(parts)
      }

    def storeViaMultipleSessions(
        immutableObjectStorage: ImmutableObjectStorage[TrancheId]
    ): Vector[TrancheId] = {
      val chunks: Seq[Vector[PartGrowthStep]] =
        thingsInChunks(chunkSizes, steps)
          .map(_.toVector)

      var trancheIdsSoFar: Vector[TrancheId] = Vector.empty

      for (partGrowthSteps <- chunks) {
        val retrievalAndStorageSession: Session[Vector[TrancheId]] = for {
          retrievedPartsFromPreviousSessions <- trancheIdsSoFar.traverse(
            immutableObjectStorage.retrieve[Part]
          )
          newPartsFromThisSession: Vector[Part] =
            (retrievedPartsFromPreviousSessions /: partGrowthSteps) {
              case (existingSubparts, partGrowthStep) =>
                existingSubparts :+ partGrowthStep(existingSubparts)
            } drop retrievedPartsFromPreviousSessions.size
          trancheIds <- newPartsFromThisSession.traverse(
            immutableObjectStorage.store
          )
        } yield trancheIds

        val Right(trancheIdsForChunk) =
          immutableObjectStorage
            .runToYieldTrancheIds(retrievalAndStorageSession)

        trancheIdsSoFar ++= trancheIdsForChunk
      }

      trancheIdsSoFar
    }

    private def thingsInChunks[Thing](
        chunkSizes: Seq[Int],
        things: Seq[Thing]
    ): Seq[Seq[Thing]] =
      if (chunkSizes.nonEmpty) {
        val (leadingChunk, remainder) = things.splitAt(chunkSizes.head)

        leadingChunk +: thingsInChunks(chunkSizes.tail, remainder)
      } else Seq.empty
  }

  case object theOneAndOnlyStandaloneObjectExample

  def configuration(
      _sessionCycleCountWhenStoredTranchesAreNotRecycled: Int
  ): ImmutableObjectStorage.Configuration =
    new ImmutableObjectStorage.Configuration {
      override val tranchesImplementationName: String =
        classOf[FakeTranches].getSimpleName

      override val sessionCycleCountWhenStoredTranchesAreNotRecycled
          : TrancheLocalObjectReferenceId =
        _sessionCycleCountWhenStoredTranchesAreNotRecycled
    }

  def configurationThatForgetsItsTranches(
      _sessionCycleCountWhenStoredTranchesAreNotRecycled: Int
  ): ImmutableObjectStorage.Configuration =
    new ImmutableObjectStorage.Configuration {
      override val tranchesImplementationName: String =
        classOf[FakeTranches].getSimpleName

      override val sessionCycleCountWhenStoredTranchesAreNotRecycled
          : TrancheLocalObjectReferenceId =
        _sessionCycleCountWhenStoredTranchesAreNotRecycled

      override def trancheCacheCustomisation(
          caffeine: CaffeineArchetype
      ): CaffeineArchetype =
        super.trancheCacheCustomisation(caffeine).maximumSize(0)
    }

  def configurationForSetsAndMaps(
      _sessionCycleCountWhenStoredTranchesAreNotRecycled: Int
  ): ImmutableObjectStorage.Configuration =
    new ImmutableObjectStorage.Configuration {
      override val tranchesImplementationName: String =
        s"${classOf[FakeTranches].getSimpleName}_specialised_for_sets_and_maps"

      override val sessionCycleCountWhenStoredTranchesAreNotRecycled
          : TrancheLocalObjectReferenceId =
        _sessionCycleCountWhenStoredTranchesAreNotRecycled

      override def canBeProxiedViaSuperTypes(clazz: Class[_]): Boolean =
        // What goes on behind the scenes for the `HashSet` and `HashMap`
        // implementations.
        (clazz.getName contains "BitmapIndexed") || (clazz.getName contains "HashCollision") ||
          (classOf[AbstractSet[_]] isAssignableFrom clazz) || (classOf[
            AbstractMap[_, _]
          ] isAssignableFrom clazz)
    }

  case object alien

  object TrialsHelpers {
    def sequencesOfDistinctIntegersFromZeroToOneLessThan(
        exclusiveLimit: Int
    ): Trials[LazyList[Int]] = {
      // NOTE: need that mapping of `distinct` as the lazy lists yielded by
      // `several` can (and will because it does not terminate) contain
      // duplicates even though the base choices are unique.
      api
        .choose(0 until exclusiveLimit)
        .several[LazyList[Int]]
        .map(_.distinct)
        .map(_.take(exclusiveLimit))
    }
  }

  object PartGrowth {

    def apply(steps: Seq[PartGrowthStep]): Trials[PartGrowth] = for {
      chunkEndIndices <- {
        val numberOfSteps            = steps.size
        val oneLessThanNumberOfSteps = numberOfSteps - 1
        if (0 < oneLessThanNumberOfSteps)
          api
            .integers(0, oneLessThanNumberOfSteps - 1)
            .flatMap(numberOfIndices =>
              TrialsHelpers
                .sequencesOfDistinctIntegersFromZeroToOneLessThan(
                  oneLessThanNumberOfSteps
                )
                .map { sequence =>
                  val innerIndices = sequence
                    .take(numberOfIndices)
                    .map(1 + _)
                    .sorted

                  0 +: innerIndices :+ numberOfSteps
                }
            )
        else api.only(0 to numberOfSteps)
      }

      chunkSizes = chunkEndIndices.zip(chunkEndIndices.tail).map {
        case (lower, higher) => higher - lower
      }
    } yield PartGrowth(steps, chunkSizes)
  }

  private val sessionCycleCounts = api.integers(0, 10)

  private val testCycleDuration: Duration = Duration.ofSeconds(30)
}

class ImmutableObjectStorageSpec extends AnyFlatSpec with Matchers {

  import ImmutableObjectStorageSpec._

  private val complexityLimit = 10000

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          trancheIds should contain theSameElementsAs trancheIds.toSet

          tranches.tranchesById.keys should contain theSameElementsAs trancheIds
      }

  private val maximumNumberOfPermutationsToChooseFrom = 200

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(
        api
          .integers(0, maximumNumberOfPermutationsToChooseFrom - 1, 0)
          .map(_.toDouble / maximumNumberOfPermutationsToChooseFrom)
      )
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (
            partGrowth,
            permutationScale,
            sessionCycleCountWhenStoredTranchesAreNotRecycled
        ) =>
          val expectedParts = partGrowth.parts()

          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          val permutedIndicesCandidates = Vector
            .tabulate(trancheIds.size)(identity)
            .permutations
            .take(maximumNumberOfPermutationsToChooseFrom)
            .toSeq

          val forwardPermutation: Map[Int, Int] = permutedIndicesCandidates(
            (permutationScale * permutedIndicesCandidates.size).toInt
          ).zipWithIndex.toMap

          val backwardsPermutation = forwardPermutation.map(_.swap)

          // NOTE: as long as we have a complete chain of tranches, it shouldn't
          // matter in what order tranche ids are submitted for retrieval.
          val permutedTrancheIds = Vector(
            trancheIds.indices map (index =>
              trancheIds(forwardPermutation(index))
            ): _*
          )

          val retrievalSession: Session[Unit] =
            for {
              permutedRetrievedParts <- permutedTrancheIds.traverse(
                immutableObjectStorage.retrieve[Part]
              )

              retrievedPartsInOriginalOrder =
                permutedRetrievedParts.indices map (index =>
                  permutedRetrievedParts(backwardsPermutation(index))
                )
            } yield {
              // Verify test expectations *inside* the session; we are testing
              // use
              // of the retrieved parts as part of the session without letting
              // them escape.
              retrievedPartsInOriginalOrder should contain theSameElementsInOrderAs expectedParts

              retrievedPartsInOriginalOrder.map(
                _.useProblematicClosure
              ) should equal(
                expectedParts.map(_.useProblematicClosure)
              )

              Inspectors.forAll(retrievedPartsInOriginalOrder)(retrievedPart =>
                Inspectors.forAll(expectedParts)(expectedPart =>
                  retrievedPart should not be theSameInstanceAs(expectedPart)
                )
              )
            }

          val result =
            immutableObjectStorage.runForEffectsOnly(retrievalSession)

          result shouldBe a[Right[_, _]]
      }

  it should "yield an object that is equal to what was stored - with a twist" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(
        api
          .integers(0, maximumNumberOfPermutationsToChooseFrom - 1, 0)
          .map(_.toDouble / maximumNumberOfPermutationsToChooseFrom)
      )
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (
            partGrowth,
            permutationScale,
            sessionCycleCountWhenStoredTranchesAreNotRecycled
        ) =>
          val expectedParts = partGrowth.parts()

          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          val permutedIndicesCandidates = Vector
            .tabulate(trancheIds.size)(identity)
            .permutations
            .take(maximumNumberOfPermutationsToChooseFrom)
            .toSeq

          val forwardPermutation: Map[Int, Int] = permutedIndicesCandidates(
            (permutationScale * permutedIndicesCandidates.size).toInt
          ).zipWithIndex.toMap

          val backwardsPermutation = forwardPermutation.map(_.swap)

          // NOTE: as long as we have a complete chain of tranches, it shouldn't
          // matter in what order tranche ids are submitted for retrieval.
          val permutedTrancheIds = Vector(
            trancheIds.indices map (index =>
              trancheIds(forwardPermutation(index))
            ): _*
          )

          val retrievalSession: Session[IndexedSeq[Part]] =
            for {
              permutedRetrievedParts <- permutedTrancheIds.traverse(
                immutableObjectStorage.retrieve[Part]
              )

            } yield permutedRetrievedParts.indices map (index =>
              permutedRetrievedParts(backwardsPermutation(index))
            )

          // Let the retrieved parts escape the session...
          val Right(retrievedParts) =
            immutableObjectStorage.runToYieldResult(retrievalSession)

          // ... here's the twist - pull the rug out from under
          // `retrievedParts`.
          // This simulates client code holding on to and then using items that
          // escape a session, when the machinery backing any proxied references
          // has been torn down in the meantime.
          immutableObjectStorage.clear()
          tranches.clear()

          retrievedParts should contain theSameElementsInOrderAs expectedParts

          retrievedParts.map(_.useProblematicClosure) should equal(
            expectedParts.map(_.useProblematicClosure)
          )

          Inspectors.forAll(retrievedParts)(retrievedPart =>
            Inspectors.forAll(expectedParts)(expectedPart =>
              retrievedPart should not be theSameInstanceAs(expectedPart)
            )
          )
      }

  it should "not exhibit a bug when a case class's copy method is called" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(
        api
          .integers(0, maximumNumberOfPermutationsToChooseFrom - 1, 0)
          .map(_.toDouble / maximumNumberOfPermutationsToChooseFrom)
      )
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (
            partGrowth,
            permutationScale,
            sessionCycleCountWhenStoredTranchesAreNotRecycled
        ) =>
          val expectedParts = partGrowth.parts()

          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          val permutedIndicesCandidates = Vector
            .tabulate(trancheIds.size)(identity)
            .permutations
            .take(maximumNumberOfPermutationsToChooseFrom)
            .toSeq

          val forwardPermutation: Map[Int, Int] = permutedIndicesCandidates(
            (permutationScale * permutedIndicesCandidates.size).toInt
          ).zipWithIndex.toMap

          val backwardsPermutation = forwardPermutation.map(_.swap)

          // NOTE: as long as we have a complete chain of tranches, it shouldn't
          // matter in what order tranche ids are submitted for retrieval.
          val permutedTrancheIds = Vector(
            trancheIds.indices map (index =>
              trancheIds(forwardPermutation(index))
            ): _*
          )

          val retrievalSession: Session[IndexedSeq[Part]] =
            for {
              permutedRetrievedParts <- permutedTrancheIds.traverse(
                immutableObjectStorage.retrieve[Part]
              )

            } yield permutedRetrievedParts.indices map (index =>
              permutedRetrievedParts(backwardsPermutation(index))
            ) map {
              // Make copies inside the session - this should be supported.
              case leaf: Leaf => leaf.copy()
              case fork: Fork => fork.copy()
            }

          // Let the retrieved parts escape the session...
          val Right(retrievedCopiedParts) =
            immutableObjectStorage.runToYieldResult(retrievalSession)

          retrievedCopiedParts should contain theSameElementsInOrderAs expectedParts

          retrievedCopiedParts.map(_.useProblematicClosure) should equal(
            expectedParts.map(_.useProblematicClosure)
          )

          Inspectors.forAll(retrievedCopiedParts)(retrievedPart =>
            Inspectors.forAll(expectedParts)(expectedPart =>
              retrievedPart should not be theSameInstanceAs(expectedPart)
            )
          )
      }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          val referenceParts = partGrowth.parts()

          val referencePartsByTrancheId = (trancheIds zip referenceParts).toMap

          for (sampleTrancheId <- trancheIds) {
            val samplingSession: Session[Unit] = for {
              _ <- (referencePartsByTrancheId(sampleTrancheId) match {
                case _: Fork =>
                  immutableObjectStorage.retrieve[Leaf](sampleTrancheId)
                case _: Leaf =>
                  immutableObjectStorage.retrieve[Fork](sampleTrancheId)
              }) flatMap (part =>
                FreeT.liftT(Try {
                  part.hashCode
                }.toEither)
              ) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
            } yield ()

            immutableObjectStorage
              .runForEffectsOnly(samplingSession) shouldBe a[Left[_, _]]
          }
      }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = false)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          for (idOfCorruptedTranche <- trancheIds) {
            tranches.tranchesById(idOfCorruptedTranche) = {
              val trancheToCorrupt = tranches.tranchesById(idOfCorruptedTranche)
              trancheToCorrupt.copy(
                payload = "*** CORRUPTION! ***"
                  .map(_.toByte)
                  .toArray ++ trancheToCorrupt.payload
              )
            }

            val rootTrancheId = trancheIds.last

            val freshImmutableObjectStorage =
              configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
                .build(tranches)

            val samplingSessionWithCorruptedTranche: Session[Unit] = for {
              _ <- freshImmutableObjectStorage.retrieve[Fork](
                rootTrancheId
              ) flatMap (part =>
                FreeT.liftT(Try {
                  part.hashCode
                }.toEither)
              ) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
            } yield ()

            freshImmutableObjectStorage.runForEffectsOnly(
              samplingSessionWithCorruptedTranche
            ) shouldBe a[Left[_, _]]
          }
      }

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = false)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          for (idOfMissingTranche <- trancheIds) {
            tranches.purgeTranche(idOfMissingTranche)

            val rootTrancheId = trancheIds.last

            val freshImmutableObjectStorage =
              configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
                .build(tranches)

            val samplingSessionWithMissingTranche: Session[Unit] = for {
              _ <- freshImmutableObjectStorage.retrieve[Fork](
                rootTrancheId
              ) flatMap (part =>
                FreeT.liftT(Try {
                  part.hashCode
                }.toEither)
              ) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
            } yield ()

            freshImmutableObjectStorage.runForEffectsOnly(
              samplingSessionWithMissingTranche
            ) shouldBe a[Left[_, _]]
          }
      }

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = false)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val Right(alienTrancheId) =
            immutableObjectStorage.runToYieldTrancheId(
              immutableObjectStorage.store(alien)
            )

          val nonAlienTrancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          for (idOfIncorrectlyTypedTranche <- nonAlienTrancheIds) {
            tranches.tranchesById(idOfIncorrectlyTypedTranche) =
              tranches.tranchesById(alienTrancheId)

            val rootTrancheId = nonAlienTrancheIds.last

            val freshImmutableObjectStorage =
              configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
                .build(tranches)

            val samplingSessionWithTrancheForIncompatibleType: Session[Unit] =
              for {
                _ <- freshImmutableObjectStorage.retrieve[Fork](
                  rootTrancheId
                ) flatMap (part =>
                  FreeT.liftT(Try {
                    part.hashCode
                  }.toEither)
                ) // Force all proxies to load (and therefore fail), as the hash calculation traverses the part structure.
              } yield ()

            freshImmutableObjectStorage.runForEffectsOnly(
              samplingSessionWithTrancheForIncompatibleType
            ) shouldBe a[Left[_, _]]
          }
      }

  it should "result in a smaller tranche when there is a tranche chain covering some of its substructure" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val isolatedSpokeTranche = {
            val isolatedSpokeTranches = new FakeTranches

            val immutableObjectStorage =
              configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
                .build(isolatedSpokeTranches)

            val root = partGrowth.parts().last

            val isolatedSpokeStorageSession: Session[TrancheId] =
              immutableObjectStorage.store(root)

            val Right(isolatedTrancheId) =
              immutableObjectStorage.runToYieldTrancheId(
                isolatedSpokeStorageSession
              )

            isolatedSpokeTranches.tranchesById(isolatedTrancheId)
          }

          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          val rootTrancheId = trancheIds.last

          val rootTranche = tranches.tranchesById(rootTrancheId)

          rootTranche.payload.length should be < isolatedSpokeTranche.payload.length
      }

  it should "be idempotent in terms of object identity when retrieving using the same tranche id" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val tranches = new FakeTranches

          val immutableObjectStorage =
            configurationThatForgetsItsTranches(
              sessionCycleCountWhenStoredTranchesAreNotRecycled
            )
              .build(tranches)

          val trancheIds =
            partGrowth.storeViaMultipleSessions(immutableObjectStorage)

          for (sampleTrancheId <- trancheIds) {
            val retrievalSession: Session[Unit] = for {
              retrievedPartTakeOne <- immutableObjectStorage.retrieve[Part](
                sampleTrancheId
              )
              retrievedPartTakeTwo <- immutableObjectStorage.retrieve[Part](
                sampleTrancheId
              )
              trancheIdFromStorageInThisSession <- immutableObjectStorage.store(
                retrievedPartTakeOne
              )
              retrievedPartTakeThree <- immutableObjectStorage.retrieve[Part](
                trancheIdFromStorageInThisSession
              )
              retrievedPartTakeFour <- immutableObjectStorage.retrieve[Part](
                sampleTrancheId
              )
            } yield {
              retrievedPartTakeTwo should be theSameInstanceAs retrievedPartTakeOne
              retrievedPartTakeThree should be theSameInstanceAs retrievedPartTakeOne
              retrievedPartTakeFour should be theSameInstanceAs retrievedPartTakeThree
            }

            immutableObjectStorage
              .runForEffectsOnly(retrievalSession) shouldBe a[Right[_, _]]
          }
      }

  it should "preserve object identity when storing and then retrieving using the same tranche id" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val tranches = new FakeTranches

          val immutableObjectStorage =
            configurationThatForgetsItsTranches(
              sessionCycleCountWhenStoredTranchesAreNotRecycled
            )
              .build(tranches)

          val parts = partGrowth.parts()

          for (samplePart <- parts) {
            val retrievalSession: Session[Unit] = for {
              trancheId <- immutableObjectStorage.store(
                samplePart
              )
              retrievedPart <- immutableObjectStorage.retrieve[Part](
                trancheId
              )
              trancheIdTakeTwo <- immutableObjectStorage.store(
                retrievedPart
              )
              retrievedPartTakeTwo <- immutableObjectStorage.retrieve[Part](
                trancheIdTakeTwo
              )
            } yield {
              retrievedPart should be theSameInstanceAs samplePart
              retrievedPartTakeTwo should be theSameInstanceAs retrievedPart
            }

            immutableObjectStorage
              .runForEffectsOnly(retrievalSession) shouldBe a[Right[_, _]]
          }
      }

  "yielding an object whose state is not stored" should "be supported" in
    partGrowthLeadingToRootForkTrials(allowDuplicates = true)
      .and(sessionCycleCounts)
      .withStrategy(
        casesLimitStrategyFactory =
          _ => CasesLimitStrategy.timed(testCycleDuration),
        complexityLimit = complexityLimit
      )
      .supplyTo {
        (partGrowth, sessionCycleCountWhenStoredTranchesAreNotRecycled) =>
          val expectedParts = partGrowth.parts()

          val tranches = new FakeTranches

          val immutableObjectStorage =
            configuration(sessionCycleCountWhenStoredTranchesAreNotRecycled)
              .build(tranches)

          val retrievalSession: Session[Vector[Part]] =
            expectedParts.pure[Session]

          // Let the retrieved parts escape the session...
          val Right(retrievedParts) =
            immutableObjectStorage.runToYieldResult(retrievalSession)

          retrievedParts should contain theSameElementsInOrderAs expectedParts

          retrievedParts.map(_.useProblematicClosure) should equal(
            expectedParts.map(_.useProblematicClosure)
          )

          Inspectors.forAll(retrievedParts)(retrievedPart =>
            Inspectors.forAll(expectedParts)(expectedPart =>
              retrievedPart should not be theSameInstanceAs(expectedPart)
            )
          )
      }

  "dealing with sets that increase in size" should "not cause problems" in sessionCycleCounts
    .withLimit(2)
    .supplyTo { sessionCycleCountWhenStoredTranchesAreNotRecycled =>
      val tranches = new FakeTranches

      val immutableObjectStorage =
        configurationForSetsAndMaps(
          sessionCycleCountWhenStoredTranchesAreNotRecycled
        ).build(tranches)

      val numberOfIterations = 1400

      val initialSession = for {
        trancheId <- immutableObjectStorage.store(Set.empty)
      } yield trancheId

      val Right(initialTrancheIdForEmptySet) =
        immutableObjectStorage
          .runToYieldTrancheId(initialSession)

      class StatefulUpdateProcess {
        private val randomBehaviour = new Random(73473L)

        def updateSet(aSet: Set[TrancheLocalObjectReferenceId], index: Int) = {
          randomBehaviour.nextInt(3) match {
            case 0 => aSet + index
            case 1 => aSet ++ aSet.map(3 * _)
            case 2 => aSet - randomBehaviour.nextInt(index)
          }
        }
      }

      object sessionUpdate  extends StatefulUpdateProcess
      object exemplarUpdate extends StatefulUpdateProcess

      val activity = (0 until numberOfIterations).foldLeft(
        IO.pure(initialTrancheIdForEmptySet -> Set.empty[Int])
      )((priorActivity, index) =>
        priorActivity.flatMap { case (trancheId, exemplarSet) =>
          val session = for {
            aSet <- immutableObjectStorage.retrieve[Set[Int]](
              trancheId
            )
            anUpdatedSet = sessionUpdate.updateSet(aSet, index)
            trancheId <- immutableObjectStorage.store(anUpdatedSet)
          } yield trancheId -> anUpdatedSet

          IO {
            val Right((trancheIdForUpdate, anUpdatedSet)) =
              immutableObjectStorage
                .runToYieldResult(session)

            if (0 == index / 100) println(anUpdatedSet)

            trancheIdForUpdate -> exemplarUpdate.updateSet(exemplarSet, index)
          }
        }
      )

      activity
        .flatMap { case (finalTrancheId, exemplarSet) =>
          IO {
            val Right(retrievedSet) =
              immutableObjectStorage.runToYieldResult(
                immutableObjectStorage.retrieve[Set[Int]](finalTrancheId)
              )

            retrievedSet shouldBe exemplarSet
          }
        }
        .unsafeRunSync()
    }

  "a class with an inherited final method" should "be able to be proxied" in sessionCycleCounts
    .withLimit(2)
    .supplyTo { sessionCycleCountWhenStoredTranchesAreNotRecycled =>
      val example =
        HasAFinalMethodAndOneThatCanBeProxied("example")

      val tranches = new FakeTranches

      val immutableObjectStorage =
        configurationForSetsAndMaps(
          sessionCycleCountWhenStoredTranchesAreNotRecycled
        ).build(tranches)

      val Right(trancheIdForFiveEntries) =
        immutableObjectStorage.runToYieldTrancheId(
          immutableObjectStorage.store(example)
        )

      val sessionWrappingFiveEntriesInAList: Session[TrancheId] =
        for {
          exampleFromStorage <- immutableObjectStorage
            .retrieve[HasAFinalMethodAndOneThatCanBeProxied](
              trancheIdForFiveEntries
            )
          trancheId <- immutableObjectStorage.store(
            List(exampleFromStorage, 2, exampleFromStorage)
          )
        } yield trancheId

      val Right(trancheIdForList) =
        immutableObjectStorage.runToYieldTrancheId(
          sessionWrappingFiveEntriesInAList
        )

      val Right(list) = immutableObjectStorage.runToYieldResult(
        immutableObjectStorage.retrieve[List[_]](trancheIdForList)
      )

      list should be(List(example, 2, example))
    }

  "a hash map" should "be able to be proxied" in sessionCycleCounts
    .withLimit(2)
    .supplyTo { sessionCycleCountWhenStoredTranchesAreNotRecycled =>
      val fiveEntries =
        HashMap(1 -> "One", 2 -> "Two", 3 -> "Three", 4 -> "Four", 5 -> "Five")

      val tranches = new FakeTranches

      val immutableObjectStorage =
        configurationForSetsAndMaps(
          sessionCycleCountWhenStoredTranchesAreNotRecycled
        ).build(tranches)

      val Right(trancheIdForFiveEntries) =
        immutableObjectStorage.runToYieldTrancheId(
          immutableObjectStorage.store(fiveEntries)
        )

      val sessionWrappingFiveEntriesInAList: Session[TrancheId] =
        for {
          fiveEntriesFromStorage <- immutableObjectStorage
            .retrieve[HashMap[Int, String]](trancheIdForFiveEntries)
          trancheId <- immutableObjectStorage.store(
            List(fiveEntriesFromStorage, 2, fiveEntriesFromStorage)
          )
        } yield trancheId

      val Right(trancheIdForList) =
        immutableObjectStorage.runToYieldTrancheId(
          sessionWrappingFiveEntriesInAList
        )

      val Right(list) = immutableObjectStorage.runToYieldResult(
        immutableObjectStorage.retrieve[List[_]](trancheIdForList)
      )

      list should be(List(fiveEntries, 2, fiveEntries))
    }
}

case class HasAFinalMethodAndOneThatCanBeProxied(wrapped: String) {
  final def finalMethod(): String = s"That's final - $wrapped!"

  def canBeProxied(): String = s"The buck stops here - $wrapped."
}
