import sbt.Configurations.config
import sbt.Defaults.testSettings
import sbt.Keys.libraryDependencies

lazy val Benchmark = config("benchmark") extend Test

resolvers in ThisBuild += Resolver.jcenterRepo

resolvers in ThisBuild += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/releases"

lazy val settings = Seq(
  organization := "com.sageserpent",
  name := "curium",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.8",
  scalacOptions ++= Seq(
    "-Xexperimental",
    "-target:jvm-1.8",
    "-Ypartial-unification"
  ),
  libraryDependencies += "org.typelevel" %% "cats-core" % "1.6.0",
  libraryDependencies += "org.typelevel" %% "alleycats-core" % "1.6.0",
  libraryDependencies += "org.typelevel" %% "cats-effect" % "1.2.0",
  libraryDependencies += "net.bytebuddy" % "byte-buddy" % "1.9.12",
  libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
  libraryDependencies += "io.github.nicolasstucki" %% "multisets" % "0.4",
  libraryDependencies += "com.twitter" %% "chill" % "0.9.3",
  libraryDependencies += "com.google.guava" % "guava" % "28.0-jre",
  libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "2.7.0",
  libraryDependencies += "org.tpolecat" %% "doobie-core" % "0.7.0-M3",
  libraryDependencies += "org.tpolecat" %% "doobie-h2" % "0.7.0-M3",
  libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "2.5.2",
  libraryDependencies += "com.h2database" % "h2" % "1.4.199",
  libraryDependencies += "com.zaxxer" % "HikariCP" % "3.3.1",
  libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21" % "provided",
  libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.21" % "test",
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.5" % "test",
  libraryDependencies += "com.sageserpent" %% "americium" % "0.1.5" % "test",
  libraryDependencies += "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.0-1" % "test",
  libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.8.2" % "benchmark",
  testFrameworks in Benchmark += new TestFramework(
    "org.scalameter.ScalaMeterFramework"
  )
)

lazy val curium = (project in file("."))
  .configs(Benchmark)
  .settings(settings ++ inConfig(Benchmark)(testSettings): _*)
