import sbt.Configurations.config
import sbt.Defaults.testSettings
import sbt.Keys.libraryDependencies

lazy val settings = Seq(
  organization := "com.sageserpent",
  name := "curium",
  scalaVersion := "2.13.6",
  scalacOptions += s"-target:jvm-${javaVersion}",
  javacOptions ++= Seq("-source", javaVersion, "-target", javaVersion),
  libraryDependencies += "org.typelevel" %% "cats-core" % "2.6.1",
  libraryDependencies += "org.typelevel" %% "alleycats-core" % "2.6.1",
  libraryDependencies += "org.typelevel" %% "cats-effect" % "3.2.8",
  libraryDependencies += "net.bytebuddy" % "byte-buddy" % "1.11.16",
  libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.0",
  libraryDependencies += "com.twitter" %% "chill" % "0.10.0",
  libraryDependencies += "com.google.guava" % "guava" % "28.0-jre",
  libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "3.0.4",
  libraryDependencies += "org.tpolecat" %% "doobie-core" % "1.0.0-RC1",
  libraryDependencies += "org.tpolecat" %% "doobie-h2" % "1.0.0-RC1",
  libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "3.5.0",
  libraryDependencies += "com.h2database" % "h2" % "1.4.200",
  libraryDependencies += "org.rocksdb" % "rocksdbjni" % "6.22.1",
  libraryDependencies += "com.zaxxer" % "HikariCP" % "5.0.0" % "test",
  libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.32" % "provided",
  libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.32" % "test",
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  libraryDependencies += "com.sageserpent" %% "americium" % "0.1.25" % "test",
  libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.21" % "benchmark",
  libraryDependencies += "org.typelevel" %% "cats-collections-core" % "0.9.3" % "benchmark",
  Benchmark / testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
  Benchmark / fork := true,
  Benchmark / javaOptions += "-Xmx1G",
  Benchmark / javaOptions += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005",
  Test / parallelExecution := false,
  publishMavenStyle := true,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT"))
)

lazy val Benchmark = config("benchmark") extend Test
val javaVersion = "1.8"

lazy val curium = (project in file("."))
  .configs(Benchmark)
  .settings(settings ++ inConfig(Benchmark)(testSettings): _*)
