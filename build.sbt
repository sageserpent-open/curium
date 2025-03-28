import sbt.Configurations.config
import sbt.Defaults.testSettings
import sbt.Keys.libraryDependencies

lazy val openSesames = Seq(
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)
lazy val settings = Seq(
  organization := "com.sageserpent",
  name         := "curium",
  scalaVersion := "2.13.16",
  scalacOptions += s"-java-output-version:${javaVersion}",
  javacOptions ++= Seq("-source", javaVersion, "-target", javaVersion),
  libraryDependencies += "org.typelevel" %% "cats-core"      % "2.7.0",
  libraryDependencies += "org.typelevel" %% "cats-free"      % "2.7.0",
  libraryDependencies += "org.typelevel" %% "alleycats-core" % "2.7.0",
  libraryDependencies += "org.typelevel" %% "cats-effect"    % "3.3.5",
  libraryDependencies += "net.bytebuddy"  % "byte-buddy"     % "1.17.4",
  libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.0",
  libraryDependencies += "io.altoo"   %% "scala-kryo-serialization" % "1.2.1",
  libraryDependencies += "io.findify" %% "flink-scala-api"          % "1.15-2"
    exclude ("com.esotericsoftware.kryo", "kryo")
    exclude ("com.twitter", "chill-java"),
  libraryDependencies += "com.google.guava" % "guava" % "28.0-jre",
  libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "3.1.2",
  libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "3.5.0",
  libraryDependencies += "com.h2database"   % "h2"          % "1.4.200",
  libraryDependencies += "org.rocksdb"      % "rocksdbjni"  % "7.4.5",
  libraryDependencies += "org.scalatest"   %% "scalatest"   % "3.2.10" % Test,
  libraryDependencies += "com.sageserpent" %% "americium"   % "1.8.1"  % Test,
  libraryDependencies += "com.zaxxer"       % "HikariCP"    % "5.0.0"  % Test,
  libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.35" % Provided,
  libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.35" % Test,
  libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.21" % Benchmark,
  libraryDependencies += "org.typelevel" %% "cats-collections-core" % "0.9.3" % Benchmark,
  Benchmark / testFrameworks += new TestFramework(
    "org.scalameter.ScalaMeterFramework"
  ),
  Test / fork := true,
  Test / javaOptions ++= openSesames,
  Benchmark / fork := true,
  Benchmark / javaOptions ++= openSesames :+ "-Xmx14G",
  Test / parallelExecution := false,
  publishMavenStyle        := true,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT"))
)

lazy val Benchmark = config("benchmark") extend Test
lazy val curium = (project in file("."))
  .configs(Benchmark)
  .settings(settings ++ inConfig(Benchmark)(testSettings): _*)
val javaVersion = "17"
