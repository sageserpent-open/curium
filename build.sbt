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
  libraryDependencies += "org.typelevel" %% "cats-core"      % "2.13.0",
  libraryDependencies += "org.typelevel" %% "cats-free"      % "2.13.0",
  libraryDependencies += "org.typelevel" %% "alleycats-core" % "2.13.0",
  libraryDependencies += "org.typelevel" %% "cats-effect"    % "3.6.0",
  libraryDependencies += "net.bytebuddy"  % "byte-buddy"     % "1.17.4",
  libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
  libraryDependencies += "io.altoo"   %% "scala-kryo-serialization" % "1.2.1",
  libraryDependencies += "io.findify" %% "flink-scala-api"          % "1.15-2"
    exclude ("com.esotericsoftware.kryo", "kryo")
    exclude ("com.twitter", "chill-java"),
  libraryDependencies += "com.google.guava" % "guava" % "33.4.6-jre",
  libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "3.2.0",
  libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "4.3.2",
  libraryDependencies += "com.h2database"   % "h2"          % "2.3.232",
  libraryDependencies += "org.rocksdb"      % "rocksdbjni"  % "9.10.0",
  libraryDependencies += "org.scalatest"   %% "scalatest"   % "3.2.19" % Test,
  libraryDependencies += "com.sageserpent" %% "americium"   % "1.20.4" % Test,
  libraryDependencies += "com.zaxxer"       % "HikariCP"    % "6.3.0"  % Test,
  libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.17" % Provided,
  libraryDependencies += "org.slf4j" % "slf4j-nop" % "2.0.17" % Test,
  libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.21" % Benchmark,
  libraryDependencies += "org.typelevel" %% "cats-collections-core" % "0.9.9" % Benchmark,
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
