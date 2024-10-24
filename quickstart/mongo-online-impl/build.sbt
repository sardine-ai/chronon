ThisBuild / scalaVersion     := "2.12.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "ai.chronon"
ThisBuild / organizationName := "Chronon"

lazy val root = (project in file("."))
  .settings(
    name := "mongo-online-impl",
    libraryDependencies ++= Seq(
//      "ai.chronon" %% "api" % "0.0.77",
//      "ai.chronon" %% "online" % "0.0.77",
      "ai.chronon" %% "api" % "0.1.0-SNAPSHOT" from "file:///Users/feltorr/Projects/chronon/api/target/scala-2.12/api-assembly-felipe-adapt-gcp-0.0.79-SNAPSHOT.jar",
      "ai.chronon" %% "online" % "0.1.0-SNAPSHOT" from "file:///Users/feltorr/Projects/chronon/online/target/scala-2.12/online-assembly-felipe-adapt-gcp-0.0.79-SNAPSHOT.jar",

      "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.1", // Batch upload + structured streaming
      "org.mongodb.scala" %% "mongo-scala-driver" % "4.8.1",    // Fetching
          "ch.qos.logback" % "logback-classic" % "1.2.3",
          "org.slf4j" % "slf4j-api" % "1.7.32",
      "com.google.cloud" % "google-cloud-bigtable" % "2.43.0",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.19" % "test"
    ),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.module" % "jackson-module-scala_2.13" % "2.14.2",
    ),
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", _ @_*)         => MergeStrategy.filterDistinctLines
  case "plugin.xml"                        => MergeStrategy.last
  case PathList("com", "fasterxml", _ @_*) => MergeStrategy.last
  case PathList("com", "google", _ @_*)    => MergeStrategy.last
  case _                                   => MergeStrategy.first
}
exportJars := true
