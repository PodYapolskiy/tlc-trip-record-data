ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "team18"
ThisBuild / organizationName := "team18"

lazy val root = (project in file("."))
  .settings(
    name := "load-data",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.2.4" % "provided",
      "org.apache.spark" %% "spark-core" % "3.2.4" % "provided",
      "org.postgresql" % "postgresql" % "42.5.1"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case x                             => MergeStrategy.first
    }
  )
