import sbt._
import Keys._
import sbtrelease.ReleasePlugin._
import sbtrelease._
import sbtrelease.ReleasePlugin.autoImport._
import ReleaseStateTransformations._


object Scaerospike extends Build {

  lazy val scaerospike: Project = Project("scaerospike", file("."),
    settings = Config.buildSettings ++
      Seq(
      	organization := "com.tapad.scaerospike"
      ) ++ Seq(libraryDependencies ++=
        Seq(
          "com.aerospike" % "aerospike-client" % "3.0.30",
          "io.netty" % "netty-buffer" % "4.0.23.Final",
          "org.scalatest" %% "scalatest" % "2.2.6"  % "test"
        )
      )
  )
}


object Config {
  val tapadNexus = "Scala Tools Nexus" at "http://nexus.tapad.com:8080/nexus/content/groups/aggregate/"
  lazy val publishToNexus = Seq(
    publishTo <<= (version) { version: String =>
      val nexus = "http://nexus.tapad.com:8080/nexus/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT") || version.trim.endsWith("TAPAD"))
        Some("snapshots" at (nexus + "snapshots/"))
      else
        Some("releases" at (nexus + "releases/"))
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false }
  )

  val buildSettings = Defaults.defaultSettings ++ publishToNexus ++ Seq(
    organization := "com.tapad",
    scalaVersion := "2.10.6",
    crossScalaVersions := Seq("2.10.6", "2.11.8"),
    releaseCrossBuild := true,
    resolvers += tapadNexus,
    publishArtifact in(Compile, packageDoc) := false
  )
}

