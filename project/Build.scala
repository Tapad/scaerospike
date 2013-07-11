import _root_.sbtassembly.Plugin.AssemblyKeys
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import sbtrelease.ReleasePlugin._
import sbtrelease._
import ReleaseStateTransformations._

object Scaerospike extends Build {

  lazy val scaerospike: Project = Project("scaerospike", file("."),
    settings = Config.buildSettings ++
      sbtassembly.Plugin.assemblySettings ++
      Seq(
        mainClass in assembly := Some("com.tapad.aerospike.DataPump"),
        jarName in assembly := "datapump.jar"
      ) ++ Seq(libraryDependencies ++=
        Seq(
          "com.aerospike" % "aerospike-client" % "3.0.1",
          "org.scalatest" %% "scalatest" % "1.9.1" % "test"
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

  val buildSettings = Defaults.defaultSettings ++ releaseSettings ++ publishToNexus ++ Seq(
    organization := "com.tapad",
    scalaVersion := "2.10.2",
    resolvers += tapadNexus,
    publishArtifact in(Compile, packageDoc) := false,
    ReleaseKeys.nextVersion := {
      ver => Version(ver).map(_.bumpBugfix.asSnapshot.string).getOrElse(versionFormatError)
    },
    ReleaseKeys.releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runTest,
      setReleaseVersion,
      commitReleaseVersion, // : ReleaseStep, performs the initial git checks
      tagRelease,
      publishArtifacts, // : ReleaseStep, checks whether `publishTo` is properly set up
      setNextVersion,
      commitNextVersion
    )
  )

}
