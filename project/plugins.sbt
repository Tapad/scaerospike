credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers += "Scala Tools Nexus" at "http://nexus.tapad.com:8080/nexus/content/groups/aggregate/"

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")


