credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers += "Scala Tools Nexus" at "http://nexus.tapad.com:8080/nexus/content/groups/aggregate/"

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.8.2")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")


