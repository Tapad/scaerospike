credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers += "Scala Tools Nexus" at "http://nexus.tapad.com:8080/nexus/content/groups/aggregate/"

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.7-TAPAD")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.4.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.3")

