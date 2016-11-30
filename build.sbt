name := "jhinterview"

version := "1.0"

scalaVersion := "2.11.8"

dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value
dependencyOverrides += "org.scala-lang" % "scala-reflect" % scalaVersion.value
dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "1.0.4"


libraryDependencies += "com.twitter" % "twitter-text" % "1.14.1" withJavadoc()
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.5" withJavadoc()
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.5" withJavadoc()
libraryDependencies += "com.vdurmont" % "emoji-java" % "3.1.3" withJavadoc()
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2" withJavadoc()
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0" withJavadoc()
libraryDependencies += "com.github.pathikrit" %% "better-files" % "2.16.0" % "test" withJavadoc()
libraryDependencies += "junit" % "junit" % "4.10" % "test"

libraryDependencies ++= {
  val version = "2.4.11"
  Seq(
    "com.typesafe.akka" %% "akka-slf4j" % version withJavadoc(),
    "com.typesafe.akka" %% "akka-actor" % version withJavadoc(),
    "com.typesafe.akka" %% "akka-stream" % version withJavadoc(),
    "com.typesafe.akka" %% "akka-http-core" % version withJavadoc(),
    "com.typesafe.akka" %% "akka-http-experimental" % version withJavadoc(),
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % version withJavadoc(),
    "com.typesafe.akka" %% "akka-stream-testkit" % version % "test" withJavadoc(),
    "com.typesafe.akka" %% "akka-http-testkit" % version % "test" withJavadoc(),
    "com.typesafe.akka" %% "akka-testkit" % version % "test" withJavadoc(),
    "org.scalatest"     %% "scalatest" % "2.2.6" % "test" withJavadoc()
  )
}

// for building
parallelExecution := true
// for testing
parallelExecution in Test := false

mainClass in (Compile, run) := Some("com.xrpn.jhi.Main")
mainClass in (Compile, packageBin) := Some("com.xrpn.jhi.Main")
