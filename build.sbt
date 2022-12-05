name := "demo"
version := "0.2.0"
scalaVersion := "2.11.8"
organization := "com.azavea"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-feature")
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

shellPrompt := { s => Project.extract(s).currentProject.id + " > " }

javaHome := Some(file("/Library/Java/JavaVirtualMachines/jdk1.8.0_261.jdk/Contents/Home/"))
// We need to bump up the memory for some of the examples working with the landsat image.
javaOptions += "-Xmx4G"

fork in run := true
outputStrategy in run := Some(StdoutOutput)
connectInput in run := true
// geotrellis-spark 2.1.0
libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "2.3.3",
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "com.typesafe.akka" %% "akka-actor"  % "2.4.3",
  "com.typesafe.akka" %% "akka-http" % "10.0.3",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.7",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "joda-time" % "joda-time" % "2.10.1",
  "ch.qos.logback" % "logback-classic" % "1.0.9",
 // "ch.megard" %% "akka-http-cors" % "0.4.2"
)

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
