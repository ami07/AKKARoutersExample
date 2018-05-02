name := "AKKARoutersExample"

version := "0.1"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.12"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %%  "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.1",
  "com.typesafe.akka" %%  "akka-multi-node-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)