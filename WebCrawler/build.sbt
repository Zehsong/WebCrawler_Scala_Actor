ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.5"

lazy val ScalaVersion = "2.13.14"
lazy val akkaHttpVersion = "10.6.3"
lazy val akkaVersion    = "2.9.5"
lazy val alpakkaVersion = "8.0.0"
lazy val ta4jVersion = "0.16"
lazy val logbackVersion = "1.5.8"
lazy val Akka = "com.typesafe.akka"

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

lazy val root = (project in file("."))
  .settings(
    name := "WebCrawler"
  )

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1",
  Akka %% "akka-actor" % akkaVersion,
  Akka %% "akka-stream" % akkaVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.mockito" %% "mockito-scala" % "1.16.37",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "com.typesafe" % "config" % "1.4.2",
  "guru.nidi" % "graphviz-java" % "0.18.1",
  "org.neo4j.driver" % "neo4j-java-driver" % "4.4.11"
)