name := "kafka-streams-aggregator-issue"

version := "0.1"

scalaVersion := "2.13.4"

val kafkaVersion = "2.6.0"
val confluentVersion = "6.0.0"
libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "io.confluent" % "kafka-streams-avro-serde" % confluentVersion,
  "io.confluent" % "kafka-schema-registry-client" % confluentVersion,
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.4",

  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test,
  "io.github.embeddedkafka" %% "embedded-kafka-schema-registry-streams" % confluentVersion % Test,
  "org.awaitility" % "awaitility-scala" % "4.0.3" % Test,
  "org.testcontainers" % "kafka" % "1.15.1" % Test,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test
)

resolvers ++= Seq(
  "Confluent" at "https://packages.confluent.io/maven/",
  "jitpack" at "https://jitpack.io"
)

// TODO: don't really specificAvro anymore
sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue

scalafmtOnCompile := true
//scalafmtTestOnCompile := true
