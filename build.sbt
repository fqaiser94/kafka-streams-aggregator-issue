name := "kafka-streams-issues"

version := "0.1"

scalaVersion := "2.13.4"

val kafkaVersion = "2.7.0"
val confluentVersion = "6.0.1"
libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.4",
  "io.confluent" % "kafka-streams-avro-serde" % confluentVersion,
  "io.confluent" % "kafka-schema-registry-client" % confluentVersion,

  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test
)

resolvers += "Confluent" at "https://packages.confluent.io/maven/"

sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue