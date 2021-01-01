name := "kafka-streams-aggregator-issue"

version := "0.1"

scalaVersion := "2.13.4"

val kafkaVersion = "2.6.0"
val confluentVersion = "6.0.0"
libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "io.confluent" % "kafka-streams-avro-serde" % confluentVersion,
  "io.confluent" % "kafka-schema-registry-client" % confluentVersion,

  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test
)

resolvers += "Confluent" at "https://packages.confluent.io/maven/"

sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue

scalafmtOnCompile := true
scalafmtTestOnCompile := true