package com.fqaiser

import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import org.apache.kafka.streams._
import org.scalatest.compatible.Assertion
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

import java.util.Properties
import _root_.scala.jdk.CollectionConverters.CollectionHasAsScala

class ZooAnimalFeederPipelineTest extends AnyFeatureSpec with Matchers with EmbeddedKafkaStreams {

  // TODO: still need a test with embedded kafka to go through adder/subtractor behaviour

  // TODO: for both single and multiple animals:
  // - test delete
  // - test update
  // - test "update" with no change

  val animalId1 = 1
  val animalId2 = 2
  val zooId = 10
  val maxCalories = 10

  private type testFn = (
      TopologyTestDriver,
      TestInputTopic[AnimalKey, AnimalValue],
      TestOutputTopic[ZooAnimalsKey, ZooAnimalsValue]
  ) => Assertion

  private def runTest(testFunction: testFn): Unit = {
    val inputTopicName = "inputTopic"
    val outputTopicName = "outputTopic"

    val schemaRegistryClient: SchemaRegistryClient = new MockSchemaRegistryClient()
    val factory = SimpleAggregator(
      inputTopicName,
      outputTopicName,
      "mockSchemaRegistryUrl",
      schemaRegistryClient
    )

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val testDriver = new TopologyTestDriver(factory.topology, props)
    val inputTopic = testDriver.createInputTopic(
      inputTopicName,
      factory.animalKeySerde.serializer,
      factory.animalValueSerde.serializer
    )
    val outputTopic = testDriver.createOutputTopic(
      outputTopicName,
      factory.outputKeySerde.deserializer(),
      factory.outputValueSerde.deserializer()
    )
    try {
      testFunction(testDriver, inputTopic, outputTopic)
    } finally {
      testDriver.close()
    }
  }

  private def outputTopicShouldContainTheSameElementsAs[K, V](
      outputTopic: TestOutputTopic[K, V],
      expected: Seq[KeyValue[K, V]]
  ): Assertion = {
    val result = outputTopic.readKeyValuesToList().asScala.toList
    println("result")
    result.foreach(println)
    println("expected")
    expected.foreach(println)
    result should contain theSameElementsInOrderAs expected
  }

  Feature("only one type of animal in a given zoo") {
    Scenario("1 animal created") {
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId, maxCalories)
        inputTopic.pipeInput(animalKey, animalValue)

        val zooAnimalsKey = ZooAnimalsKey(zooId)
        val zooAnimalsValue = ZooAnimalsValue(List(animalValue))
        val expected = Seq(
          new KeyValue(zooAnimalsKey, zooAnimalsValue)
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }
  }

}
