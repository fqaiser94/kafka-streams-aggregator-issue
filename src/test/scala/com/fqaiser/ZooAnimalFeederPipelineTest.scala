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
  val zooId1 = 10
  val foodId1 = 100
  val calories1 = 1
  val maxCalories = 10

  private type testFn = (
      TopologyTestDriver,
      TestInputTopic[AnimalKey, AnimalValue],
      TestInputTopic[FoodKey, FoodValue],
      TestOutputTopic[OutputKey, OutputValue]
  ) => Assertion

  private def runTest(testFunction: testFn): Unit = {
    val animalsTopicName = "animalsTopic"
    val foodTopicName = "foodTopic"
    val outputTopicName = "outputTopic"

    val schemaRegistryClient: SchemaRegistryClient = new MockSchemaRegistryClient()
    val schemaRegistryUrl: String = "mockUrl"
    val factory = ZooAnimalFeederPipeline(
      animalsTopicName,
      foodTopicName,
      outputTopicName,
      schemaRegistryUrl,
      schemaRegistryClient
    )

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val testDriver = new TopologyTestDriver(factory.topology, props)
    val animalsTopic = testDriver.createInputTopic(
      animalsTopicName,
      factory.animalKeySerde.serializer,
      factory.animalValueSerde.serializer
    )
    val foodTopic = testDriver.createInputTopic(
      foodTopicName,
      factory.foodKeySerde.serializer,
      factory.foodValueSerde.serializer
    )
    val outputTopic = testDriver.createOutputTopic(
      outputTopicName,
      factory.outputKeySerde.deserializer(),
      factory.outputValueSerde.deserializer()
    )
    try {
      testFunction(testDriver, animalsTopic, foodTopic, outputTopic)
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
    Scenario("1 animal created, no food arrives") {
      runTest { (testDriver, animalTopic, foodTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
        animalTopic.pipeInput(animalKey, animalValue)

        val expected = Seq.empty

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }

    Scenario("1 animal created, 1 food parcel arrives") {
      runTest { (testDriver, animalTopic, foodTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
        animalTopic.pipeInput(animalKey, animalValue)

        val foodKey = FoodKey(foodId1)
        val foodValue = FoodValue(foodId1, zooId1, calories1)
        foodTopic.pipeInput(foodKey, foodValue)

        val expected = Seq(
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }
  }

}
