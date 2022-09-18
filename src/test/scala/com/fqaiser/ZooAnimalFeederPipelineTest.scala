package com.fqaiser

import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.kafka.streams._
import org.scalatest.compatible.Assertion
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

import java.util.Properties
import _root_.scala.jdk.CollectionConverters.CollectionHasAsScala

class ZooAnimalFeederPipelineTest extends AnyFeatureSpec with Matchers {

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
  val maxCalories = 5

  private type testFn = (
      TopologyTestDriver,
      TestInputTopic[AnimalKey, AnimalValue],
      TestInputTopic[FoodKey, FoodValue],
      TestOutputTopic[OutputKey, OutputValue],
      TestOutputTopic[AnimalStatusKey, AnimalCalorieFill]
  ) => Assertion

  private def runTest(testFunction: testFn): Unit = {
    val animalsTopicName = "animalsTopic"
    val foodTopicName = "foodTopic"
    val processedTopicName = "processedFoodTopic"
    val animalStatusTopicName = "animalStatusTopic"

    val schemaRegistryClient: SchemaRegistryClient = new MockSchemaRegistryClient()
    val schemaRegistryUrl: String = "mockUrl"
    val factory = ZooAnimalFeederPipeline(
      animalsTopicName,
      foodTopicName,
      processedTopicName,
      animalStatusTopicName,
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
    val processedFoodTopic = testDriver.createOutputTopic(
      processedTopicName,
      factory.processedFoodKeySerde.deserializer(),
      factory.processedFoodValueSerde.deserializer()
    )
    val animalStatusTopic = testDriver.createOutputTopic(
      animalStatusTopicName,
      factory.animalStatusKeySerde.deserializer(),
      factory.animalStatusValueSerde.deserializer()
    )
    try {
      testFunction(testDriver, animalsTopic, foodTopic, processedFoodTopic, animalStatusTopic)
    } finally {
      testDriver.close()
    }
  }

  private def topicShouldContainTheSameElementsAs[K, V](
      topic: TestOutputTopic[K, V],
      expected: Seq[KeyValue[K, V]]
  ): Assertion = {
    val result = topic.readKeyValuesToList().asScala.toList
    println("result")
    result.foreach(println)
    println("expected")
    expected.foreach(println)
    result should contain theSameElementsInOrderAs expected
  }

  Feature("only one type of animal in a given zoo") {
    Scenario("1 animal created, no food arrives") {
      runTest { (testDriver, animalTopic, foodTopic, processedFoodTopic, animalStatusTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
        animalTopic.pipeInput(animalKey, animalValue)

        val processedFoodExpected = Seq.empty
        val animalStatusExpected = Seq.empty

        topicShouldContainTheSameElementsAs(processedFoodTopic, processedFoodExpected)
        topicShouldContainTheSameElementsAs(animalStatusTopic, animalStatusExpected)
      }
    }

    Scenario("1 animal created, 1 food parcel of 1 calorie arrives") {
      runTest { (testDriver, animalTopic, foodTopic, processedFoodTopic, animalStatusTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
        animalTopic.pipeInput(animalKey, animalValue)

        val foodKey = FoodKey(foodId1)
        val foodValue = FoodValue(foodId1, zooId1, calories1)
        foodTopic.pipeInput(foodKey, foodValue)

        val processedFoodExpected = Seq(
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1))
        )
        val animalStatusExpected = Seq(
          new KeyValue(AnimalStatusKey(zooId1, animalId1), AnimalCalorieFill(calories1))
        )

        topicShouldContainTheSameElementsAs(processedFoodTopic, processedFoodExpected)
        topicShouldContainTheSameElementsAs(animalStatusTopic, animalStatusExpected)
      }
    }

    Scenario("1 animal created, 2 food parcel of 1 calorie each arrive") {
      runTest { (testDriver, animalTopic, foodTopic, processedFoodTopic, animalStatusTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
        animalTopic.pipeInput(animalKey, animalValue)

        val foodKey = FoodKey(foodId1)
        val foodValue = FoodValue(foodId1, zooId1, calories1)
        foodTopic.pipeInput(foodKey, foodValue)
        foodTopic.pipeInput(foodKey, foodValue)

        val processedFoodExpected = Seq(
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1)),
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1 * 2))
        )
        val animalStatusExpected = Seq(
          new KeyValue(AnimalStatusKey(zooId1, animalId1), AnimalCalorieFill(calories1)),
          new KeyValue(AnimalStatusKey(zooId1, animalId1), AnimalCalorieFill(calories1 * 2))
        )

        topicShouldContainTheSameElementsAs(processedFoodTopic, processedFoodExpected)
        topicShouldContainTheSameElementsAs(animalStatusTopic, animalStatusExpected)
      }
    }

    Scenario("1 animal created, 6 food parcels of 1 calorie each arrive") {
      runTest { (testDriver, animalTopic, foodTopic, processedFoodTopic, animalStatusTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
        animalTopic.pipeInput(animalKey, animalValue)

        val foodKey = FoodKey(foodId1)
        val foodValue = FoodValue(foodId1, zooId1, calories1)
        (1 to 6).foreach(_ => foodTopic.pipeInput(foodKey, foodValue))

        val processedFoodExpected = Seq(
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1 * 1)),
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1 * 2)),
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1 * 3)),
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1 * 4)),
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1 * 5)),
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, -1, 0))
        )
        val animalStatusExpected = Seq(
          new KeyValue(AnimalStatusKey(zooId1, animalId1), AnimalCalorieFill(calories1 * 1)),
          new KeyValue(AnimalStatusKey(zooId1, animalId1), AnimalCalorieFill(calories1 * 2)),
          new KeyValue(AnimalStatusKey(zooId1, animalId1), AnimalCalorieFill(calories1 * 3)),
          new KeyValue(AnimalStatusKey(zooId1, animalId1), AnimalCalorieFill(calories1 * 4)),
          new KeyValue(AnimalStatusKey(zooId1, animalId1), AnimalCalorieFill(calories1 * 5))
        )

        topicShouldContainTheSameElementsAs(processedFoodTopic, processedFoodExpected)
        topicShouldContainTheSameElementsAs(animalStatusTopic, animalStatusExpected)
      }
    }
  }

  Feature("Test state store range method rigorously?") {
    Scenario("Lots of animals at a zoo") {

    }
  }

  Feature("Edge cases") {
    Scenario("what should happen when a Food parcel comes in for an animal that is not associated with the zoo?") {
      // TODO: this isn't possible right now b/c our food parcels don't contain animalId.
    }
  }

}
