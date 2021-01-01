package com.fqaiser

import java.util.Properties
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.Assertion
import org.scalatest.compatible.Assertion
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.CollectionHasAsScala

class TopologyFactoryTest extends AnyFeatureSpec with Matchers {

  // TODO: still need a test with embedded kafka to go through adder/subtractor behaviour

  // TODO: for both single and multiple animals:
  // - test delete
  // - test update
  // - test "update" with no change

  val animalId1 = 1
  val animalId2 = 2
  val zooId = 10
  val amount = 10

  private type testFn = (TopologyTestDriver, TestInputTopic[AnimalKey, AnimalValue], TestOutputTopic[ZooAnimalsKey, ZooAnimalsValue]) => Assertion

  private def runTest(testFunction: testFn): Unit = {
    val inputTopicName = "inputTopic"
    val outputTopicName = "outputTopic"

    val schemaRegistryClient: SchemaRegistryClient = new MockSchemaRegistryClient()
    val factory: TopologyFactory = TopologyFactory(inputTopicName, outputTopicName, "mockSchemaRegistryUrl", schemaRegistryClient)

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val testDriver = new TopologyTestDriver(factory.topology1, props)
    val inputTopic = testDriver.createInputTopic(inputTopicName, factory.animalKeySerde.serializer, factory.animalValueSerde.serializer)
    val outputTopic = testDriver.createOutputTopic(outputTopicName, factory.outputKeySerde.deserializer(), factory.outputValueSerde.deserializer())
    try {
      testFunction(testDriver, inputTopic, outputTopic)
    } finally {
      testDriver.close()
    }
  }

  private def outputTopicShouldContainTheSameElementsAs[K, V](outputTopic: TestOutputTopic[K, V], expected: Seq[KeyValue[K, V]]): Assertion = {
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
        val animalValue = AnimalValue(animalId1, zooId, amount)
        inputTopic.pipeInput(animalKey, animalValue)

        val zooAnimalsKey = ZooAnimalsKey(zooId)
        val zooAnimalsValue = ZooAnimalsValue(List(animalValue))
        val expected = Seq(
          new KeyValue(zooAnimalsKey, zooAnimalsValue)
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }

    Scenario("1 animal created and then updated") {
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue1 = AnimalValue(animalId1, zooId, amount)
        val animalValue2 = animalValue1.copy(amount = animalValue1.amount + 10)
        inputTopic.pipeInput(animalKey, animalValue1)
        inputTopic.pipeInput(animalKey, animalValue2)

        val zooAnimalsKey = ZooAnimalsKey(zooId)
        val expected = Seq(
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List(animalValue1))),
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List.empty)),
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List(animalValue2)))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }

    Scenario("1 animal created and then deleted") {
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId, amount)
        inputTopic.pipeInput(animalKey, animalValue)
        inputTopic.pipeInput(animalKey, null)

        val zooAnimalsKey = ZooAnimalsKey(zooId)
        val expected = Seq(
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List(animalValue))),
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List.empty))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }

    Scenario("1 animal created, updated, and then deleted") {
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue1 = AnimalValue(animalId1, zooId, amount)
        val animalValue2 = animalValue1.copy(amount = animalValue1.amount + 10)
        inputTopic.pipeInput(animalKey, animalValue1)
        inputTopic.pipeInput(animalKey, animalValue2)
        inputTopic.pipeInput(animalKey, null)

        val zooAnimalsKey = ZooAnimalsKey(zooId)
        val expected = Seq(
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List(animalValue1))),
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List.empty)),
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List(animalValue2))),
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List.empty))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }
  }

  Feature("Edge cases") {
    Scenario("Same event comes in multiple times") {
      // interesting, multiple events come in with the exact same values, notice no multiple output events
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId, amount)
        inputTopic.pipeInput(animalKey, animalValue)
        inputTopic.pipeInput(animalKey, animalValue)
        inputTopic.pipeInput(animalKey, animalValue)

        val zooAnimalsKey = ZooAnimalsKey(zooId)
        val expected = Seq(
          new KeyValue(zooAnimalsKey, ZooAnimalsValue(List(animalValue)))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }
  }

  Feature("multiple types of animals in a given zoo") {
    Scenario("2 animal create events") {
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animalValue1 = AnimalValue(animalId1, zooId, amount)
        val animalValue2 = AnimalValue(animalId2, zooId, amount)
        inputTopic.pipeInput(AnimalKey(animalId1), animalValue1)
        inputTopic.pipeInput(AnimalKey(animalId2), animalValue2)

        val expected = Seq(
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animalValue1))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animalValue1, animalValue2)))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }

    Scenario("2 animal create events and 1 animal is updated") {
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animal1Value = AnimalValue(animalId1, zooId, amount)
        val animal2Value = AnimalValue(animalId2, zooId, amount)
        val animal2Value2 = animal2Value.copy(amount = animal2Value.amount + 10)
        inputTopic.pipeInput(AnimalKey(animalId1), animal1Value)
        inputTopic.pipeInput(AnimalKey(animalId2), animal2Value)
        inputTopic.pipeInput(AnimalKey(animalId2), animal2Value2)

        val expected = Seq(
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value, animal2Value))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value, animal2Value2)))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }

    Scenario("2 animal create events and 1 animal is deleted") {
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animal1Value = AnimalValue(animalId1, zooId, amount)
        val animal2Value = AnimalValue(animalId2, zooId, amount)
        inputTopic.pipeInput(AnimalKey(animalId1), animal1Value)
        inputTopic.pipeInput(AnimalKey(animalId2), animal2Value)
        inputTopic.pipeInput(AnimalKey(animalId2), null)

        val expected = Seq(
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value, animal2Value))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value)))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }

    Scenario("2 animal create events and 1 animal is updated and the other animal is deleted") {
      runTest { (testDriver, inputTopic, outputTopic) =>
        val animal1Value = AnimalValue(animalId1, zooId, amount)
        val animal2Value = AnimalValue(animalId2, zooId, amount)
        val animal2Value2 = animal2Value.copy(amount = animal2Value.amount + 10)
        inputTopic.pipeInput(AnimalKey(animalId1), animal1Value)
        inputTopic.pipeInput(AnimalKey(animalId2), animal2Value)
        inputTopic.pipeInput(AnimalKey(animalId2), animal2Value2)
        inputTopic.pipeInput(AnimalKey(animalId1), null)

        val expected = Seq(
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value, animal2Value))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal1Value, animal2Value2))),
          new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animal2Value2)))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }
  }

}
