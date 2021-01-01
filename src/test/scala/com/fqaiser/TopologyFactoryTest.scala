package com.fqaiser

import java.util.Properties
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.CollectionHasAsScala

class TopologyFactoryTest extends AnyFeatureSpec with Matchers {

  val schemaRegistryClient: SchemaRegistryClient = new MockSchemaRegistryClient()

  // TODO: still need a test with embedded kafka to go through adder/subtractor behaviour

  val inputTopicName = "inputTopic"
  val outputTopicName = "outputTopic"

  val animalId1 = 1
  val animalId2 = 2
  val zooId = 10
  val amount = 10

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
  val factory: TopologyFactory = TopologyFactory(inputTopicName, outputTopicName, "mockSchemaRegistryUrl", schemaRegistryClient)

  Feature("only one type of animal in a given zoo") {
    Scenario("1 animal create event") {
      val testDriver = new TopologyTestDriver(factory.topology, props)
      val inputTopic = testDriver.createInputTopic(inputTopicName, factory.inputKeySerde.serializer, factory.inputValueSerde.serializer)
      val outputTopic = testDriver.createOutputTopic(outputTopicName, factory.outputKeySerde.deserializer(), factory.outputValueSerde.deserializer())

      val animalKey = AnimalKey(animalId1)
      val animalValue = AnimalValue(animalId1, zooId, amount)
      inputTopic.pipeInput(animalKey, animalValue)

      val result = outputTopic.readKeyValuesToList().asScala.toList

      val zooAnimalsKey = ZooAnimalsKey(zooId)
      val zooAnimalsValue = ZooAnimalsValue(List(animalValue))
      val expected = Seq(
        new KeyValue(zooAnimalsKey, zooAnimalsValue)
      )

      result should contain theSameElementsInOrderAs expected
      testDriver.close()
    }
  }


  // TODO: for both single and multiple animals:
  // - test delete
  // - test update
  // - test "update" with no change

  Feature("multiple types of animals in a given zoo") {
    Scenario("1 animal create event") {
      val testDriver = new TopologyTestDriver(factory.topology, props)
      val inputTopic = testDriver.createInputTopic(inputTopicName, factory.inputKeySerde.serializer, factory.inputValueSerde.serializer)
      val outputTopic = testDriver.createOutputTopic(outputTopicName, factory.outputKeySerde.deserializer(), factory.outputValueSerde.deserializer())

      val animalValue1 = AnimalValue(animalId1, zooId, amount)
      val animalValue2 = AnimalValue(animalId2, zooId, amount)
      inputTopic.pipeInput(AnimalKey(animalId1), animalValue1)
      inputTopic.pipeInput(AnimalKey(animalId2), animalValue2)

      val result = outputTopic.readKeyValuesToList().asScala.toList

      val expected = Seq(
        new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animalValue1))),
        new KeyValue(ZooAnimalsKey(zooId), ZooAnimalsValue(List(animalValue1, animalValue2)))
      )

      result should contain theSameElementsInOrderAs expected
      testDriver.close()
    }
  }

}
