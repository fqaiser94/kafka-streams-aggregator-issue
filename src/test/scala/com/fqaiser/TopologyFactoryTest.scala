package com.fqaiser

import java.util.Properties

import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.CollectionHasAsScala

class TopologyFactoryTest extends AnyFunSuite with Matchers {

  val schemaRegistryClient: SchemaRegistryClient = new MockSchemaRegistryClient()

  // TODO: still need a test with embedded kafka to go through adder/subtractor behaviour

  val inputTopicName = "inputTopic"
  val outputTopicName = "outputTopic"

  val animalId = 1
  val zooId = 10
  val amount = 10

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
  val factory: TopologyFactory = TopologyFactory(inputTopicName, outputTopicName, schemaRegistryClient)

  test("single input output") {
    val testDriver = new TopologyTestDriver(factory.topology, props)
    val inputTopic = testDriver.createInputTopic(inputTopicName, factory.inputKeySerde.serializer, factory.inputValueSerde.serializer)
    val outputTopic = testDriver.createOutputTopic(outputTopicName, factory.outputKeySerde.deserializer(), factory.outputValueSerde.deserializer())

    println(s"schemaRegistryClient.getAllSubjects: ${schemaRegistryClient.getAllSubjects}")

    val animalKey = AnimalKey(animalId)
    val animalValue = AnimalValue(zooId, amount)
    inputTopic.pipeInput(animalKey, animalValue)

    val result = outputTopic.readKeyValuesToList().asScala.toList

    val zooAnimalsKey = ZooAnimalsKey(zooId)
    val zooAnimalsValue = ZooAnimalsValue(List(animalValue))
    val expected = Seq(
      new KeyValue(zooAnimalsKey, zooAnimalsValue)
    )

    println(result)
    result should contain theSameElementsInOrderAs expected
    testDriver.close()
  }

}
