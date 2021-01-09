package com.fqaiser.serde

import SerdeUtils._
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.IterableHasAsScala

object SerdeUtilsTest {

  final case class SimpleCC(a: Int)

}

class SerdeUtilsTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  import com.fqaiser.serde.SerdeUtilsTest.SimpleCC

  private val schemaRegistryUrl: String = "mockUrl"
  private val schemaRegistryClient: SchemaRegistryClient = new MockSchemaRegistryClient()

  override def beforeEach(): Unit = {
    super.beforeEach()
    schemaRegistryClient.reset()
  }

  private val topicName = "SimpleCC"
  private val simpleCC = SimpleCC(1)

  test("should serialize simple case class as key") {
    val serde = ccSerde[SimpleCC](schemaRegistryUrl, schemaRegistryClient, isKey = true)
    val data = simpleCC
    val serialized = serde.serializer().serialize(topicName, data)
    val deserialized = serde.deserializer().deserialize(topicName, serialized)
    deserialized shouldEqual data
    schemaRegistryClient.getAllSubjects.asScala should contain theSameElementsAs Seq(s"$topicName-key")
  }

  test("should serialize simple case class as value") {
    val serde = ccSerde[SimpleCC](schemaRegistryUrl, schemaRegistryClient, isKey = false)
    val data = simpleCC
    val serialized = serde.serializer().serialize(topicName, data)
    val deserialized = serde.deserializer().deserialize(topicName, serialized)
    deserialized shouldEqual data
    schemaRegistryClient.getAllSubjects.asScala should contain theSameElementsAs Seq(s"$topicName-value")
  }

  test("should serialize null simple case class as key") {
    val serde = ccSerde[SimpleCC](schemaRegistryUrl, schemaRegistryClient, isKey = true)
    val data = null.asInstanceOf[SimpleCC]
    val serialized = serde.serializer().serialize(topicName, data)
    val deserialized = serde.deserializer().deserialize(topicName, serialized)
    deserialized shouldEqual data
    schemaRegistryClient.getAllSubjects.asScala should contain theSameElementsAs Seq(s"$topicName-key")
  }

  test("should serialize null simple case class as value") {
    val serde = ccSerde[SimpleCC](schemaRegistryUrl, schemaRegistryClient, isKey = false)
    val data = null.asInstanceOf[SimpleCC]
    val serialized = serde.serializer().serialize(topicName, data)
    val deserialized = serde.deserializer().deserialize(topicName, serialized)
    deserialized shouldEqual data
    schemaRegistryClient.getAllSubjects.asScala should contain theSameElementsAs Seq(s"$topicName-value")
  }

}
