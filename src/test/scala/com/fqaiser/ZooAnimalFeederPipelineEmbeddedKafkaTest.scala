package com.fqaiser

import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.awaitility.scala.AwaitilitySupport
import org.scalatest.BeforeAndAfterAll
import org.scalatest.compatible.Assertion
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.time.Duration.ofMillis
import java.util
import java.util.Properties
import scala.jdk.CollectionConverters.IterableHasAsScala

class ZooAnimalFeederPipelineEmbeddedKafkaTest
    extends AnyFeatureSpec
    with Matchers
    with BeforeAndAfterAll
    with AwaitilitySupport {

  val animalId1 = 1
  val animalId2 = 2
  val zooId1 = 10
  val foodId1 = 100
  val calories1 = 1
  val maxCalories = 5

  val confluentVersion = "6.0.0"
  val schemaRegistry = new SchemaRegistryContainer(confluentVersion)
  val kafka = new KafkaContainer(DockerImageName.parse(s"confluentinc/cp-kafka:$confluentVersion"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
  }

  override def afterAll(): Unit = {
    kafka.stop()
    super.afterAll()
  }

  object TestAdminClient {
    private val adminClientConfigs = {
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
      props
    }
    private val adminClient: AdminClient =
      AdminClient.create(adminClientConfigs)

    private val replicationFactor = 1.toShort

    def createTopic(topicName: String, numPartitions: Int = 1): Unit =
      adminClient
        .createTopics(util.Collections.singletonList(new NewTopic(topicName, numPartitions, replicationFactor)))
        .all()
        // get call is to ensure that the topic is created before moving on from this method
        .get()
  }

  case class TestInputTopic[K, V](
      topicName: String,
      numPartitions: Int,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ) {

    // create topic during construction of instance
    TestAdminClient.createTopic(topicName, numPartitions)

    val producerProps: Properties = {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
      props.put(ProducerConfig.ACKS_CONFIG, "all")
      props
    }
    private val kafkaProducer = new KafkaProducer[K, V](producerProps, keySerializer, valueSerializer)

    def pipeInput(k: K, v: V, partition: Int): Unit =
      kafkaProducer.send(new ProducerRecord(topicName, partition, k, v)).get()
  }

  case class TestOutputTopic[K, V](
      topicName: String,
      numPartitions: Int,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ) {

    // create topic during construction of instance
    TestAdminClient.createTopic(topicName, numPartitions)

    private val consumerProps = {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, java.util.UUID.randomUUID.toString)
      props
    }
    private val consumer = new KafkaConsumer[K, V](consumerProps, keyDeserializer, valueDeserializer)

    // TODO: use awaitility
    def readKeyValuesToList(): List[KeyValue[K, V]] = {
      consumer.subscribe(util.Arrays.asList(topicName))
      val records = consumer.poll(ofMillis(10000))
      println("************ readKeyValuesToList")
      records.forEach(x => println(x))
      records.asScala.toList.map(record => new KeyValue(record.key(), record.value))
    }
  }

  private type testFn = (
      KafkaStreams,
      TestInputTopic[AnimalKey, AnimalValue],
      TestInputTopic[FoodKey, FoodValue],
      TestOutputTopic[OutputKey, OutputValue]
  ) => Assertion

  private def runTest(testFunction: testFn): Unit = {
    val animalsTopicName = "animalsTopic"
    val foodTopicName = "foodTopic"
    val outputTopicName = "outputTopic"

    // TODO: switch to real schema registry
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
    // TODO: should be in the factory, along with EXACTLY_ONCE config
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)

    val kafkaStreams = new KafkaStreams(factory.topology, props)

    val animalsTopic = TestInputTopic[AnimalKey, AnimalValue](
      animalsTopicName,
      2,
      factory.animalKeySerde.serializer,
      factory.animalValueSerde.serializer
    )
    val foodTopic = TestInputTopic[FoodKey, FoodValue](
      foodTopicName,
      2,
      factory.foodKeySerde.serializer,
      factory.foodValueSerde.serializer
    )
    val outputTopic = TestOutputTopic[OutputKey, OutputValue](
      outputTopicName,
      2,
      factory.outputKeySerde.deserializer(),
      factory.outputValueSerde.deserializer()
    )
    try {
      kafkaStreams.cleanUp()
      kafkaStreams.start()
      testFunction(kafkaStreams, animalsTopic, foodTopic, outputTopic)
    } finally {
      kafkaStreams.close()
    }
  }

  private def outputTopicShouldContainTheSameElementsAs[K, V](
      outputTopic: TestOutputTopic[K, V],
      expected: Seq[KeyValue[K, V]]
  ): Assertion = {
    val result = outputTopic.readKeyValuesToList()
    println("result")
    result.foreach(println)
    println("expected")
    expected.foreach(println)
    result should contain theSameElementsInOrderAs expected
  }

  Feature("") {
    Scenario("1 animal created, 1 food parcel of 1 calorie arrives") {
      runTest { (stream, animalTopic, foodTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
        animalTopic.pipeInput(animalKey, animalValue, partition = 1)

        // sleep to avoid situation where food goes to streams app before the animal exists in the state store
        Thread.sleep(5000)

        val foodKey = FoodKey(foodId1)
        val foodValue = FoodValue(foodId1, zooId1, calories1)
        foodTopic.pipeInput(foodKey, foodValue, partition = 1)

        val expected = Seq(
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }
  }

  Feature("Preseed state store changelog topic with some state") {
    Scenario("1 animal with existing calorie fill") {
      ???
    }

    Scenario("1 animal with existing calorie fill and 1 new animal comes in") {
      ???
    }
  }

}
