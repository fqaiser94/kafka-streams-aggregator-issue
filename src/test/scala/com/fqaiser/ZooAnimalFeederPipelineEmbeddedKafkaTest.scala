package com.fqaiser

import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.awaitility.scala.AwaitilitySupport
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
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
    with BeforeAndAfterEach
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

  override def beforeEach(): Unit = {
    super.beforeEach()
    kafka.start()
  }

  override def afterEach(): Unit = {
    kafka.stop()
    super.afterEach()
  }

  case class TestAdminClient() {
    private val adminClientConfigs = {
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
      props
    }
    val adminClient: AdminClient =
      AdminClient.create(adminClientConfigs)

    private val replicationFactor = 1.toShort

    def createTopic[K, V](testInputTopic: TestInputTopic[K, V]): Unit =
      createTopic(testInputTopic.topicName, testInputTopic.numPartitions)

    def createTopic[K, V](testOutputTopic: TestOutputTopic[K, V]): Unit =
      createTopic(testOutputTopic.topicName, testOutputTopic.numPartitions)

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
    TestAdminClient().createTopic(this)

    val producerProps: Properties = {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
      props.put(ProducerConfig.ACKS_CONFIG, "all")
      props
    }
    private val kafkaProducer = new KafkaProducer[K, V](producerProps, keySerializer, valueSerializer)

    // partition can be null, in which case kafka defaults to DefaultPartitioner
    def pipeInput(k: K, v: V, partition: java.lang.Integer = null): Unit =
      kafkaProducer.send(new ProducerRecord(topicName, partition, k, v)).get()
  }

  case class TestOutputTopic[K, V](
      topicName: String,
      numPartitions: Int,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ) {

    // create topic during construction of instance
    TestAdminClient().createTopic(this)

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
      TestAdminClient,
      SchemaRegistryClient,
      TestInputTopic[AnimalKey, AnimalValue],
      TestInputTopic[FoodKey, FoodValue],
      TestOutputTopic[OutputKey, OutputValue]
  ) => Assertion

  private def runKafkaTest(
      testFunction: testFn,
      preStartActions: (ZooAnimalFeederPipeline, TestAdminClient) => Unit = (_, _) => ()
  ): Unit = {
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

    val adminClient = TestAdminClient()

    try {
      kafkaStreams.cleanUp()
      preStartActions(factory, adminClient)
      kafkaStreams.start()
      testFunction(kafkaStreams, adminClient, schemaRegistryClient, animalsTopic, foodTopic, outputTopic)
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

  Feature("basic") {
    Scenario("1 animal created, 1 food parcel of 1 calorie arrives") {
      runKafkaTest { (stream, adminClient, schemaRegistryClient, animalTopic, foodTopic, outputTopic) =>
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

    Scenario("1 animal created, 2 food parcels of 1 calorie each arrives") {
      runKafkaTest { (stream, adminClient, schemaRegistryClient, animalTopic, foodTopic, outputTopic) =>
        val animalKey = AnimalKey(animalId1)
        val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
        animalTopic.pipeInput(animalKey, animalValue, partition = 1)

        // sleep to avoid situation where food goes to streams app before the animal exists in the state store
        Thread.sleep(5000)

        val foodKey = FoodKey(foodId1)
        val foodValue = FoodValue(foodId1, zooId1, calories1)
        foodTopic.pipeInput(foodKey, foodValue, partition = 1)
        foodTopic.pipeInput(foodKey, foodValue, partition = 1)

        val expected = Seq(
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1)),
          new KeyValue(OutputKey(foodId1), OutputValue(foodId1, zooId1, calories1, animalId1, calories1 * 2))
        )

        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
      }
    }
  }

  Feature("Pre-seed state store changelog topic with some state") {
    Scenario("1 animal with existing calorie fill, single partitioned topics") {
      val initialCalorieFill = 3

      runKafkaTest(
        (stream, adminClient, schemaRegistryClient, animalTopic, foodTopic, outputTopic) => {
          val animalKey = AnimalKey(animalId1)
          val animalValue = AnimalValue(animalId1, zooId1, maxCalories)
          animalTopic.pipeInput(animalKey, animalValue)

          // sleep to avoid situation where food goes to streams app before the animal exists in the state store
          Thread.sleep(5000)

          val foodKey = FoodKey(foodId1)
          val foodValue = FoodValue(foodId1, zooId1, calories1)
          foodTopic.pipeInput(foodKey, foodValue)

          val expected = Seq(
            new KeyValue(
              OutputKey(foodId1),
              OutputValue(foodId1, zooId1, calories1, animalId1, initialCalorieFill + calories1)
            )
          )

          outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
        },
        (factory, adminClient: TestAdminClient) => {
          val stateStoreTopic = "test-animalCaloriesCount-changelog"
          val changeLogTopic = TestInputTopic(
            stateStoreTopic,
            2,
            factory.animalKeySerde.serializer(),
            factory.animalCalorieFillSerde.serializer()
          )
          changeLogTopic.pipeInput(AnimalKey(animalId1), AnimalCalorieFill(initialCalorieFill))
        }
      )
    }

    Scenario("try to prove we need to partition by zooId") {
      val initialCalorieFill = 3

      val animalId = 4
      val animalKey = AnimalKey(animalId)

      runKafkaTest(
        (stream, adminClient, schemaRegistryClient, animalTopic, foodTopic, outputTopic) => {
          val animalValue = AnimalValue(animalId, zooId1, maxCalories)
          animalTopic.pipeInput(animalKey, animalValue)

          // sleep to avoid situation where food goes to streams app before the animal exists in the state store
          Thread.sleep(5000)

          val foodKey = FoodKey(foodId1)
          val foodValue = FoodValue(foodId1, zooId1, calories1)
          foodTopic.pipeInput(foodKey, foodValue)

          val expected = Seq(
            new KeyValue(
              OutputKey(foodId1),
              OutputValue(foodId1, zooId1, calories1, animalId, initialCalorieFill + calories1)
            )
          )

          outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
        },
        (factory, adminClient: TestAdminClient) => {
          val stateStoreTopic = "test-animalCaloriesCount-changelog"
          val changeLogTopic = TestInputTopic(
            stateStoreTopic,
            2,
            factory.animalKeySerde.serializer(),
            factory.animalCalorieFillSerde.serializer()
          )
          changeLogTopic.pipeInput(animalKey, AnimalCalorieFill(initialCalorieFill))
        }
      )
    }

    Scenario("1 animal with existing calorie fill and 1 new animal comes in") {
      ???
    }
  }

}
