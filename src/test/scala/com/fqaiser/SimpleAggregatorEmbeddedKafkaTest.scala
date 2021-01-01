//package com.fqaiser
//
//import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
//import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaConfig
//import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
//import org.apache.kafka.clients.consumer.KafkaConsumer
//import org.apache.kafka.clients.producer.KafkaProducer
//import org.apache.kafka.streams.{
//  KafkaStreams,
//  KeyValue,
//  StreamsConfig,
//  TestInputTopic,
//  TestOutputTopic
//}
//import org.scalatest.compatible.Assertion
//import org.scalatest.featurespec.AnyFeatureSpec
//import org.scalatest.matchers.should.Matchers
//
//import java.util.Properties
//
//class SimpleAggregatorEmbeddedKafkaTest
//    extends AnyFeatureSpec
//    with Matchers
//    with EmbeddedKafkaStreams {
//
//  private type testFn = (
//      // The application
//      KafkaStreams,
//      // Input topic producer
//      KafkaProducer[AnimalKey, AnimalValue],
//      // Output topic consumer
//      KafkaConsumer[ZooAnimalsKey, ZooAnimalsValue]
//  ) => Assertion
//
//  private def runTest(testFunction: testFn): Unit = {
//    val inputTopicName = "inputTopic"
//    val outputTopicName = "outputTopic"
//
//    implicit val embeddedKafkaConf: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
//
//    val schemaRegistryUrl: String = s"localhost:${embeddedKafkaConf.schemaRegistryPort}"
//    val schemaRegistryClient: SchemaRegistryClient =
//      new CachedSchemaRegistryClient(schemaRegistryUrl, 10)
//    val factory = SimpleAggregator(
//      inputTopicName,
//      outputTopicName,
//      schemaRegistryUrl,
//      schemaRegistryClient
//    )
//
//    val props = new Properties()
//    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
//    val bootstrapServers = s"localhost:${embeddedKafkaConf.kafkaPort}"
//    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
//
//    withRunningKafka {
//      val kstream = new KafkaStreams(factory.topology, props)
//
//      createCustomTopic(inputTopicName)
//      val inputTopicProducer = new KafkaProducer[AnimalKey, AnimalValue](
//        new Properties(),
//        factory.animalKeySerde.serializer,
//        factory.animalValueSerde.serializer
//      )
//
//      createCustomTopic(outputTopicName)
//      val outputTopicConsumer = new KafkaConsumer[ZooAnimalsKey, ZooAnimalsValue](
//        new Properties(),
//        factory.outputKeySerde.deserializer(),
//        factory.outputValueSerde.deserializer()
//      )
//
//      try {
//        testFunction(kstream, inputTopicProducer, outputTopicConsumer)
//      } finally {
//        kstream.close()
//      }
//    }
//  }
//
//  private def outputTopicShouldContainTheSameElementsAs[K, V](
//      outputTopic: KafkaConsumer[K, V],
//      expected: Seq[KeyValue[K, V]]
//  ): Assertion = {
//    val result = outputTopic.readKeyValuesToList().asScala.toList
//    println("result")
//    result.foreach(println)
//    println("expected")
//    expected.foreach(println)
//    result should contain theSameElementsInOrderAs expected
//  }
//
//  Feature("") {
//    Scenario("") {
//      runTest { (stream, inputProducer, outputConsumer) =>
//        val animalKey = AnimalKey(animalId1)
//        val animalValue = AnimalValue(animalId1, zooId, maxCalories)
//        inputTopic.pipeInput(animalKey, animalValue)
//
//        val zooAnimalsKey = ZooAnimalsKey(zooId)
//        val zooAnimalsValue = ZooAnimalsValue(List(animalValue))
//        val expected = Seq(
//          new KeyValue(zooAnimalsKey, zooAnimalsValue)
//        )
//
//        outputTopicShouldContainTheSameElementsAs(outputTopic, expected)
//      }
//    }
//  }
//
//}
