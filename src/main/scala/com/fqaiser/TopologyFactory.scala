package com.fqaiser

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}

import scala.jdk.CollectionConverters.MapHasAsJava


case class TopologyFactory(inputTopic: String, outputTopic: String, schemaRegistryClient: SchemaRegistryClient) {

  val serdeProps = Map(
   (AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock")
  ).asJava

  val inputKeySerde = new SpecificAvroSerde[AnimalKey](schemaRegistryClient)
  inputKeySerde.configure(serdeProps, true)
  val inputValueSerde = new SpecificAvroSerde[AnimalValue](schemaRegistryClient)
  inputValueSerde.configure(serdeProps, true)

  val outputKeySerde = new SpecificAvroSerde[ZooAnimalsKey](schemaRegistryClient)
  outputKeySerde.configure(serdeProps, true)
  val outputValueSerde = new SpecificAvroSerde[ZooAnimalsValue](schemaRegistryClient)
  outputValueSerde.configure(serdeProps, true)

  def topology: Topology = {
    val streamsBuilder = new StreamsBuilder()

    val animals = streamsBuilder.table(inputTopic)(Consumed.`with`(inputKeySerde, inputValueSerde))
    val zooAnimals = animals
      .groupBy((k, v) => (ZooAnimalsKey(v.zooId), v))(Grouped.`with`(outputKeySerde, inputValueSerde))
      .aggregate(
        initializer = ZooAnimalsValue(List.empty))(
        adder = (k, v, agg) => agg.copy(animalValues = agg.animalValues.appended(v)),
        subtractor = (k, v, agg) => agg.copy(animalValues = agg.animalValues.dropWhile(_ == v)))(
        Materialized.`with`(outputKeySerde, outputValueSerde))

    zooAnimals.toStream.to(outputTopic)(Produced.`with`(outputKeySerde, outputValueSerde))

    streamsBuilder.build()
  }
}