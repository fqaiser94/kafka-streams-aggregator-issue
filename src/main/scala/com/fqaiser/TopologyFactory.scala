package com.fqaiser

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}

case class TopologyFactory(inputTopic: String, outputTopic: String, schemaRegistryClient: SchemaRegistryClient) {

  val inputKeySerde = new SpecificAvroSerde[AnimalKey](schemaRegistryClient)
  val inputValueSerde = new SpecificAvroSerde[AnimalValue](schemaRegistryClient)

  val outputKeySerde = new SpecificAvroSerde[ZooAnimalsKey](schemaRegistryClient)
  val outputValueSerde = new SpecificAvroSerde[ZooAnimalsValue](schemaRegistryClient)

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