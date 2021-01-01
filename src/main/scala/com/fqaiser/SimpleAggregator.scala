package com.fqaiser

import com.fqaiser.Serde.makeSerde
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}

case class SimpleAggregator(
    animalsTopic: String,
    outputTopic: String,
    schemaRegistryUrl: String,
    schemaRegistryClient: SchemaRegistryClient
) {

  private def serde[T <: SpecificRecord](isKey: Boolean): SpecificAvroSerde[T] =
    makeSerde[T](schemaRegistryUrl, schemaRegistryClient, isKey)

  val foodKeySerde = serde[FoodKey](isKey = true)
  val foodValueSerde = serde[FoodValue](isKey = false)

  val animalKeySerde = serde[AnimalKey](isKey = true)
  val animalValueSerde = serde[AnimalValue](isKey = false)

  val outputKeySerde = serde[ZooAnimalsKey](isKey = true)
  val outputValueSerde = serde[ZooAnimalsValue](isKey = false)

  /**
    * Simple topology using the Streams DSL
    */
  def topology: Topology = {
    val streamsBuilder = new StreamsBuilder()

    val animals = streamsBuilder.table(animalsTopic)(
      Consumed.`with`(animalKeySerde, animalValueSerde)
    )
    val zooAnimals = animals
      .groupBy((k, v) => (ZooAnimalsKey(v.zooId), v))(
        Grouped.`with`(outputKeySerde, animalValueSerde)
      )
      .aggregate(initializer = ZooAnimalsValue(List.empty))(
        adder = (k, v, agg) => agg.copy(animalValues = (agg.animalValues.toSet + v).toList),
        subtractor = (k, v, agg) => agg.copy(animalValues = (agg.animalValues.toSet - v).toList)
      )(Materialized.`with`(outputKeySerde, outputValueSerde))

    zooAnimals.toStream.to(outputTopic)(
      Produced.`with`(outputKeySerde, outputValueSerde)
    )

    streamsBuilder.build()
  }
}
