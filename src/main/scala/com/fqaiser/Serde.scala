package com.fqaiser

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord

import scala.jdk.CollectionConverters.MapHasAsJava

object Serde {

  def makeSerde[T <: SpecificRecord](
      schemaRegistryUrl: String,
      schemaRegistryClient: SchemaRegistryClient,
      isKey: Boolean
  ): SpecificAvroSerde[T] = {

    val serde = new SpecificAvroSerde[T](schemaRegistryClient)
    val serdeProps = Map(
      (
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryUrl
      )
    ).asJava
    serde.configure(serdeProps, isKey)
    serde
  }

}
