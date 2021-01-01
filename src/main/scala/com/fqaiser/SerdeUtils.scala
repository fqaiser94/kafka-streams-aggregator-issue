package com.fqaiser

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde

import java.util.Collections.singletonMap

object SerdeUtils {

  def specificRecordSerde[T <: SpecificRecord](
      schemaRegistryUrl: String,
      schemaRegistryClient: SchemaRegistryClient,
      isKey: Boolean
  ): SpecificAvroSerde[T] = {
    val serde = new SpecificAvroSerde[T](schemaRegistryClient)
    serde.configure(
      singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
      isKey
    )
    serde
  }

  def ccSerde[T <: Product](
      schemaRegistryUrl: String,
      schemaRegistryClient: SchemaRegistryClient,
      isKey: Boolean
  ): Serde[T] = ???

}
