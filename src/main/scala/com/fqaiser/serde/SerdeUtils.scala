package com.fqaiser.serde

import com.sksamuel.avro4s.{Decoder, Encoder, RecordFormat}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.util.Collections.singletonMap

object SerdeUtils {

  // TODO: unit test
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

  // TODO: unit test
  def ccSerde[CC: Encoder: Decoder](
      schemaRegistryUrl: String,
      schemaRegistryClient: SchemaRegistryClient,
      isKey: Boolean
  ): Serde[CC] = {
    implicit val recordFormat: RecordFormat[CC] = RecordFormat[CC]
    CaseClassSerde[CC](schemaRegistryUrl, schemaRegistryClient, isKey)
  }

}
