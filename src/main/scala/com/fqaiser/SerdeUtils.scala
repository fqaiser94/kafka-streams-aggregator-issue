package com.fqaiser

import com.sksamuel.avro4s._
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
    new CaseClassSerde[CC](schemaRegistryUrl, schemaRegistryClient, isKey)
  }

  private class CaseClassDeserializer[CC: RecordFormat](
      schemaRegistryUrl: String,
      schemaRegistryClient: SchemaRegistryClient,
      isKey: Boolean
  ) extends Deserializer[CC] {

    private val deserializer = new KafkaAvroDeserializer(schemaRegistryClient)
    deserializer.configure(
      singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
      isKey
    )

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = deserializer.close()

    override def deserialize(topic: String, data: Array[Byte]): CC = {
      if (data == null)
        null.asInstanceOf[CC]
      else {
        val recordFormat = implicitly[RecordFormat[CC]]
        val record = deserializer.deserialize(topic, data).asInstanceOf[GenericRecord]
        recordFormat.from(record)
      }
    }
  }

  private class CaseClassSerializer[CC: RecordFormat](
      schemaRegistryUrl: String,
      schemaRegistryClient: SchemaRegistryClient,
      isKey: Boolean
  ) extends Serializer[CC] {

    private val serializer = new KafkaAvroSerializer(schemaRegistryClient)
    serializer.configure(
      singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
      isKey
    )

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = serializer.close()

    override def serialize(topic: String, data: CC): Array[Byte] = {
      val recordFormat = implicitly[RecordFormat[CC]]
      val record = recordFormat.to(data)
      serializer.serialize(topic, record)
    }
  }

  private class CaseClassSerde[CC: RecordFormat](
      schemaRegistryUrl: String,
      schemaRegistryClient: SchemaRegistryClient,
      isKey: Boolean
  ) extends Serde[CC] {

    override def deserializer(): Deserializer[CC] =
      new CaseClassDeserializer(schemaRegistryUrl, schemaRegistryClient, isKey)

    override def serializer(): Serializer[CC] =
      new CaseClassSerializer(schemaRegistryUrl, schemaRegistryClient, isKey)

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}
  }

}
