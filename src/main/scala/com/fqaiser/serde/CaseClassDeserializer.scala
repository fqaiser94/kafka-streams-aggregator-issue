package com.fqaiser.serde

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Deserializer

import java.util.Collections.singletonMap

final case class CaseClassDeserializer[CC: RecordFormat](
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
