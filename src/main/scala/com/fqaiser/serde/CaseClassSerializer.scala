package com.fqaiser.serde

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import org.apache.kafka.common.serialization.Serializer

import java.util.Collections.singletonMap

final case class CaseClassSerializer[CC: RecordFormat](
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

  // TODO: can't serialize null data. not good.
  override def serialize(topic: String, data: CC): Array[Byte] = {
    val recordFormat = implicitly[RecordFormat[CC]]
    val record = recordFormat.to(data)
    serializer.serialize(topic, record)
  }
}
