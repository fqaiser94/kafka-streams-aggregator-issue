package com.fqaiser.serde

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

final case class CaseClassSerde[CC: RecordFormat](
    schemaRegistryUrl: String,
    schemaRegistryClient: SchemaRegistryClient,
    isKey: Boolean
) extends Serde[CC] {

  override def deserializer(): Deserializer[CC] =
    CaseClassDeserializer(schemaRegistryUrl, schemaRegistryClient, isKey)

  override def serializer(): Serializer[CC] =
    CaseClassSerializer(schemaRegistryUrl, schemaRegistryClient, isKey)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}
