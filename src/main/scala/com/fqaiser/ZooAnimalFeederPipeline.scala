package com.fqaiser

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.kstream.{Transformer, ValueTransformer}
import org.apache.kafka.streams.processor.{ProcessorContext, StreamPartitioner}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import org.apache.kafka.streams.{KeyValue, Topology}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class ZooAnimalFeederPipeline(
    animalsTopicName: String,
    foodTopicName: String,
    outputTopicName: String,
    schemaRegistryUrl: String,
    schemaRegistryClient: SchemaRegistryClient
) {

  private def specificRecordSerde[T <: SpecificRecord](isKey: Boolean): SpecificAvroSerde[T] =
    SerdeUtils.specificRecordSerde[T](schemaRegistryUrl, schemaRegistryClient, isKey)

  val foodKeySerde: SpecificAvroSerde[FoodKey] = specificRecordSerde[FoodKey](isKey = true)
  val foodValueSerde: SpecificAvroSerde[FoodValue] = specificRecordSerde[FoodValue](isKey = false)

  val animalKeySerde: SpecificAvroSerde[AnimalKey] = specificRecordSerde[AnimalKey](isKey = true)
  val animalValueSerde: SpecificAvroSerde[AnimalValue] =
    specificRecordSerde[AnimalValue](isKey = false)

  val outputKeySerde: SpecificAvroSerde[OutputKey] = specificRecordSerde[OutputKey](isKey = true)
  val outputValueSerde: SpecificAvroSerde[OutputValue] = specificRecordSerde[OutputValue](isKey = false)

  private case class ZooIdAnimalId(zooId: Int, animalId: Int)
  private val zooIdAnimalIdSerde =
    SerdeUtils.ccSerde[ZooIdAnimalId](schemaRegistryUrl, schemaRegistryClient, isKey = true)

  private case class ZooId(zooId: Int)
  private val zooIdSerde =
    SerdeUtils.ccSerde[ZooId](schemaRegistryUrl, schemaRegistryClient, isKey = true)

  def topology: Topology = {
    val streamsBuilder = new StreamsBuilder()

    val food: KStream[ZooId, FoodValue] = streamsBuilder
      .stream(foodTopicName)(Consumed.`with`(foodKeySerde, foodValueSerde))
      .selectKey((k, v) => ZooId(v.zooId))
      .repartition(
        Repartitioned.`with`(partitioner = makeZooIdPartitioner[ZooId, FoodValue](_.zooId))(
          zooIdSerde,
          foodValueSerde
        )
      )

    val zooAnimalsTable: KTable[ZooIdAnimalId, AnimalValue] =
      streamsBuilder
        .stream(animalsTopicName)(Consumed.`with`(animalKeySerde, animalValueSerde))
        .selectKey((k, v) => ZooIdAnimalId(v.zooId, v.animalId))
        // Repartition so that all animals for a given zoo end up on a particular partition
        .repartition(
          Repartitioned.`with`(makeZooIdPartitioner[ZooIdAnimalId, AnimalValue](_.zooId))(
            zooIdAnimalIdSerde,
            animalValueSerde
          )
        )
        .toTable(Materialized.as("zooAnimals")(zooIdAnimalIdSerde, animalValueSerde))

    val zooAnimalStateStoreName: String = zooAnimalsTable.queryableStoreName
    val output: KStream[OutputKey, OutputValue] = food
      .transformValues(
        () => animalFeederValueTransformer(zooAnimalStateStoreName),
        zooAnimalStateStoreName)
      .selectKey((k, v) => OutputKey(v.foodId))

    output.to(outputTopicName)(Produced.`with`(outputKeySerde, outputValueSerde))

    streamsBuilder.build()
  }

  private def makeZooIdPartitioner[K, V](extractZooIdFromKey: K => Int): StreamPartitioner[K, V] =
    (topic: String, key: K, value: V, numPartitions: Int) => {
      // TODO: look at DefaultPartitioner for how to make this work safely?
      val zooId = extractZooIdFromKey(key)
      val zooIdByteArray = BigInt(zooId).toByteArray
      Utils.toPositive(Utils.murmur2(zooIdByteArray)) % numPartitions
    }

  private def animalFeederValueTransformer(
      zooAnimalStateStoreName: String
  ): ValueTransformer[FoodValue, OutputValue] =
    new ValueTransformer[FoodValue, OutputValue] {
      var zooAnimalStateStore: TimestampedKeyValueStore[ZooIdAnimalId, AnimalValue] = _
      // TODO: create another state store for animalCalorieFill?

      override def init(context: ProcessorContext): Unit = {
        zooAnimalStateStore = context
          .getStateStore(zooAnimalStateStoreName)
          .asInstanceOf[TimestampedKeyValueStore[ZooIdAnimalId, AnimalValue]]
      }

      override def transform(value: FoodValue): OutputValue = {
        val zooIdAnimals = getAnimals(value.zooId)
        OutputValue(value.foodId, value.zooId, value.calories, zooIdAnimals.map(_.animalId).head)
      }

      override def close(): Unit = {}

      private def getAnimals(zooId: Int): ArrayBuffer[AnimalValue] = {
        var zooIdAnimals = ArrayBuffer.empty[AnimalValue]
        val iterator = zooAnimalStateStore.all()
        while (iterator.hasNext) {
          val timestampedKeyValue = iterator.next()
          if (timestampedKeyValue.key.zooId == zooId)
            zooIdAnimals += timestampedKeyValue.value.value()
        }
        iterator.close()
        zooIdAnimals
      }
    }

}
