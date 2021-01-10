package com.fqaiser

import com.fqaiser.serde.SerdeUtils
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.Stores

import java.util

case class ZooAnimalFeederPipeline(
    animalsTopicName: String,
    foodTopicName: String,
    processedFoodTopicName: String,
    animalStatusTopicName: String,
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

  val processedFoodKeySerde: SpecificAvroSerde[OutputKey] = specificRecordSerde[OutputKey](isKey = true)
  val processedFoodValueSerde: SpecificAvroSerde[OutputValue] = specificRecordSerde[OutputValue](isKey = false)

  val animalCalorieFillSerde: SpecificAvroSerde[AnimalCalorieFill] =
    specificRecordSerde[AnimalCalorieFill](isKey = false)

  val zooIdAnimalIdSerde: Serde[ZooIdAnimalId] =
    SerdeUtils.ccSerde[ZooIdAnimalId](schemaRegistryUrl, schemaRegistryClient, isKey = true)

  private case class ZooId(zooId: Int)
  private val zooIdSerde =
    SerdeUtils.ccSerde[ZooId](schemaRegistryUrl, schemaRegistryClient, isKey = true)

  val animalStatusKeySerde = specificRecordSerde[AnimalStatusKey](isKey = true)
  val animalStatusValueSerde = specificRecordSerde[AnimalCalorieFill](isKey = false)

  def topology: Topology = {
    val streamsBuilder = new StreamsBuilder()

    val food: KStream[ZooId, FoodValue] = streamsBuilder
      .stream(foodTopicName)(Consumed.`with`(foodKeySerde, foodValueSerde))
      .selectKey((k, v) => ZooId(v.zooId))
      .repartition(
        Repartitioned.`with`(partitioner = ZooIdPartitioner[ZooId, FoodValue](_.zooId))(
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
          Repartitioned.`with`(ZooIdPartitioner[ZooIdAnimalId, AnimalValue](_.zooId))(
            zooIdAnimalIdSerde,
            animalValueSerde
          )
        )
        .toTable(Materialized.as("zooAnimals")(zooIdAnimalIdSerde, animalValueSerde))

    val animalCaloriesCountStoreName: String = "animalCaloriesCount"
    val animalCaloriesCountStoreBuilder = Stores
      .timestampedKeyValueStoreBuilder(
        Stores.persistentTimestampedKeyValueStore(animalCaloriesCountStoreName),
        // We use `zooIdAnimalId` instead of just `animalId` as the key
        // The reason for this is b/c we want to partition the state store by zooId Int
        // This enables us to easily pre-populate the state store with only (animal) data on each partition for
        // the relevant zooIds.
        zooIdAnimalIdSerde,
        animalCalorieFillSerde
      )
      .withLoggingEnabled(util.Collections.singletonMap("cleanup.policy", "compact"))
      .withCachingEnabled()

    streamsBuilder.addStateStore(animalCaloriesCountStoreBuilder)

    val processed = food
      .transformValues(
        () => AnimalFeederValueTransformer(zooAnimalsTable.queryableStoreName, animalCaloriesCountStoreName),
        zooAnimalsTable.queryableStoreName,
        animalCaloriesCountStoreName
      )

    processed
      .selectKey((k, v) => OutputKey(v.outputValue.foodId))
      .mapValues((k, v) => v.outputValue)
      .to(processedFoodTopicName)(Produced.`with`(processedFoodKeySerde, processedFoodValueSerde))

    processed
      .filter((k, v) => v.stateStoreValueIfUpdated.isDefined)
      .mapValues(_.stateStoreValueIfUpdated.get)
      .map((k, v) => (AnimalStatusKey(v._1.zooId, v._1.animalId), v._2))
      .to(animalStatusTopicName)(Produced.`with`(animalStatusKeySerde, animalStatusValueSerde))

    streamsBuilder.build()
  }
}
