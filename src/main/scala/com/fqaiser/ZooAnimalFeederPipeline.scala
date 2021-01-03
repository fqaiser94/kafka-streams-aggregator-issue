package com.fqaiser

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.Stores

import java.util

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

  private val zooIdAnimalIdSerde =
    SerdeUtils.ccSerde[ZooIdAnimalId](schemaRegistryUrl, schemaRegistryClient, isKey = true)

  val animalCalorieFillSerde =
    SerdeUtils.ccSerde[AnimalCalorieFill](schemaRegistryUrl, schemaRegistryClient, isKey = false)

  private case class ZooId(zooId: Int)
  private val zooIdSerde =
    SerdeUtils.ccSerde[ZooId](schemaRegistryUrl, schemaRegistryClient, isKey = true)

  def topology: Topology = {
    val streamsBuilder = new StreamsBuilder()

    val food: KStream[ZooId, FoodValue] = streamsBuilder
      .stream(foodTopicName)(Consumed.`with`(foodKeySerde, foodValueSerde))
      .peek((k, v) => println(s"************ receivedFood: ${(k, v)}"))
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
        .peek((k, v) => println(s"************ animal: ${(k, v)}"))
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
      .keyValueStoreBuilder(
        Stores.persistentTimestampedKeyValueStore(animalCaloriesCountStoreName),
        animalKeySerde,
        animalCalorieFillSerde
      )
      .withLoggingEnabled(util.Collections.singletonMap("cleanup.policy", "compact"))
      .withCachingEnabled()

    streamsBuilder.addStateStore(animalCaloriesCountStoreBuilder)

    // TODO: need to pass in animalCaloriesCount state store name?
    val zooAnimalStateStoreName: String = zooAnimalsTable.queryableStoreName
    val output: KStream[OutputKey, OutputValue] = food
      .transformValues(
        () => AnimalFeederValueTransformer(zooAnimalStateStoreName, animalCaloriesCountStoreName),
        zooAnimalStateStoreName,
        animalCaloriesCountStoreName
      )
      .selectKey((k, v) => OutputKey(v.foodId))
      .peek((k, v) => println(s"************ some animal consumed some food: ${(k, v)}"))

    output.to(outputTopicName)(Produced.`with`(outputKeySerde, outputValueSerde))

    streamsBuilder.build()
  }
}
