package com.fqaiser

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, StreamPartitioner}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{KeyValue, Topology}

case class ZooAnimalFeederPipeline(
    animalsTopic: String,
    foodTopic: String,
    outputTopic: String,
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

  def topology: Topology = {
    val streamsBuilder = new StreamsBuilder()

    val food = streamsBuilder
      .stream(foodTopic)(Consumed.`with`(foodKeySerde, foodValueSerde))
      .selectKey((k, v) => v.zooId)
      .repartition(
        Repartitioned.`with`(partitioner = makeZooIdPartitioner[Int, FoodValue](zooId => zooId))(
          Serdes.Integer,
          foodValueSerde
        )
      )

    val zooAnimalTable: KTable[ZooIdAnimalId, AnimalValue] =
      streamsBuilder
        .stream(animalsTopic)(Consumed.`with`(animalKeySerde, animalValueSerde))
        .selectKey((k, v) => ZooIdAnimalId(v.zooId, v.animalId))
        // Repartition so that all animals for a given zoo end up on a particular partition
        .repartition(
          Repartitioned.`with`(makeZooIdPartitioner[ZooIdAnimalId, AnimalValue](_.zooId))(
            zooIdAnimalIdSerde,
            animalValueSerde
          )
        )
        .toTable

    val zooAnimalStateStoreName: String = zooAnimalTable.queryableStoreName
    val output: KStream[OutputKey, OutputValue] = food.transform(
      () => animalFeederTransformer(zooAnimalStateStoreName),
      zooAnimalStateStoreName
    )

    output.to(outputTopic)(Produced.`with`(outputKeySerde, outputValueSerde))

    streamsBuilder.build()
  }

  private def makeZooIdPartitioner[K, V](extractZooIdFromKey: K => Int): StreamPartitioner[K, V] =
    (topic: String, key: K, value: V, numPartitions: Int) => {
      // TODO: look at DefaultPartitioner for how to make this work safely?
      val zooId = extractZooIdFromKey(key)
      val zooIdByteArray = BigInt(zooId).toByteArray
      Utils.toPositive(Utils.murmur2(zooIdByteArray)) % numPartitions
    }

  private def animalFeederTransformer(
      zooAnimalStateStoreName: String
  ): Transformer[Int, FoodValue, KeyValue[OutputKey, OutputValue]] =
    new Transformer[Int, FoodValue, KeyValue[OutputKey, OutputValue]] {
      var zooAnimalStateStore: KeyValueStore[ZooIdAnimalId, AnimalValue] = _
      // TODO: create another state store for animalCalorieFill?

      override def init(context: ProcessorContext): Unit = {
        zooAnimalStateStore = context
          .getStateStore(zooAnimalStateStoreName)
          .asInstanceOf[KeyValueStore[ZooIdAnimalId, AnimalValue]]
      }

      override def transform(
          key: Int,
          value: FoodValue
      ): KeyValue[OutputKey, OutputValue] = {
        val zooId = value.zooId
        val zooIdAnimals = getAnimals(zooId)
        // TOOD: for now just feed one animal
        new KeyValue(
          OutputKey(value.foodId),
          OutputValue(value.foodId, value.zooId, value.calories, zooIdAnimals.map(_.animalId).head)
        )
      }

      override def close(): Unit = {}

      private def getAnimals(zooId: Int): Seq[AnimalValue] = {
        var zooIdAnimals = Seq.empty[AnimalValue]
        val iterator = zooAnimalStateStore.all()
        while (iterator.hasNext) {
          val curr = iterator.next()
          if (curr.key.zooId == zooId) {
            zooIdAnimals = zooIdAnimals :+ curr.value
          }
        }
        iterator.close()
        zooIdAnimals
      }
    }

}
