package com.fqaiser

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, StreamPartitioner}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, TimestampedKeyValueStore, ValueAndTimestamp}
import org.apache.kafka.streams.{KeyValue, Topology}

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
      .transform(
        () => animalFeederTransformer(zooAnimalStateStoreName),
        zooAnimalStateStoreName
      )
      .peek((k, v) => println("********* got some output"))

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

  private def animalFeederTransformer(
      zooAnimalStateStoreName: String
  ): Transformer[ZooId, FoodValue, KeyValue[OutputKey, OutputValue]] =
    new Transformer[ZooId, FoodValue, KeyValue[OutputKey, OutputValue]] {
      var zooAnimalStateStore: TimestampedKeyValueStore[ZooIdAnimalId, AnimalValue] = _
      // TODO: create another state store for animalCalorieFill?

      override def init(context: ProcessorContext): Unit = {
        println("********* transformer init")
        zooAnimalStateStore = context
          .getStateStore(zooAnimalStateStoreName)
          .asInstanceOf[TimestampedKeyValueStore[ZooIdAnimalId, AnimalValue]]
      }

      override def transform(
          key: ZooId,
          value: FoodValue
      ): KeyValue[OutputKey, OutputValue] = {
        println("********* transformer transform")
        val zooIdAnimals = getAnimals(value.zooId)
        val result = new KeyValue(
          OutputKey(value.foodId),
          OutputValue(value.foodId, value.zooId, value.calories, zooIdAnimals.map(_.animalId).head)
        )
        println(s"********* transformer transform result: $result")
        result
      }

      override def close(): Unit = {}

      private def getAnimals(zooId: Int): Seq[AnimalValue] = {
        var zooIdAnimals = Seq.empty[AnimalValue]
        val iterator = zooAnimalStateStore.all()
        while (iterator.hasNext) {
          val curr = iterator.next()
          if (curr.key.zooId == zooId)
            zooIdAnimals = zooIdAnimals :+ curr.value.value()
        }
        iterator.close()
        println(s"********* zooIdAnimals: $zooIdAnimals")
        zooIdAnimals
      }
    }

}
