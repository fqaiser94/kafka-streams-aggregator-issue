package com.fqaiser

import com.fqaiser.Serde.makeSerde
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.{KeyValue, Topology}
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor
import org.apache.kafka.streams.processor.{ProcessorContext, StreamPartitioner}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KTable, Materialized, Produced, Repartitioned}
import org.apache.kafka.streams.state.KeyValueStore


case class TopologyFactory(
                            animalsTopic: String,
                            foodTopic: String,
                            outputTopic: String,
                            schemaRegistryUrl: String,
                            schemaRegistryClient: SchemaRegistryClient
                          ) {

  private def serde[T <: SpecificRecord](isKey: Boolean): SpecificAvroSerde[T] =
    makeSerde[T](schemaRegistryUrl, schemaRegistryClient, isKey)

  val foodKeySerde = serde[FoodKey](isKey = true)
  val foodValueSerde = serde[FoodValue](isKey = false)

  val animalKeySerde = serde[AnimalKey](isKey = true)
  val animalValueSerde = serde[AnimalValue](isKey = false)

  val outputKeySerde = serde[ZooAnimalsKey](isKey = true)
  val outputValueSerde = serde[ZooAnimalsValue](isKey = false)

  // this is a simple topology using the Streams DSL
  def topology1: Topology = {
    val streamsBuilder = new StreamsBuilder()

    val animals = streamsBuilder.table(animalsTopic)(Consumed.`with`(animalKeySerde, animalValueSerde))
    val zooAnimals = animals
      .groupBy((k, v) => (ZooAnimalsKey(v.zooId), v))(Grouped.`with`(outputKeySerde, animalValueSerde))
      .aggregate(
        initializer = ZooAnimalsValue(List.empty))(
        adder = (k, v, agg) => agg.copy(animalValues = (agg.animalValues.toSet + v).toList),
        subtractor = (k, v, agg) => agg.copy(animalValues = (agg.animalValues.toSet - v).toList))(
        Materialized.`with`(outputKeySerde, outputValueSerde))

    zooAnimals.toStream.to(outputTopic)(Produced.`with`(outputKeySerde, outputValueSerde))

    streamsBuilder.build()
  }

  case class FoodEatenKey(zooId: Int)

  case class FoodEatenValue(foodId: Int, zooId: Int, animalId: Int)

  case class ZooAndAnimalId(zooId: Int, animalId: Int)

  // this is a simple topology using the Processor API
  def topology2: Topology = {
    val streamsBuilder = new StreamsBuilder()

    def makeZooIdPartitioner[K, V](extractZooIdFromKey: K => Int): StreamPartitioner[K, V] = {
      new StreamPartitioner[K, V] {
        override def partition(topic: String, key: K, value: V, numPartitions: Int): Integer = {
          // TODO: look at DefaultPartitioner for how to make this work safely?
          val zooId = extractZooIdFromKey(key)
          val zooIdByteArray = BigInt(zooId).toByteArray
          Utils.toPositive(Utils.murmur2(zooIdByteArray)) % numPartitions
        }
      }
    }

    val zooIdPartitioner: StreamPartitioner[ZooAndAnimalId, AnimalValue] = new StreamPartitioner[ZooAndAnimalId, AnimalValue] {
      override def partition(topic: String, key: ZooAndAnimalId, value: AnimalValue, numPartitions: Int): Integer = {
        key.zooId.asInstanceOf[Integer]
      }
    }

    // maybe write a custom partitioner?
    // which partitions the inputTopic by a field in value (i.e. zooId)
    // then the beacon stream can also be partitioned by zooId in there

    val food = streamsBuilder.stream(foodTopic)(Consumed.`with`(foodKeySerde, foodValueSerde))
      .selectKey((k, v) => v.zooId)
      .repartition(Repartitioned.`with`(partitioner = makeZooIdPartitioner[Int, FoodValue](zooId => zooId))(???, foodValueSerde))

    val zooAnimalTable: KTable[ZooAndAnimalId, AnimalValue] = {
      streamsBuilder.table(animalsTopic)(Consumed.`with`(animalKeySerde, animalValueSerde))
        // Has to be ZooAnimalId
        // Why not just ZooId?
        // Because then we'll have to collect all the animals into a set. This is likely to trigger kafka limits.
        .groupBy((k, v) => (ZooAndAnimalId(v.zooId, v.animalId), v))(???)
        .aggregate(???)(???, ???)(Materialized.`with`(???, ???))
        .toStream
        // Repartition so that all animals for a given zoo end up on a particular partition
        .repartition(Repartitioned.`with`(partitioner = makeZooIdPartitioner[ZooAndAnimalId, AnimalValue](_.zooId))(???, animalValueSerde))
        // TODO: dunno if this will work frankly, what if toTable repartitions again?
        //  Can maybe migitate this with groupByKey and a dummy aggretation: https://docs.confluent.io/platform/current/streams/faq.html#option-3-perform-a-dummy-aggregation
        //  Less likely, but what if it ignores the full key and only looks at zooId?
        .toTable()
    }

    // partition this by zooId?
    // note, only partition, no need to change the key from animalId?
    // doesn't work, food and animals need to have the same key according to the join operator
    // but maybe we don't need to use the join operator at all? go into a processor node and access the underlying
    // database directly?
    // okay how about key by zooId and animalId
    // then partition by only zooId
    // and then in my transformer, I can look up only entries with the correct zooId in the key?

    // Maybe this will work, maybe not. Might have to materialize it manually.
    val zooAnimalStateStoreName: String = zooAnimalTable.queryableStoreName
    val stateStoreNames: Seq[String] = Seq(zooAnimalStateStoreName)

    // I think this can just be a lambda
    val transformerSupplier = new TransformerSupplier[Int, FoodValue, KeyValue[Int, FoodEatenValue]] {
      private def transformer(): Transformer[Int, FoodValue, KeyValue[Int, FoodEatenValue]] = new Transformer[Int, FoodValue, KeyValue[Int, FoodEatenValue]] {
        var zooAnimalStateStore: KeyValueStore[ZooAndAnimalId, AnimalValue] = _
        // TODO: create another state store for animalCalorieFill?

        override def init(context: ProcessorContext): Unit = {
          zooAnimalStateStore = context.getStateStore(zooAnimalStateStoreName).asInstanceOf[KeyValueStore[ZooAndAnimalId, AnimalValue]]
        }

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

        override def transform(key: Int, value: FoodValue): KeyValue[Int, FoodEatenValue] = {
          val zooId = value.zooId
          val zooIdAnimals = getAnimals(zooId)
          // TODO: add filling logic later
          // for now just return all the animals the food could feed
          new KeyValue(key, FoodEatenValue(value.foodId, value.zooId, zooIdAnimals.map(_.animalId)))
        }

        override def close(): Unit = {
          Unit
        }
      }

      override def get(): Transformer[Int, FoodValue, KeyValue[Int, FoodEatenValue]] = {
        transformer()
      }
    }

    val output = food.transform(transformerSupplier, stateStoreNames: _*)
    output.to(outputTopic)(Produced.`with`(outputKeySerde, outputValueSerde))

    streamsBuilder.build()
  }
}