package com.fqaiser

import com.fqaiser.serde.SerdeUtils
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.Collections
import scala.jdk.CollectionConverters.IterableHasAsScala

class GetSizeTest extends AnyFunSuite with Matchers with EmbeddedKafka {

  private val kafkaConfig = EmbeddedKafkaConfig()
  private val schemaRegistryUrl = s"http://localhost:${kafkaConfig.schemaRegistryPort}"

  private val defaultString = "asdkansdkanknsd"
  private val defaultListLong = (1 to 10).toList.map(_.toLong)
  private case class Key(str1: String = defaultString)
  private case class Value(
      id: Int = 1,
      fkId1: Int = 2,
      fkId2: Int = 3,
      fkId3: Int = 4,
      str1: String = defaultString,
      str2: String = defaultString,
      str3: String = defaultString,
      str4: String = defaultString,
      str5: String = defaultString,
      str6: String = defaultString,
      frs: List[Long] = defaultListLong,
      fts: List[Long] = defaultListLong
  )
  private case class ListOfValue(values: List[Value])

  private val topicName = "HelloTopic"

  test("Hello") {
    withRunningKafka {
      val keySerde = SerdeUtils.ccSerde[Key](schemaRegistryUrl, null, isKey = true)
      val valueSerde = SerdeUtils.ccSerde[Value](schemaRegistryUrl, null, isKey = false)

      publishToKafka(topicName, Key(), Value())(
        kafkaConfig,
        keySerde.serializer(),
        valueSerde.serializer()
      )
      withConsumer[Key, Value, Unit] { consumer =>
        consumer.subscribe(Collections.singletonList(topicName))
        val records = consumer.poll(10000)
        val size = records.asScala.toList.map(x => x.serializedKeySize() + x.serializedValueSize()).head
        println(s"size: $size")
      }(kafkaConfig, keySerde.deserializer(), valueSerde.deserializer())
    }
  }

  test("kafka breaks when record is too big") {
    // Based on the previous result of 150 bytes, 6666 values should break kafka
    withRunningKafka {
      val keySerde = SerdeUtils.ccSerde[Key](schemaRegistryUrl, null, isKey = true)
      val listOfValueSerde = SerdeUtils.ccSerde[ListOfValue](schemaRegistryUrl, null, isKey = false)

      val values = (1 to 10000).map(_ => Value()).toList

      an[org.apache.kafka.common.errors.RecordTooLargeException] should be thrownBy {
        throw intercept[net.manub.embeddedkafka.KafkaUnavailableException] {
          publishToKafka(topicName, Key(), ListOfValue(values))(
            kafkaConfig,
            keySerde.serializer(),
            listOfValueSerde.serializer()
          )
        }.getCause.getCause
      }

      withConsumer[Key, ListOfValue, Unit] { consumer =>
        consumer.subscribe(Collections.singletonList(topicName))
        val records = consumer.poll(10000)
        records.count() shouldEqual (0)
      }(kafkaConfig, keySerde.deserializer(), listOfValueSerde.deserializer())
    }
  }

  test("no problems when record is under 1 MB") {
    withRunningKafka {
      val keySerde = SerdeUtils.ccSerde[Key](schemaRegistryUrl, null, isKey = true)
      val listOfValueSerde = SerdeUtils.ccSerde[ListOfValue](schemaRegistryUrl, null, isKey = false)

      val values = (1 to 5000).map(_ => Value()).toList

      publishToKafka(topicName, Key(), ListOfValue(values))(
        kafkaConfig,
        keySerde.serializer(),
        listOfValueSerde.serializer()
      )

      withConsumer[Key, ListOfValue, Unit] { consumer =>
        consumer.subscribe(Collections.singletonList(topicName))
        val records = consumer.poll(10000).asScala.toList
        val size = records.map(x => x.serializedKeySize() + x.serializedValueSize()).head
        println(s"size: $size")
        records.map(x => (x.key(), x.value())) should contain theSameElementsAs Seq((Key(), ListOfValue(values)))
      }(kafkaConfig, keySerde.deserializer(), listOfValueSerde.deserializer())
    }
  }

}
