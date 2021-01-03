package com.fqaiser

import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.processor.StreamPartitioner

/**
  *
  * @param zooIdExtractor: A function to extract zooId from the key.
  * @tparam K: type of the key of messages
  * @tparam V: type of the value of messages
  */
case class ZooIdPartitioner[K, V](zooIdExtractor: K => Int) extends StreamPartitioner[K, V] {
  override def partition(topic: String, key: K, value: V, numPartitions: Int): Integer = {
    val zooId = zooIdExtractor(key)
    val zooIdByteArray = BigInt(zooId).toByteArray
    Utils.toPositive(Utils.murmur2(zooIdByteArray)) % numPartitions
  }
}
