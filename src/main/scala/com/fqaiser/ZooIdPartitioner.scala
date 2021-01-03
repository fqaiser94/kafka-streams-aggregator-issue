package com.fqaiser

import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.processor.StreamPartitioner

case class ZooIdPartitioner[K, V](extractZooIdFromKey: K => Int) extends StreamPartitioner[K, V] {
  override def partition(topic: String, key: K, value: V, numPartitions: Int): Integer = {
    val zooId = extractZooIdFromKey(key)
    val zooIdByteArray = BigInt(zooId).toByteArray
    Utils.toPositive(Utils.murmur2(zooIdByteArray)) % numPartitions
  }
}
