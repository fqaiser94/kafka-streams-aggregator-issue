package com.fqaiser

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

object ZooIdPartitionerTest {

  case class ZooKey(zooId: Int)

  case class ZooValue(numAnimals: Int = 10)

}

// TODO: complete
class ZooIdPartitionerTest extends AnyFeatureSpec with Matchers {

  import ZooIdPartitionerTest._

  private val topic = "zooTopic"
  private val zooIdPartitioner = ZooIdPartitioner[ZooKey, ZooValue](_.zooId)

  Feature("Basic tests") {
    Scenario("1") {
      val result = zooIdPartitioner.partition(topic, ZooKey(1), ZooValue(), 8)
      val expected = 1
      result shouldEqual expected
    }

    Scenario("2") {
      val result = zooIdPartitioner.partition(topic, ZooKey(2), ZooValue(), 8)
      val expected = 5
      result shouldEqual expected
    }
  }

  Feature("partition should always return a value between 0 and numPartitions -1") {
    Scenario("numPartitions = 1") {

    }

    Scenario("numPartitions = 2") {

    }

    Scenario("numPartitions = 8") {

    }
  }

  Feature("partition return value should be evenly distributed over a range of zooIds") {
    Scenario() {

    }
  }

}
