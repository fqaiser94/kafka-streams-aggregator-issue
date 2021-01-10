package com.fqaiser

import org.scalatest.Assertion
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

  private type Expectation = Integer => Assertion
  private def runForRangeOfZooIds(numPartitions: Int, expectations: Seq[Expectation]): Unit = {
    (1 to 1000).foreach { zooId =>
      val result = zooIdPartitioner.partition(topic, ZooKey(zooId), ZooValue(), numPartitions).toInt
      expectations.foreach(_(result))
    }
  }

  Feature("partition should always return a value between 0 and (numPartitions - 1)") {
    def checkZooIdAlwaysWithinExpectedRange(numPartitions: Int): Unit = {
      runForRangeOfZooIds(numPartitions, Seq(
        x => x.toInt should be >= 0,
        x => x.toInt should be <= (numPartitions - 1)
      ))
    }

    Seq(1, 2, 8, 16).foreach { numPartitions =>
      Scenario(s"numPartitions = $numPartitions") {
        checkZooIdAlwaysWithinExpectedRange(numPartitions)
      }
    }
  }

  Feature("partition return value should be evenly distributed over a range of zooIds") {
    Scenario("???") {

    }
  }

}
