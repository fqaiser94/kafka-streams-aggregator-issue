package com.fqaiser

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

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

  private def doForRangeOfZooIds[T](rangeOfZooIds: Seq[Int], numPartitions: Int, whatToDo: Integer => T): Unit = {
    rangeOfZooIds.foreach { zooId =>
      val result = zooIdPartitioner.partition(topic, ZooKey(zooId), ZooValue(), numPartitions).toInt
      whatToDo(result)
    }
  }

  Feature("partition should always return a value between 0 and (numPartitions - 1)") {
    def checkZooIdAlwaysWithinExpectedRange(numPartitions: Int): Unit = {
      doForRangeOfZooIds(1 to 1000, numPartitions, x => x.toInt should (be >= (0) and be <= (numPartitions - 1)))
    }

    Seq(1, 2, 8, 16).foreach { numPartitions =>
      Scenario(s"numPartitions = $numPartitions") {
        checkZooIdAlwaysWithinExpectedRange(numPartitions)
      }
    }
  }

  Feature("partition return value should be evenly distributed over a range of zooIds") {
    def runTest(numPartitions: Int) = {
      val rangeOfZooIds = 1 to 1000000
      var partitionValues = ArrayBuffer.empty[Int]
      doForRangeOfZooIds(
        rangeOfZooIds = rangeOfZooIds,
        numPartitions = numPartitions,
        whatToDo = x => partitionValues = partitionValues.append(x.toInt)
      )

      val expectedPrcntOfValuesInEachPartition = 100 / numPartitions
      println(s"running for numPartitions: $numPartitions")
      println(s"expectedPrcntOfValuesInEachPartition: $expectedPrcntOfValuesInEachPartition")

      partitionValues
        .groupBy(identity)
        .view
        // calculate number of occurences
        .mapValues(_.size)
        // calculate percentage of total
        .mapValues(x => ((x.toDouble / rangeOfZooIds.size.toDouble) * 100.0).toInt)
        .toList
        .sortBy(_._1)
        .map { case (k, v) => println(s"$k:$v"); (k, v) }
        .map { case (k, v) => v }
        .foreach(x => x shouldEqual expectedPrcntOfValuesInEachPartition)
    }

    Seq(1, 8, 16, 32).foreach { numPartitions =>
      Scenario(s"numPartitions = $numPartitions") {
        runTest(numPartitions)
      }
    }

  }

}
