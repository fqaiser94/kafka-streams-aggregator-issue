package com.fqaiser

import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, TimestampedKeyValueStore}

import scala.collection.mutable.ArrayBuffer

// TODO: test
case class AnimalFeederValueTransformer(zooAnimalStateStoreName: String, animalCaloriesCountStoreName: String)
    extends ValueTransformer[FoodValue, OutputValue] {

  var zooAnimalStateStore: TimestampedKeyValueStore[ZooIdAnimalId, AnimalValue] = _
  var animalCalorieFillStateStore: KeyValueStore[AnimalKey, AnimalCalorieFill] = _

  override def init(context: ProcessorContext): Unit = {
    zooAnimalStateStore = context
      .getStateStore(zooAnimalStateStoreName)
      .asInstanceOf[TimestampedKeyValueStore[ZooIdAnimalId, AnimalValue]]

    animalCalorieFillStateStore = context
      .getStateStore(animalCaloriesCountStoreName)
      .asInstanceOf[KeyValueStore[AnimalKey, AnimalCalorieFill]]

    println(s"************ show me what you got already")
    // TODO: so this is where I fail, makes sense
    animalCalorieFillStateStore.all().forEachRemaining(x => println(x))
    println()
  }

  override def transform(value: FoodValue): OutputValue = {
    val zooIdAnimals = getAnimals(value.zooId)
    val selectedAnimal = zooIdAnimals.head
    val animalId = selectedAnimal.animalId
    val animalKey = AnimalKey(animalId)
    val currentCalorieFill = Option(animalCalorieFillStateStore.get(animalKey)).map(_.fill).getOrElse(0)
    val newCalorieFill = currentCalorieFill + value.calories
    if (newCalorieFill <= selectedAnimal.maxCalories) {
      animalCalorieFillStateStore.put(animalKey, AnimalCalorieFill(newCalorieFill))
      OutputValue(value.foodId, value.zooId, value.calories, animalId, newCalorieFill)
    } else {
      OutputValue(value.foodId, value.zooId, value.calories, -1, 0)
    }
  }

  override def close(): Unit = {}

  private def getAnimals(zooId: Int): ArrayBuffer[AnimalValue] = {
    var zooIdAnimals = ArrayBuffer.empty[AnimalValue]
    val iterator = zooAnimalStateStore.all()
    while (iterator.hasNext) {
      val timestampedKeyValue = iterator.next()
      if (timestampedKeyValue.key.zooId == zooId)
        zooIdAnimals += timestampedKeyValue.value.value()
    }
    iterator.close()
    zooIdAnimals
  }
}