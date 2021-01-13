For the past year, I've been working heavily with kafka-streams (specifically the Scala library) to build
so-called "real-time" streaming applications.  

While kafka-streams is in general a useful library and a strong alternative to spark-streaming/flink frameworks. 

The nice about kafka-streams is that it is just another library, 
This is in contrast to spark or flink which are "frameworks" that require independent cluster maangement.  


Sadly though, I've had a number of issues with kafka-streams.  
I regret to say that it's not particularly reccommended, at least not for important production use-cases (e.g. billing). 


# Model

Let's say you have many zoos.  
Furthermore, each zoo contains animals.   
Your pipeline receives food parcels, each stamped for a particular zoo. 
The pipeline needs to feed the animals in each zoo.  
But each animal only needs up to a maximum number of calories.  
Once all the animals have been fed, we can throw the rest of the food away. 

# Useful Research

1. https://www.learningjournal.guru/courses/kafka/kafka-foundation-training/custom-partitioner/
2. https://blog.knoldus.com/custom-partitioner-in-kafka-lets-take-quick-tour/
3. https://danlebrero.com/2018/12/17/big-results-in-kafka-streams-range-query-rocksdb/
4. https://sachabarbs.wordpress.com/2019/04/23/kafkastreams-using-processor-api-in-dsl/
5. https://stackoverflow.com/questions/39985048/kafka-streaming-concurrency
6. https://www.michael-noll.com/blog/2020/01/16/what-every-software-engineer-should-know-about-apache-kafka-fundamentals/
7. https://www.confluent.io/blog/kafka-streams-tables-part-3-event-processing-fundamentals/
8. https://stackoverflow.com/questions/62302561/shared-kafka-statestore-best-practices#:~:text=There%20are%20no%20race%20conditions,executed%20in%20a%20single%20thread.&text=If%20two%20processor%20access%20the,in%20the%20same%20sub%2Dtopology.
9. https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/
10. https://objectpartners.com/2019/07/31/slimming-down-your-kafka-streams-data/
11. https://www.youtube.com/watch?v=_KAFdwJ0zBA&ab_channel=Devoxx

  
