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
  
