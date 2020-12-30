For the past year, I've been working heavily with kafka-streams (specifically the Scala library) to build
so-called "real-time" streaming applications.  

While kafka-streams is in general a useful library and a strong alternative to spark-streaming/flink frameworks. 

The nice about kafka-streams is that it is just another library, 
This is in contrast to spark or flink which are "frameworks" that require independent cluster maangement.  


Sadly though, I've had a number of issues with kafka-streams.  
I regret to say that it's not particularly reccommended, at least not for important production use-cases (e.g. billing). 

  