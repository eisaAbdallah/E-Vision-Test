# E-Vision-Test

my solution is to build a parser of csv which are mapped with a java object that can be used in java code to the 

amount and mak the calculation on change to dollar and the 10 % calculation

And Then i can send these results to the each  topic created for each calculation Name  them (topic1,topic2 )  for example to be stored on kafka in memeory storage

And then i will consume this topics and it`s data stored in the partitions and write every result or topic to files on the system directories through file writer class

i should either create an api for producer with help of the springboot restcontroller And two apis for the consumers to write the file
                         
                         -------------******-----------
first  i creatred the the admin conf of kafka 
and added in the properties file the property spring.autoconfigure.exclude to disable autodatasiource configuration 
i created three apis get http methods 
1- /prodcer to send data to topics
2- /starter-consumer to  consime data in the topic of the lessthan 1000 amount                        
3- /dollar-consumer to  consime data in the topic of the greaterthan 1000 amount  and change it to $   
4-i made csv parsing usinfg open parser of the csv with assigned class named with ParserCSV enclosed with the three metioned properties

                          -------------******-----------

used  Tools:
Ide Intelliji
JDK 11
maven V3.6.3 buiding tool
kafka dependencies(client/spring-kafka) v 2.6.0
open csv dependency  V 5.3 

