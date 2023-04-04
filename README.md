# Place Reviews App : Big Data Project
## The first architecture - Airflow and TelegramBot are responsible of this flow

<img src="https://github.com/yosshor/NayaProjectBigData/blob/main/image/architecture1img.png" alt="google advertising" height="288" width="388"/> 

You can start the process by inserting a new place into Telegram Bot and if the place exists in google maps API then i store the place_id and the place name into MongoDB and then all the flow are started, first sends the results from Google Maps API into Kafka topic 1 then from kafka to Spark Streaming to build Sentiment and Polarity columns to see the corolations between the user rating and his review, at the end Spark send it simultaneously to HDFS path, and stores it there as Parquet format file and also to Kafka to another Topic - Topic 2.
And Kafka Topic 2 make some ajdustment like convert the location point to geopoint for Kibana etc, and send the Data to ElasticSearch and then we can see and examine the result on the Kibana dashboard  

## The second architecture
### Brings the all Places name from MongoDB, then after the user select one place, Spark brings distinct data from HDFS and return summary of the data that made with Spark
<img src="https://github.com/yosshor/NayaProjectBigData/blob/main/image/architecture2img.png" alt="google advertising" height="188" width="400"/> 

