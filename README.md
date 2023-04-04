# Place Reviews App : Big Data Project
## The first architecture - Airflow and TelegramBot are responsible of this flow

<img src="https://github.com/yosshor/NayaProjectBigData/blob/main/image/architecture1img.png" alt="google advertising" height="288" width="388"/> 

you can start the process by inserting a new place into Telegram Bot and if the place exists in google maps API then i store the place_id and the place name into MongoDB and then all the flow are started, first send the results from Google Maps API into Kafka topic 1 then from kafka to Spark Streaming to build Sentiment and Polarity columns to see the corolations between the user rating and his review, at the end Spark send it simultaneously to HDFS path,and store it as Parquet format and to Kafka with another Topic - Topic 2.
And Kafka Topic 2 make some ajdustment like convert the location point to geopoint etc, and send the Data to ElasticSearch and see the result on the Kibana dashboard  

## The second architecture
### 
<img src="https://github.com/yosshor/NayaProjectBigData/blob/main/image/architecture2img.png" alt="google advertising" height="188" width="400"/> 

### MongoDb for getting the Place_name, then bring distinct place_id that user selected from HDFS with Spark
