# Place Reviews App  NayaProjectBigData
## The first architecture - Airflow and TelegramBot are responsible of all the flow
### you can start the process by inserting new place to Telegram Bot and if the place exists in google maps API than i store the place_id and the place name in MongoDB
### and then all the flow are started, first send it into Kafka topic 1 from kafka to Spark Streaming to build Sentiment and Polarity columns to see the corolations between user rating and his review, at the end Spark send it simultaneously to HDFS path,and store it as Parquet format and to Kafka with another Topic 2

<img src="https://github.com/yosshor/NayaProjectBigData/blob/main/image/architecture1img.png" alt="google advertising" height="288" width="388"/> 
