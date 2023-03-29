import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from textblob import TextBlob
from pyspark.sql.types import StringType, StructType, IntegerType, MapType, FloatType
import pandas as pd
import json


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
os.environ['HADOOP_USER_NAME']='hdfs'

bootstrapServers = "cnt7-naya-cdh63:9092"
topics = "reviews"
topic_back = "fromspark"

def polarity_detection(text):
    return TextBlob(text).sentiment.polarity

def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity

def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, FloatType())
    words = words.withColumn("polarity", polarity_detection_udf("text"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, FloatType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("text"))
    return words

spark = SparkSession\
        .builder\
        .appName("reviews")\
        .getOrCreate()


df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", topics)\
    .load()



schema = StructType() \
    .add("city", StringType()) \
    .add("place_id", StringType()) \
    .add("name", StringType()) \
    .add("types", StringType()) \
    .add("user_ratings_total",StringType()) \
    .add("geometry",MapType(StringType(),MapType(StringType(),StringType()))) \
    .add("serves_beer", StringType()) \
    .add("serves_breakfast", StringType()) \
    .add("serves_brunch", StringType()) \
    .add("serves_dinner", StringType()) \
    .add("serves_lunch", StringType()) \
    .add("serves_vegetarian_food", StringType()) \
    .add("serves_wine", StringType()) \
    .add("time", StringType()) \
    .add("author_name", StringType()) \
    .add("author_url", StringType()) \
    .add("text", StringType()) \
    .add("rating", IntegerType()) 

df_reviews = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json('value', schema).alias("value"))\
    .select("value.*")

df_reviews = text_classification(df_reviews)
df_reviews_kafka = df_reviews.selectExpr("to_json(struct(*)) AS value") \
   .writeStream \
   .format("kafka") \
   .outputMode("append") \
   .option("kafka.bootstrap.servers", bootstrapServers) \
   .option("topic", topic_back) \
   .option("checkpointLocation", "/home/naya/MyPythonExercises/project/filesTest/checkpoint/") \
   .start() 
   

target_parquet_hdfs = df_reviews \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("columnNameOfCorruptRecord", "malformed_rows")\
    .option("path", "hdfs://cnt7-naya-cdh63:8020/tmp/staging/hdfsProject/") \
    .option("checkpointLocation", "hdfs://cnt7-naya-cdh63:8020//tmp/staging/checkpoint") \
    .start()


df_reviews_kafka.awaitTermination()
target_parquet_hdfs.awaitTermination()
