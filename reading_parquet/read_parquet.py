import pandas as pd
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)
path = "hdfs://cnt7-naya-cdh63:8020/tmp/staging/hdfsProject/part-00000-49b6a345-e09d-4d4e-834b-2b50660e56c7-c000.snappy.parquet"

parquet_df = spark.read.parquet(path)
parquet_df.show(n=30,vertical=True,truncate=False)
parquet_df.show().select("city","name")
parquet_df = parquet_df.filter(parquet_df.city == "Dallal Restaurant")
parquet_df = parquet_df[0].asDict()
