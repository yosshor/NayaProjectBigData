from kafka import KafkaConsumer
from time import sleep
from elasticsearch import Elasticsearch
import json
from elasticsearch import helpers
from bson import json_util
import pyarrow as pa
from datetime import datetime
import pyarrow.parquet as pq

topic_back = "fromspark"
index_name_es = "nayaproject"
brokers = ['cnt7-naya-cdh63:9092']

#ElasticSearch parameters
es = Elasticsearch(host="localhost", port=9200) 

# hdfs connector 
fs = pa.hdfs.connect(
    host='cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)


# First we set the KafkaConsumer class to create a generator of the messages.
consumer = KafkaConsumer(
    topic_back,
    group_id='newgroup',
    bootstrap_servers=brokers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    max_poll_records=100,
    api_version=(2, 2, 1))

for message in consumer:
    try:
        message = message.value.decode('utf-8-sig').replace("\n", "")
        jdoc = {"data": json.loads(message)}
        lat,lon = float(jdoc["data"]["geometry"]["location"]["lat"]),\
                  float(jdoc["data"]["geometry"]["location"]["lon"])
        jdoc["data"]["geometry"]["location"]["lat"] = lat
        jdoc["data"]["geometry"]["location"]["lon"] = lon
        jdoc["data"]["rating"] = int(jdoc["data"]["rating"])
        jdoc["data"]["polarity"] = float(jdoc["data"]["polarity"])
        jdoc["data"]["subjectivity"] = float(jdoc["data"]["subjectivity"])
        jdoc["data"]["types"] = list(jdoc["data"]["types"].split(','))[0].replace("[","").replace("\"","")
        jdoc["data"]["user_ratings_total"] = int(jdoc["data"]["user_ratings_total"])
        try:
            es.index(index=index_name_es, doc_type='_doc', body=jdoc)
            sleep(1)
            print("uploaded to Elastic index name = " + index_name_es)
        except Exception as e:
            print(f"your Exception is : {e}")
    except Exception as e:
        print(f"your Exception is : {e}")


