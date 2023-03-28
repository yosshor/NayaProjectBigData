from kafka import KafkaConsumer
from time import sleep
from elasticsearch import Elasticsearch
import json
from elasticsearch import helpers
from bson import json_util

es = Elasticsearch(host="localhost", port=9200)
index_name_es = "nayaproject"
mappings = {
     "properties": {
             "city": {"type": "text"},
             "place_id": {"type": "text", "analyzer": "standard"},
             "name": {"type": "text"},
             "types": {"type": "text"},
             "user_ratings_total": {"type": "integer"},
             "location": {"type": "geo_point"},
             "serves_beer": {"type": "text", "analyzer": "standard"},
             "serves_breakfast": {"type": "text", "analyzer": "standard"},
             "serves_brunch": {"type": "text", "analyzer": "standard"},
             "serves_dinner": {"type": "text", "analyzer": "standard"},
             "serves_lunch": {"type": "text", "analyzer": "standard"},
             "serves_wine": {"type": "text", "analyzer": "standard"},
             "time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"},
             "author_name": {"type": "text", "analyzer": "standard"},
             "author_url": {"type": "text", "analyzer": "standard"},
             "text": {"type": "text", "analyzer": "standard"},
             "rating": {"type": "integer"},
             "polarity" : {"type": "float"},
             "subjectivity" :  {"type": "float"}
             }
         }

print(json.dumps(mappings))
es.indices.create(index=index_name_es, mappings=json.dumps(mappings))
