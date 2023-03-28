import googlemaps
import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, mean, count, desc


def place_exists_check(new_place,google_maps_key):
    gmaps = googlemaps.Client(key=google_maps_key)
    result = gmaps.find_place(input=new_place, input_type='textquery')
    address = ''
    place_exists = 0
    if result['status'] == 'ZERO_RESULTS':
        text = 'Place doesn''t exist, please try again'
    else:
        place_id = result['candidates'][0]['place_id']
        place_lookup = gmaps.place(place_id)
        address_components = place_lookup['result']['address_components']
        for component in address_components:
            address = address + ' ' + component['short_name']
        text = f'Add {new_place} at address: {address}? Y or N?'
        place_exists = 1
    return place_exists, text, place_id

def insert_place_to_mongo(place, place_id):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["reviews"]
    places_col = db["places"]
    search = {'place': place}
    update_with = {"$set": {'place': place,'place_id':place_id }}
    places_col.update_one(search, update_with, upsert=True)

def get_mongo_places():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["reviews"]
    places_col = db["places"]
    cursor = places_col.find({},{'_id':0,'place':1 })
    places = [item['place'] for item in cursor]
    print(places)
    return ' | '.join(str(v) for v in places)

def send_place_review(place,parquet_path):  
    spark = SparkSession.builder.appName('restaurant_score').getOrCreate()
    score = spark.read.parquet(parquet_path) 
    score = score.filter(score.name.contains(place))
    score = score \
        .agg( mean('rating').alias("rating_mean_score") \
             , mean('polarity').alias("polarity_mean_score") \
             , count('rating').alias("reviews_count")
             ) \
        .collect()
    score = score[0].asDict()
    return score
