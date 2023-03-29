import googlemaps
from kafka import KafkaProducer
import json
import pymongo
from bson import json_util


topic = 'reviews'
brokers = ['cnt7-naya-cdh63:9092']
producer = KafkaProducer(bootstrap_servers=brokers,)
keys=[]

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["reviews"]
places_col = db["places"]
cursor = places_col.find({},{'_id':0, 'place_id':1})
places_ids = [item['place_id'] for item in cursor]
with open('/home/naya/NayaBigDataProject/secret/secret.txt','r') as f:
     keys.append(f.read().split('\n'))
google_maps_key = keys[0][0].split('google_maps_key:')[1].replace("'","")
gmaps = googlemaps.Client(key=google_maps_key)

for place_id in places_ids:
    results = gmaps.place(place_id, reviews_sort ='newest')
    name = results['result']['name']
    city = results['result']['address_components'][2]['short_name']
    lat = results['result']['geometry']['location']['lat']
    lng = results['result']['geometry']['location']['lng']
    serves_beer = str(results['result'].get('serves_beer') or 'False')
    serves_breakfast = str( results['result'].get('serves_breakfast') or 'False')
    serves_brunch = str(results['result'].get('serves_brunch') or 'False')
    serves_dinner = str(results['result'].get('serves_dinner') or 'False')
    serves_lunch = str(results['result'].get('serves_lunch') or 'False')
    serves_vegetarian_food = str(results['result'].get('serves_vegetarian_food') or 'False')
    serves_wine = str(results['result'].get('serves_wine') or 'False')
    user_ratings_total = str(results['result'].get('user_ratings_total') or 'False')
    types = str(results['result'].get('types') or 'False')
    reviews = results['result']['reviews']
    for review in reviews:
        time = review['time']
        author_name = review['author_name']
        author_url = review['author_url']
        text = review['text']
        rating = review['rating']    
        data = [{
            "city": city,
            "place_id": place_id,
            "name": name,
            "types":types,
            "user_ratings_total": user_ratings_total,
            "geometry": { "location" : { "lat": lat, "lon": lng}},
            "serves_beer": str(serves_beer),
            "serves_breakfast": str(serves_breakfast),
            "serves_brunch": str(serves_brunch),
            "serves_dinner": str(serves_dinner),
            "serves_lunch": str(serves_lunch),
            "serves_vegetarian_food": str(serves_vegetarian_food),
            "serves_wine": str(serves_wine),
            "time":time,
            "author_name": author_name,
            "author_url": author_url,
            "text": str(text).replace('\n',''),
            "rating":rating
            }]
        producer.send(topic, value=json.dumps(data, default=json_util.default).encode('utf-8'))
        producer.flush()
        print(f"data sent via from Airflow to kafka topic : {topic}  ")






