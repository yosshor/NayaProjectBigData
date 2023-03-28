import os
import json
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from time import sleep
from bson import json_util
from bot_func.bot_functions import insert_place_to_mongo

topic1 = 'reviews'
brokers = ['cnt7-naya-cdh63:9092']
producer = KafkaProducer(bootstrap_servers=brokers,)

#####nearby_places_details_csv#########
def get_reviews(api_response_string,google_maps_key):
    result_json = json.loads(api_response_string)
    print("read json successfully")
    nearby_places_result_json = result_json['results']
    nearby = []
    for i in range(len(nearby_places_result_json)):
        name = nearby_places_result_json[i]['name'],
        place_id = nearby_places_result_json[i]['place_id']
        nearby.append({'name':name,'place_id':place_id})
    #reviews of all places and save it to reviews.json file     
    for i in range(len(nearby)):
        place_id = nearby[i]['place_id']
        place_name = nearby[i]['name']
        url = "https://maps.googleapis.com/maps/api/place/details/json?place_id="+place_id+"&reviews_sort=newest&key="+google_maps_key
        payload={}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload)
        results = response.text
        api_response = json.loads(results)
        places_resluts_json = api_response['result']
        reviews = []
        if 'reviews' in places_resluts_json:
            for j in range(len(places_resluts_json['reviews'])):
                if 'adr_address' in places_resluts_json:
                    city = BeautifulSoup(places_resluts_json['adr_address'], "html.parser").find('span', {'class': 'locality'}).text
                else : city = 'Unknown'
                place_id = places_resluts_json['place_id']
                name = places_resluts_json['name']
                lat = places_resluts_json['geometry']['location']['lat']
                lng = places_resluts_json['geometry']['location']['lng']
                if 'serves_beer' in places_resluts_json:
                    serves_beer = places_resluts_json['serves_beer']
                else: serves_beer = 'Unknown'
                if 'serves_breakfast' in places_resluts_json:
                    serves_breakfast = places_resluts_json['serves_breakfast']
                else: serves_breakfast = 'Unknown'
                if 'serves_brunch' in places_resluts_json:
                    serves_brunch = places_resluts_json['serves_brunch']
                else: serves_brunch = 'Unknown'
                if 'serves_dinner' in places_resluts_json:
                    serves_dinner = places_resluts_json['serves_dinner']
                else: serves_dinner = 'Unknown'
                if 'serves_lunch' in places_resluts_json:
                    serves_lunch = places_resluts_json['serves_lunch']
                else: serves_lunch = 'Unknown'
                if 'serves_vegetarian_food' in places_resluts_json:
                    serves_vegetarian_food = places_resluts_json['serves_vegetarian_food']
                else: serves_vegetarian_food = 'Unknown'
                if 'serves_wine' in places_resluts_json:
                    serves_wine = places_resluts_json['serves_wine']
                else: serves_wine = 'Unknown'
                time = places_resluts_json['reviews'][j]['time']
                review_time = str(datetime.fromtimestamp(time))
                review_author_name = places_resluts_json['reviews'][j]['author_name']
                review_author_url = places_resluts_json['reviews'][j]['author_url']
                if places_resluts_json['reviews'][j]['text'] is None \
                or len(places_resluts_json['reviews'][j]['text']) == 0 \
                or places_resluts_json['reviews'][j]['text'].lstrip().rstrip() == "":
                    review_text = 'NA'                   
                else: 
                    review_text = places_resluts_json['reviews'][j]['text']
                author_name = places_resluts_json['reviews'][j]['author_name']
                author_url = places_resluts_json['reviews'][j]['author_url'] 
                rating = places_resluts_json['reviews'][j]['rating']
                types = places_resluts_json['types']
                print(types)
                user_ratings_total = places_resluts_json['user_ratings_total']
                
                data = [{
                        "city": city,
                        "place_id": place_id,
                        "name": name,
                        "types":types,
                        "user_ratings_total": user_ratings_total,
                        "geometry": { "location" :{ "lat": lat,"lon": lng} },
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
                        "text": str(review_text).replace('\n',''),
                        "rating":rating
                      }]
                producer.send(topic1, value=json.dumps(data, default=json_util.default).encode('utf-8'))
                producer.flush()
                print('try insert to mongoDB')
                try:
                    insert_place_to_mongo(place_name[0],place_id)
                except Exception as Error:
                    print(f'The Error is : {Error}')
                sleep(1)
                reviews.append(data)