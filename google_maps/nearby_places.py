import requests
import googlemaps
import json
from google_maps.place_reviews import get_reviews

def get_nearby_places(place_name,google_maps_key):
    key=str(google_maps_key)
    gmaps = googlemaps.Client(key=key)
    places_results = gmaps.places(place_name)
    place_lat = str(places_results['results'][0]['geometry']['location']['lat'])
    place_lng = str(places_results['results'][0]['geometry']['location']['lng'])
    #call api to get the names of all restaurants in the area and save it in nearby_places.json file
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location="+place_lat+"%2C"+place_lng+"&radius=1500&type=restaurant&key="+key
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    results = response.text
    api_response = json.loads(results)
    api_response_string = json.dumps(api_response)
    get_reviews(api_response_string,google_maps_key)