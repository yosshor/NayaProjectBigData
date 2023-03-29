import telebot
from google_maps.nearby_places import get_nearby_places
from google_maps.place_reviews import get_reviews
from bot_func.bot_functions import *
from google_maps.get_place_info import get_reviews_for_place

keys=[]
with open('/home/naya/MyPythonExercises/finalProject/secret/secret.txt','r') as f:
     keys.append(f.read().split('\n'))

google_maps_key = keys[0][0].split('google_maps_key:')[1].replace("'","")
bot_token = keys[0][1].split('bot_token:')[1].replace("'","")
print(bot_token,google_maps_key)




bot = telebot.TeleBot(bot_token)
parquet_path = 'hdfs://cnt7-naya-cdh63:8020/tmp/staging/hdfsProject/'
place_dict = {}


@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, 'Hello, welcome to the reviews bot')


@bot.message_handler(commands=['add_place'])
def add_place(message):
    text = "which place would you like to collect reviews for?"
    sent_msg = bot.send_message(message.chat.id, text)
    bot.register_next_step_handler(sent_msg, add_place_handler)


def add_place_handler(message):
    new_place = message.text
    place_exists, text, place_id = place_exists_check(new_place,google_maps_key)
    place_dict['place'] = new_place
    place_dict['place_id'] = place_id
    sent_msg = bot.send_message(message.chat.id, text)
    bot.register_next_step_handler(sent_msg, add_place_confirmation)


def add_place_confirmation(message):
    #if place exists, insert place_id and place name to MongoDB and send to kafka

    user_response = message.text
    if user_response == 'N':
        sent_msg = bot.send_message(message.chat.id, 'Please try again')
    elif user_response == 'Y':
        res = insert_place_to_mongo(place_dict['place'], place_dict['place_id'])
        get_reviews_for_place(place_dict['place_id'],google_maps_key)
        sent_msg = bot.send_message(message.chat.id, f"{place_dict['place']} added")
        get_nearby_places(place_dict['place'],google_maps_key)
    else:
        sent_msg = bot.send_message(message.chat.id, 'Please reply with Y or N')

@bot.message_handler(commands=['review_place'])
def add_place(message):
    available_places = get_mongo_places()
    text = "which place would you like to collect reviews for? " + available_places
    sent_msg = bot.send_message(message.chat.id, text)
    bot.register_next_step_handler(sent_msg, review_place_handler)


def review_place_handler(message):
    place = message.text
    result = send_place_review(place.strip(),parquet_path)
    if result['polarity_mean_score'] is None or result['rating_mean_score'] is None:
        bot.send_message(message.chat.id, f"The {place} not found")
    else:
        rating_mean_score = result['rating_mean_score'] 
        polarity_mean_score = round(float(result['polarity_mean_score']),2)
        reviews_count = result['reviews_count']
        text = f'''The avg reviews results for {place} are: \n
        mean rating score: {rating_mean_score} \n
        mean polarity: {polarity_mean_score} \n
        total reviews count: {reviews_count}
        '''
        bot.send_message(message.chat.id, text)

bot.polling()