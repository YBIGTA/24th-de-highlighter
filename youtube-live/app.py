from fastapi import FastAPI
from kafka import KafkaProducer
from dotenv import load_dotenv
import json
import pytchat
import os
import pafy
from pymongo import MongoClient
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Load environment variables
kafka_server = '43.203.141.74:9092'
client = MongoClient("mongodb://ec2-43-203-141-74.ap-northeast-2.compute.amazonaws.com:27017")
db = client['youtube_live']
collection = db['youtube_live_chat']

def get_chat_from_live(video_id, producer):
    chat = pytchat.create(video_id=video_id)
    while chat.is_alive():
        data = chat.get()
        for c in data.items:
            #save to mongodb
            timestamp = datetime.strptime(c.datetime, "%Y-%m-%d %H:%M:%S")
            data2 = {'timestamp': timestamp, 'author': c.author.name, 'message': c.message}
            collection.insert_one(data2)
            
            #save to json
            data2 = {'timestamp': c.datetime, 'author': c.author.name, 'message': c.message}
            file_path = "chat/"+ str(c.datetime).replace(" ", "_").replace(":","-") + ".json"
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data2, f, ensure_ascii=False)
            
            #send timestamp to kafka
            producer.send('stream_filter', value=c.datetime.encode('utf-8'))


async def start_chat_fetching():
    api_key = os.environ.get("GCP_KEY")
    pafy.set_api_key(api_key)
    video_id = os.getenv('YOUTUBE_URL')
    if video_id:
        # Setup Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=[kafka_server]
        )
        get_chat_from_live(video_id, producer)
    else:
        print("No YOUTUBE_URL environment variable set.")

app.add_event_handler("startup", start_chat_fetching)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

