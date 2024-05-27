import pytchat
import pafy
import pandas as pd
import os
from dotenv import load_dotenv
import json


def get_chat_from_live(video_id):
    '''
    fetch chat from youtube live
    Input: video id / url (url에 v=어쩌고 있어야함)
    Output: real-time chat
    '''
    #file_path = 'chat/test.csv'

    chat = pytchat.create(video_id=video_id)

    while chat.is_alive():
        try:
            data = chat.get()
            items = data.items
            for c in items:
                data.tick()
                data2 = {'timestamp': c.datetime, 'author': c.author.name, 'message': c.message}
                file_path = "chat/"+ str(c.datetime).replace(" ", "_").replace(":","-") + ".json"
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data2, f, ensure_ascii=False)
                
        except KeyboardInterrupt:
            print("Interupted by user")
            chat.terminate()
            break
        
if __name__ == "__main__":
    api_key = os.environ.get("GCP_KEY") 
    pafy.set_api_key(api_key)
    video_url = os.getenv('YOUTUBE_URL')
    if video_url:
        get_chat_from_live(video_url)
    else:
        print("No YOUTUBE_URL environment variable set.")