#pip3 install tweepy
import json
from tweepy import OAuthHandler, Stream, StreamListener

from datetime import datetime

#Chaves de acesso ao twitter

consumer_key = "XXXXXXXXXXXXXXXXXXXXXXXXX"
consumer_secret = "XXXXXXXXXXXXXXXXXXXXXXXXX"

access_token = "XXXXXXXXXXXXXXXXXXXXXXXXX"
access_token_secret = "XXXXXXXXXXXXXXXXXXXXXXXXX"

data_hoje = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

out = open(f"collected_tweets_{data_hoje}.txt", "w")

class MyListener(StreamListener):
    def __init__(self):
        super().__init__()
        self.counter = 0
        self.limit = 4000
        
    def on_data(self, data):
        #print(data)
        itemString = json.dumps(data)
        out.write(itemString + '\n')
        
        self.counter += 1
        if self.counter < self.limit:
            return True
        else:
            stream.disconnect()

    def on_error(self, status):
        print(status)
    
if __name__ == "__main__":
    l = MyListener()
    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)

    stream.filter(track=["Trump"])