from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import os
import pandas as pd
import socket
import yaml

CREDS_FILE = os.path.join(os.curdir, 'credconf.yaml')
with open(CREDS_FILE, 'r') as f:
   creds = yaml.load(f, Loader=yaml.FullLoader)

consumer_key = creds["TWTR"]["APIKEY"]
consumer_secret = creds["TWTR"]["APISECRET"]
access_token = creds["TWTR"]["ACCESSTOKEN"]
access_secret = creds["TWTR"]["ACCESSTOKENSECRET"]

class TweetListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg["text"].encode("UTF-8"))
            self.client_socket.send(msg["text"].encode("UTF-8"))
            return True
        except BaseException as e:
            print("Error ", e)
        return True

    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=["guitar"])

if __name__ == "__main__":
    s = socket.socket()
    host = "127.0.0.1"
    port = 5555
    s.bind((host, port))

    print("listening on port " + str(port))

    s.listen(5)
    c, addr = s.accept()

    sendData(c)


