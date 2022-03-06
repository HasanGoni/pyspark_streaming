# importing library for tweeter
import tweepy
# authenticate tweeter handler
from tweepy import OAuthHandler
from tweepy import Stream
#from tweepy.streaming import StreamListener

import socket
import json

# from configuration differenct
from config import ACCESS_TOKEN
from config import ACCESS_TOKEN_SECRET

from config import API_KEY

from config import API_KEY_SECRET
# An access token used in authentication that allows you to pull specific data.
from config import BEARER_TOKEN

from config import PORT_NUMBER


SEARCH_TOPIC = ['corona']


class TweetsListener(tweepy.Stream):
    # def __init__(self, csocket):
        # self.client_socket = csocket
    
    def on_data(self, data):
        try:
            msg = json.load(data)

            print(msg['text'].encode('utf-8'))

            self.client_socket.send(
                msg['text'].encode('utf-8')
            )
            return True

        except BaseException as e:
            # Error hanling
            print(f"Ahh! Look what is wrong: {e}")
            return True
    
    def on_error(self, status):
        print(status)
        return True


if __name__ == '__main__':

    s = socket.socket()

    host = "127.0.0.1"
    port = PORT_NUMBER
    s.bind((host, port))

    print(f'listening to port {PORT_NUMBER}\n')

    # Wait and Establish the connection with
    # client
    s.listen(5)

    # This sends back data and adress 
    # from where it comes from
    data, addr = s.accept()

    print(f'Recieved reqquest from {addr} \n')


    twitter_stream = TweetsListener(
    API_KEY, API_KEY_SECRET,
    ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


    # filter the tweet realted 
    # to any topic
    twitter_stream.filter(
        track=SEARCH_TOPIC
    )