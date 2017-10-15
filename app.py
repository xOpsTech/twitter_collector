import json
import time
import os
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream, Cursor, API
from elasticsearch import Elasticsearch
import constatnts

# create instance of elasticsearch
es = Elasticsearch(constatnts.ES_IP)
tenant_id = None

class TweetStreamListener(StreamListener):
    # on success
    def on_data(self, data):
        # decode json
        tweet = json.loads(data)
        process_tweet(tweet)
        return True

    # on failure
    def on_error(self, status):
        print status


def process_tweet(tweet):
    try:
        if 'text' in tweet:  # only messages contains 'text' field is a tweet
            tweet_json = {
                'id': tweet['id'],  # This is the tweet's id
                'created_at': tweet['created_at'],  # when the tweet posted
                'text': tweet['text'],  # content of the tweet

                'user_id': tweet['user']['id'],  # id of the user who posted the tweet
                'user_name': tweet['user']['name'],  # name of the user, e.g. "Wei Xu"
                'user_screen_name': tweet['user']['screen_name'],  # name of the user account, e.g. "cocoweixu"
                'timestamp': int(time.time() * 1000)
            }

            hashtags = []
            for hashtag in tweet['entities']['hashtags']:
                hashtags.append(hashtag['text'])

            tweet_json.update({
                'hashtags': hashtags
            })

            write_to_es(tweet_json)

    except Exception, e:
        print e


def read_all_data(api):
    # stuff = api.user_timeline(screen_name='Google', count=100, include_rts=True)
    # for status in stuff:
    for status in Cursor(api.user_timeline).items():
        # process status here
        tweet_json = status._json
        process_tweet(tweet_json)


def write_to_es(tweet_json):
    try:
        es_index = 'metrics-' + tenant_id
        es_doc_type = 'tweets'
        es.index(id=tweet_json['id'], index=es_index, doc_type=es_doc_type, body=tweet_json)
    except Exception, e:
        print e


if __name__ == '__main__':
    configs = os.getenv('configs')
    if configs is None:
        print 'Exiting due to missing configs'
        exit()

    try:
        configs_json = json.loads(configs)
    except:
        configs_json = dict()

    tenant = configs_json.get('tenant', 'test')
    consumer_key = configs_json.get('consumer_key', 'QVhBEkpOIlyogRyl7IxDPEodC')
    consumer_secret = configs_json.get('consumer_secret', 'MHYnqIMe26QodxSaqV7haJSHfPz4WjtlSqfflOTRxuD0Exctdl')
    access_token = configs_json.get('access_token', '905887188167966720-JBNFzwipYqgF6Zkc8MKMn4yZDdccouA')
    access_token_secret = configs_json.get('access_token_secret', 'dSWeNeRoV6soe3Vcj2rpfvKfns9YYhmxzCTv3uiE8nU5S')

    tenant_id = tenant

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)
    api = API(auth)

    # search twitter for "congress" keyword
    # stream.filter(track=['congress'])
    stream.userstream(async=True)
    read_all_data(api)
