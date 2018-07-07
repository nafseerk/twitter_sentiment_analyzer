import sys
import os
import datetime
import jsonpickle
import time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, AppAuthHandler
from tweepy import Stream, API
from tweepy import TweepError

# TODO - add code to filter only the tweets that have location
# TODO - add code to send json stream to socket


class TweetExtractor:

    def __init__(self, eoi, tweets_data_path):

        # set the Event of Interest (EOI)
        self.event = eoi

        # Twitter Credentials
        self.access_token = 'ADD YOUR ACCESS TOKEN'
        self.access_token_secret = 'ADD YOUR ACCESS TOKEN SECRET'
        self.consumer_key = 'ADD YOUR API KEY'
        self.consumer_secret = 'ADD YOUR API KEY SECRET'

        # Directory for storing raw twitter data
        self.tweets_data_path = tweets_data_path

    class LiveTweetListener(StreamListener):

        def __init__(self, output_file_name):
            super().__init__()
            self.output_file_name = output_file_name
            self.tweet_count = 0

        def on_data(self, data):

            try:
                with open(self.output_file_name, 'a') as out_file:
                    out_file.write(data)

                self.tweet_count += 1

                if self.tweet_count % 100 == 0:
                    print("Downloaded {0} tweets".format(self.tweet_count))

                return True
            except BaseException as e:
                print('Error while getting data from twitter stream', str(e))
                time.sleep(5)
            return True

        def on_error(self, status_code):
            print('Error while listening to tweet: Status =', status_code)

    def extract_live_tweets(self, keywords):

        # Create output file name for storing raw twitter data
        current_date = datetime.datetime.today().strftime('%Y-%m-%d')
        output_dir = self.tweets_data_path + '/' + self.event + '/'
        output_file = output_dir + current_date + '_live.json'
        try:
            os.makedirs(output_dir)
        except FileExistsError:
            pass

        # This handles Twitter authentication and the connection to Twitter Streaming API
        listener = self.LiveTweetListener(output_file)
        auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords
        stream.filter(track=keywords)

    def extract_historic_tweets(self, keywords):

        # Get twitter handle
        auth = AppAuthHandler(self.consumer_key, self.consumer_secret)
        api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        if not api:
            print("Error while authenticating with twitter")
            sys.exit(-1)

        # Form search query from the keywords
        search_query = ' OR '.join(keywords)

        # Create output file name for storing raw twitter data
        current_date = datetime.datetime.today().strftime('%Y-%m-%d')
        output_dir = self.tweets_data_path + '/' + self.event + '/'
        output_file = output_dir + current_date + '_historic.json'
        try:
            os.makedirs(output_dir)
        except FileExistsError:
            pass

        # Set query params
        max_tweets = 1000000000  # Some arbitrary large number
        tweets_per_query = 100  # this is the max the API permits

        '''
           params for maintaining context/continuity across the entire query
           
           1. If results from a specific ID onwards are required, set since_id to that ID.
           else default to no lower limit, go as far back as API allows
        
           2. If results only below a specific ID are, set max_id to that ID.
           else default to no upper limit, start from the most recent tweet matching the search query.
        
        '''
        since_id = None
        max_id = -1

        # Execute the search query (tweets_per_query at a time)
        tweet_count = 0
        with open(output_file, 'w') as out_file:
            while tweet_count < max_tweets:
                try:
                    if max_id <= 0:
                        if not since_id:
                            new_tweets = api.search(q=search_query,
                                                    count=tweets_per_query)
                        else:
                            new_tweets = api.search(q=search_query,
                                                    count=tweets_per_query,
                                                    since_id=since_id)
                    else:
                        if not since_id:
                            new_tweets = api.search(q=search_query,
                                                    count=tweets_per_query,
                                                    max_id=str(max_id - 1))
                        else:
                            new_tweets = api.search(q=search_query,
                                                    count=tweets_per_query,
                                                    max_id=str(max_id - 1),
                                                    since_id=since_id)
                    if not new_tweets:
                        print("No more tweets found")
                        break
                    for tweet in new_tweets:
                        out_file.write(jsonpickle.encode(tweet._json, unpicklable=False))
                        out_file.write('\n')
                    tweet_count += len(new_tweets)
                    print("Downloaded {0} tweets".format(tweet_count))
                    max_id = new_tweets[-1].id
                except TweepError as e:
                    # Just exit if any error
                    print("some error : " + str(e))
                    break

        print("Downloaded {0} tweets, Saved to {1}".format(tweet_count, output_file))


if __name__ == '__main__':

    # Set the EOI here
    eoi = 'TRUMP'

    # Add your keywords defining EOI
    keywords = ['donald trump', 'trump', 'us president', 'potus']

    # Set the output file name for twitter data
    twitter_raw_data_path = '/Users/apple/twitter_data/'

    tweet_extractor = TweetExtractor(eoi, twitter_raw_data_path)

    # Run either extract_live_tweets (blocking call) OR extract_historic_tweets

    tweet_extractor.extract_live_tweets(keywords)

    # tweet_extractor.extract_historic_tweets(keywords)
