# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Additional imports
import sys
import os
import datetime


# This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)


class TweetExtractor:

    def __init__(self, tweets_data_path):

        # Twitter Credentials
        self.access_token = 'ADD YOUR ACCESS TOKEN'
        self.access_token_secret = 'ADD YOUR ACCESS TOKEN SECRET'
        self.consumer_key = 'ADD YOUR API KEY'
        self.consumer_secret = 'ADD YOUR API KEY SECRET'

        # Directory for storing raw twitter data
        self.tweets_data_path = tweets_data_path

    def extract_real_time_tweets(self, eoi, keywords):

        # This handles Twitter authentication and the connection to Twitter Streaming API
        listener = StdOutListener()
        auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        stream = Stream(auth, listener)

        # Create output file name for raw twitter data
        current_date = datetime.datetime.today().strftime('%Y-%m-%d')
        output_dir = self.tweets_data_path + '/' + eoi + '/'
        output_file = output_dir + current_date + '.txt'
        try:
            os.makedirs(output_dir)
        except FileExistsError:
            pass

        # Redirect std out to the file
        sys.stdout = open(output_file, 'a')

        # This line filter Twitter Streams to capture data by the keywords
        stream.filter(track=keywords)


if __name__ == '__main__':

    twitter_raw_data_path = '/Users/apple/twitter_data/'
    tweet_extractor = TweetExtractor(twitter_raw_data_path)

    # Set the EOI
    eoi = 'TRUMP'
    keywords = ['donald trump', 'trump', 'us president', 'potus']

    tweet_extractor.extract_real_time_tweets(eoi, keywords)
