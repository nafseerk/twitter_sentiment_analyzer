import random


class SentimentAnalyzer:

    def __init__(self):
        pass

    def get_sentiment_score(self, tokens):

        # Using Random score until the method is implemented
        score = random.randint(-1,1)

        # NLTK Sentiment analysis code to be added here

        return score

    def score_tweets(self, input_file):

        # Assumes that the input_file has .json extension
        output_file = input_file[:-5] + '_scored.csv'

        # apply sentiment analysis to each line of input file and write the result to an output CSV file
        # in the following format for each line:
        # tweet_id,score,lat,lng,timestamp


if __name__ == '__main__':
    parsed_twitter_data_file = 'ADD FILE PATH HERE'

    # Create Sentiment Analyzer
    sentiment_analyzer = SentimentAnalyzer()

    # Parse the tweets in a file and write to output file
    sentiment_analyzer.score_tweets(parsed_twitter_data_file)
