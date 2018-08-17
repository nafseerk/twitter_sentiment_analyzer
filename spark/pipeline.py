import os
import pyspark
from pyspark import SparkContext, SparkConf

# import application code
from tweet_extractor.location_cache import get_location_cache, save_location_cache
from tweet_extractor.tweets_filter import filter_datapoint
from parsing.language_parser import LanguageParser
from sentiment.sentiment_analyer import SentimentAnalyzer

# Configure spark
conf = SparkConf().setAppName('sentiment_analyzer').setMaster('local')
sc = SparkContext(conf=conf)

# Application specific configs
app_root_directory = '/Users/apple/Desktop/new_cs846/twitter_sentiment_analyzer/'
location_cache_file = app_root_directory + 'tweet_extractor/location_cache.pickle'

# Load dataset from local system
datapoints = sc.textFile("file:///Users/apple/twitter_data/2018-07-10_historic.json")
print('Total number of datapoints = ', datapoints.count())

# Stage 1 - Filter datapoints
location_cache = get_location_cache(file_loc=location_cache_file)
filtered_datapoints = datapoints.map(lambda x: filter_datapoint(x, location_cache))
save_location_cache(location_cache, file_loc=location_cache_file)
print('Total number of filtered datapoints = ', filtered_datapoints.count())

# Stage 2 - Parsing datapoints
parser = LanguageParser('en')
parsed_datapoints = filtered_datapoints.map(parser.parse_datapoint)
print('Total number of parsed datapoints = ', parsed_datapoints.count())

# Stage 2 - Sentiment analyzing datapoints
sentiment_analyzer = SentimentAnalyzer()
scored_datapoints = parsed_datapoints.map(sentiment_analyzer.score_datapoint)
print('Total number of scored datapoints = ', scored_datapoints.count())

# Filtering Null values
final = scored_datapoints.filter(lambda x: x is not None)
print('Total number of final datapoints = ', final.count())
print(final.collect())

# Save output locally for use by the visualizer
combinedRDD_with_header = sc.parallelize( ['tweet_id,score,lat,lng,timestamp']).union( final )
combinedRDD_with_header.coalesce( 1 ).saveAsTextFile("file:///Users/apple/twitter_data/2018-07-10_historic_parsed_scored.csv")

sc.stop()
