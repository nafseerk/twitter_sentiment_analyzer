import json
import datetime
import time
import geopy.geocoders
from geopy.exc import GeocoderTimedOut
from tweet_extractor.location_cache import get_location_cache, save_location_cache
from geocoder.geocoder import Geocoder

# Set timeout period for Geocoder API
geopy.geocoders.options.default_timeout = 7

# For duplicate checking of tweets
unique_tweets = {}

# round-robin rate-respecting Geocoder
geocoder = Geocoder()


# Convert twitter date to posix timestamp
def get_timestamp_from_date(date_string):
    twitter_date_format = '%a %b %d %H:%M:%S +0000 %Y'
    datetime_obj = datetime.datetime.strptime(date_string, twitter_date_format)
    timestamp = time.mktime(datetime_obj.timetuple())
    return int(timestamp)


# Get only require fields from a raw tweet
def get_required_fields(input_json_string, location_cache):

    # JSON string to JSON object
    in_json = json.loads(input_json_string)

    # Duplicate checking for tweets
    if in_json['id'] in unique_tweets:
        print('Duplicate tweet. Ignoring')
        return None
    unique_tweets[in_json['id']] = True

    # Ignore non-English tweets
    if in_json['lang'] != 'en':
        return None

    out_json = {'id': in_json['id'],
                'lang': in_json['lang'],
                'text': in_json['text']
                }

    try:
        # Find latitude and longitude from user location field
        if in_json['user']['location']:
            twitter_location = in_json['user']['location']

            # If location not cached, use geocoding API
            if twitter_location in location_cache:
                print('Location {0} cached as {1}, {2}'.format(twitter_location,
                                                               location_cache[twitter_location][0],
                                                               location_cache[twitter_location][1]))

                out_json['latitude'] = location_cache[twitter_location][0]
                out_json['longitude'] = location_cache[twitter_location][1]
            else:
                print('Location not cached. Fetching from GeoCoder')

                try:
                    location = geocoder.geocode(twitter_location)

                    # If geocoder didn't find location, return None
                    if not location:
                        return None

                    location_cache[twitter_location] = (location[0], location[1])
                    location_cache[twitter_location] = (location[0], location[1])
                    out_json['latitude'] = location_cache[twitter_location][0]
                    out_json['longitude'] = location_cache[twitter_location][1]

                except Exception as e:
                    print('Exception while getting required fields:', str(e))
                    return None

        # If no location in tweet, return None
        else:
            return None

        # Convert twitter date to timestamp
        out_json['timestamp'] = get_timestamp_from_date(in_json['created_at'])

    except (KeyError, GeocoderTimedOut) as e:
        print('Exception while filtering tweet:', str(e))
        return None

    return json.dumps(out_json)


def filter_datapoint(datapoint):

    if datapoint is None:
        return None

    # Load the location_cache if exists
    location_cache = get_location_cache()

    # Filter only the relevant fields
    filtered_datapoint = get_required_fields(datapoint, location_cache)

    # Save location cache to file
    save_location_cache(location_cache)

    return filtered_datapoint


# Filter all raw tweets in a file
def filter_tweets(input_file):

    # Assumes that the input_file has .json extension
    output_file = input_file[:-5] + '_filtered.json'

    # Load the location_cache if exists
    location_cache = get_location_cache()
    print('Loaded location cache of size', len(location_cache))

    # Create output file
    open(output_file, 'w', encoding='utf-8')

    results = []
    filter_tweet_count = 0
    for i, tweet in enumerate(open(input_file, 'r', encoding='utf-8')):
        filtered_tweet = get_required_fields(tweet, location_cache)
        if filtered_tweet is not None:
            results.append(filtered_tweet)
            filter_tweet_count += 1

            if filter_tweet_count % 50 == 0:
                print('Filtered {0} tweets from {1} tweets'.format(filter_tweet_count, i+1))

                # Save location cache to file
                save_location_cache(location_cache)

                # Save results so far to file
                with open(output_file, 'a', encoding='utf-8') as out_file:
                    print('Saving {0} to file'.format(len(results)))
                    for row in results:
                        out_file.write(row)
                        out_file.write('\n')
                    results = []

    with open(output_file, 'a', encoding='utf-8') as out_file:
        print('Saving {0} to file'.format(len(results)))
        for row in results:
            out_file.write(row)
            out_file.write('\n')


if __name__ == '__main__':
    raw_twitter_data_file = '/Users/apple/twitter_data/TRUDEUA/2018-07-10_historic.json'
    filter_tweets(raw_twitter_data_file)
