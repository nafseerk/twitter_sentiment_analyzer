import json
import datetime
import time
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim


# Convert twitter date to posix timestamp
def get_timestamp_from_date(date_string):
    twitter_date_format = '%a %b %d %H:%M:%S +0000 %Y'
    datetime_obj = datetime.datetime.strptime(date_string, twitter_date_format)
    timestamp = time.mktime(datetime_obj.timetuple())
    return int(timestamp)


# Get only require fields from a raw tweet
def get_required_fields(input_json_string):

    # JSON string to JSON object
    in_json = json.loads(input_json_string)

    try:
        out_json = {}

        out_json['id'] = in_json['id']

        # Find latitude and longitude from user location field
        geo_locator = Nominatim()
        location = geo_locator.geocode(in_json['user']['location'])
        if not location:
            return None
        out_json['latitude'] = location.latitude
        out_json['longitude'] = location.longitude

        # Convert twitter date to timestamp
        out_json['timestamp'] = get_timestamp_from_date(in_json['created_at'])

        # filter tweet text and language
        out_json['lang'] = in_json['lang']
        out_json['text'] = in_json['text']

    except (KeyError, GeocoderTimedOut) as e:
        print('Exception while filtering tweet:', str(e))
        return None

    return json.dumps(out_json)


# Filter all raw tweets in a file
def filter_tweets(input_file):

    # Assumes that the input_file has .json extension
    output_file = input_file[:-5] + '_filtered.json'

    with open(output_file, 'w') as out_file:
        filter_tweet_count = 0
        for tweet in open(input_file, 'r'):
            filtered_tweet = get_required_fields(tweet)
            # print(filtered_tweet)
            if filtered_tweet is not None:
                out_file.write(filtered_tweet)
                out_file.write('\n')
                filter_tweet_count += 1

                if filter_tweet_count % 100 == 0:
                    print('Filtered {0} tweets'.format(filter_tweet_count))


if __name__ == '__main__':
    raw_twitter_data_file = '/Users/apple/twitter_data/TRUMP/2018-07-08_historic.json'
    filter_tweets(raw_twitter_data_file)
