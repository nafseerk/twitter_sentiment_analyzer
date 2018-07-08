import json

from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim


# Get only require fields from a raw tweet
def get_required_fields(input_json_string):

    # JSON string to JSON object
    in_json = json.loads(input_json_string)

    # All the required fields at the first level of nesting
    required_fields = ['id', 'text', 'lang', 'timestamp_ms']

    # Fetch the required fields
    out_json = {key: in_json[key] for key in required_fields}

    # Find latitude and longitude from user location field
    try:
        geo_locator = Nominatim()
        location = geo_locator.geocode(in_json['user']['location'])

        # If location present, use this JSON
        if location:
            out_json['latitude'] = location.latitude
            out_json['longitude'] = location.longitude
            return True, json.dumps(out_json)

        # If no location present, ignore this JSON
        else:
            return False, json.dumps(out_json)

    except GeocoderTimedOut:
        return False, json.dumps(out_json)


# Filter all raw tweets in a file
def filter_tweets_reqd_fields(input_file):

    # Assumes that the input_file has .json extension
    output_file = input_file[:-5] + '_filtered.json'

    with open(output_file, 'w') as out_file:
        filter_tweet_count = 0
        for tweet in open(input_file, 'r'):
            has_location, filtered_tweet = get_required_fields(tweet)
            # print(filtered_tweet)
            if has_location:
                out_file.write(filtered_tweet)
                out_file.write('\n')
            filter_tweet_count += 1

            if filter_tweet_count % 50 == 0:
                print('Filtered {0} tweets'.format(filter_tweet_count))


if __name__ == '__main__':
    raw_twitter_data_file = '/Users/apple/twitter_data/RUSCRO/2018-07-07_live.json'
    filter_tweets_reqd_fields(raw_twitter_data_file)
