import os
import pickle

location_cache_file = './location_cache.pickle'


def get_location_cache():

    location_cache = {}
    if os.path.exists(location_cache_file):
        location_cache = pickle.load(open(location_cache_file, "rb"))

    print('Cache Size', len(location_cache))

    return location_cache


def save_location_cache(location_cache):
    pickle.dump(location_cache, open(location_cache_file, "wb"))


if __name__ == '__main__':

    location_cache = get_location_cache()
