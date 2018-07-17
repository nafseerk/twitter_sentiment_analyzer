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
    print('Size of Cache =',len(location_cache))

    count = 0
    for key in location_cache.keys():
        location = location_cache[key]
        if location[0] is None or location[1] is None:
            count += 1
            del location_cache[key]

    print('Size of Cache = {0}, Removed {1} invalid entries'.format(len(location_cache), count))
    save_location_cache(location_cache)
