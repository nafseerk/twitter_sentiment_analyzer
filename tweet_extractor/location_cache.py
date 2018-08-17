import os
import pickle

location_cache_file = './location_cache.pickle'


def get_location_cache(file_loc=location_cache_file):

    location_cache = {}
    if os.path.exists(file_loc):
        location_cache = pickle.load(open(file_loc, "rb"))

    print('Cache Size', len(location_cache))

    return location_cache


def save_location_cache(location_cache, file_loc=location_cache_file):
    pickle.dump(location_cache, open(file_loc, "wb"))


if __name__ == '__main__':

    # The below code tests and cleans up the location cache
    loc_cache = get_location_cache()
    print('Size of Cache =', len(loc_cache))

    count = 0
    for key in loc_cache.keys():
        location = loc_cache[key]
        if location[0] is None or location[1] is None:
            count += 1
            del loc_cache[key]

    print('Size of Cache = {0}, Removed {1} invalid entries'.format(len(loc_cache), count))
    save_location_cache(loc_cache)
