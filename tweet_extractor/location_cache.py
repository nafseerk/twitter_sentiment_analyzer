import os
import pickle

location_cache_file = './location_cache.pickle'
reverse_location_cache_file = './reverse_location_cache.pickle'


def get_location_cache():

    location_cache = {}
    if os.path.exists(location_cache_file):
        location_cache = pickle.load(open(location_cache_file, "rb"))

    print('Cache Size', len(location_cache))

    return location_cache


def save_location_cache(location_cache):
    pickle.dump(location_cache, open(location_cache_file, "wb"))


def get_reverse_location_cache():
    reverse_location_cache = {}
    if os.path.exists(reverse_location_cache_file):
        reverse_location_cache = pickle.load(open(reverse_location_cache_file, "rb"))

    print('Reverse Cache Size', len(reverse_location_cache))

    return reverse_location_cache


def save_reverse_location_cache(reverse_location_cache):
    pickle.dump(reverse_location_cache, open(reverse_location_cache_file, "wb"))


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

    # The below code tests and cleans up the reverse location cache
    reverse_loc_cache = get_reverse_location_cache()
    print('Size of Reverse Cache =', len(reverse_loc_cache))

    count = 0
    for key in reverse_loc_cache.keys():
        country_name, country_code = reverse_loc_cache[key]
        if country_name is None or country_code is None:
            count += 1
            del reverse_loc_cache[key]

    print('Size of Reverse Cache = {0}, Removed {1} invalid entries'.format(len(reverse_loc_cache), count))
    save_reverse_location_cache(reverse_loc_cache)
