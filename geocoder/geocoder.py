from geopy.geocoders import Nominatim, GoogleV3, Yandex, ArcGIS, GeocodeFarm, Photon
import time


class TimedGeocoder:

    def __init__(self, geocoder):

        # The geocoder object that is to be time-tracked
        self.geocoder = geocoder

        # timestamp of last availability
        self.last_available_time = time.time()

        # Waiting period in minutes before retrying the geocoder
        self.waiting_period = 60

        # geocoder availability
        self.available = True

    def is_available(self):
        return self.available

    def geocode(self, location):

        if self.available:
            try:
                location_obj = self.geocoder.geocode(location)
                if location_obj:
                    print('geocoder {0} returned ({1}, {2}) for location {3}'.format(type(self.geocoder).__name__,
                                                                                     location_obj.latitude,
                                                                                     location_obj.longitude,
                                                                                     location))
                else:
                    print('geocoder {0} returned None for location {1}'.format(type(self.geocoder).__name__,
                                                                               location))
                self.last_available_time = time.time()
                return location_obj
            # Any exception causes the geocoder to be unavailable.
            except Exception as e:
                print('Exception using geocoder', type(self.geocoder).__name__, str(e))
                self.available = False
                return None

        else:
            current_time = time.time()
            test_location = 'Waterloo, Ontario, Canada'
            if (current_time - self.last_available_time) / 60 > self.waiting_period:
                try:
                    if self.geocoder.geocode(test_location) is not None:
                        self.available = True
                        self.last_available_time = time.time()
                        return self.geocode(location)

                # Still unavailable. Double the waiting period
                except Exception:
                    self.waiting_period *= 2


class Geocoder:

    def __init__(self):
        self.geocoders = [
            TimedGeocoder(Nominatim()),
            TimedGeocoder(GoogleV3()),
            TimedGeocoder(Yandex()),
            TimedGeocoder(ArcGIS()),
            TimedGeocoder(GeocodeFarm()),
            TimedGeocoder(Photon())
        ]

        self.next = 0
        self.size = len(self.geocoders)

    def geocode(self, location):
        starting_geocoder = self.next

        if self.next == self.size - 1:
            time.sleep(1)

        while True:
            location_obj = None
            try:
                if self.geocoders[self.next].is_available():
                    location_obj = self.geocoders[self.next].geocode(location)
                self.next = (self.next + 1) % self.size
            except Exception:
                self.next = (self.next + 1) % self.size

            if location_obj:
                return location_obj.latitude, location_obj.longitude

            if self.next == starting_geocoder:
                return None


if __name__ == '__main__':
    geocoder = Geocoder()
    test_location = 'Waterloo, Ontario, Canada'

    for i in range(geocoder.size):
        location_result = geocoder.geocoders[i].geocode(test_location)
