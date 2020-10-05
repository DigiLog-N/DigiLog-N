from DataSource import DataSource


class WeatherDataSource(DataSource):
    def __init__(self, name, data=None):
        if data:
            del (data['_id'])
            self.d = data
        else:
            self.d = {'name': name}
            self.d['fields'] = {}
            self.d['path_to_plasma_file'] = None
            self.d['description'] = None
            self.d['provider'] = None
            self.d['latitude'] = None
            self.d['longitude'] = None
            self.d['elevation'] = None
            self.d['state'] = None

    def set_latitude(self, latitude):
        self.d['latitude'] = latitude

    def set_longitude(self, longitude):
        self.d['longitude'] = longitude

    def set_elevation(self, elevation):
        self.d['elevation'] = elevation

    def set_state(self, state):
        self.d['state'] = state

    def get_latitude(self):
        return self.d['latitude']

    def get_longitude(self):
        return self.d['longitude']

    def get_elevation(self):
        return self.d['elevation']

    def get_state(self):
        return self.d['state']

