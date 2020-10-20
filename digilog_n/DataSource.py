import pymongo
import json


class DataSource:
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

    # TODO: Setters and getters are not very pythonic, but perhaps for now,
    #  it's suitable to use them to give the form definition.
    def set_path_to_plasma_file(self, path_to_plasma_file):
        self.d['path_to_plasma_file'] = path_to_plasma_file

    def set_description(self, description):
        self.d['description'] = description

    def set_provider(self, provider):
        self.d['provider'] = provider

    def get_name(self):
        return self.d['name']

    def get_path_to_plasma_file(self):
        return self.d['path_to_plasma_file']

    def get_description(self):
        return self.d['description']

    def get_provider(self):
        return self.d['provider']

    def add_field(self, name, units, precision, description, data_type):
        if name in self.d['fields']:
            raise ValueError("A field named '%s' already exists. Use update_field()." % name)

        self.d['fields'][name] = {'name': name, 'units': units, 'precision': precision, 'description': description, 'data_type': data_type}

    def update_field(self, name, units=None, precision=None, description=None, data_type=None):
        if not name in self.d['fields']:
            raise ValueError("A field named '%s' does not exist. Use add_field()." % name)

        if units:
            self.d['fields'][name]['units'] = units

        if precision:
            self.d['fields'][name]['precision'] = precision

        if description:
            self.d['fields'][name]['description'] = description

        if data_type:
            self.d['fields'][name]['data_type'] = data_type

    def delete_field(self, name):
        if not name in self.d['fields']:
            raise ValueError("A field named '%s' does not exist." % name)

        del(self.d['fields'][name])

    def get_fields(self):
        return self.d['fields']

    def json(self):
        return json.dumps(self.d, indent=4)
