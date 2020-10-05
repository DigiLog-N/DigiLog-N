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
            # self.d['field_order'] = []
            self.d['path_to_plasma_file'] = None
            self.d['description'] = None
            self.d['provider'] = None

    def set_path_to_plasma_file(self, path_to_plasma_file):
        self.d['path_to_plasma_file'] = path_to_plasma_file

    def set_description(self, description):
        self.d['description'] = description

    def set_provider(self, provider):
        self.d['provider'] = provider

    def add_field(self, name, units, precision, description):
        if name in self.d['fields']:
            raise ValueError("A field named '%s' already exists. Use update_field()." % name)

        self.d['fields'][name] = {'name': name, 'units': units, 'precision': precision, 'description': description}
        # self.d['field_order'].append(name)

    def update_field(self, name, units=None, precision=None, description=None):
        if not name in self.d['fields']:
            raise ValueError("A field named '%s' does not exist. Use add_field()." % name)

        if units:
            self.d['fields'][name]['units'] = units

        if precision:
            self.d['fields'][name]['precision'] = precision

        if description:
            self.d['fields'][name]['description'] = description

    def delete_field(self, name):
        if not name in self.d['fields']:
            raise ValueError("A field named '%s' does not exist." % name)

        del(self.d['fields'][name])
        # self.d['field_order'] = [x for x in self.d['field_order'] if x != name]

    def get_fields(self):
        return self.d['fields']

    '''
    def reorder_fields(self, list_of_names):
        for name in list_of_names:
            if not name in self.d['field_order']:
                raise ValueError("A field named '%s' does not exist." % name)

        for name in self.d['field_order']:
            if not name in list_of_names:
                raise ValueError("You must include '%s' in your new field order." % name)

        self.d['field_order'] = list_of_names
    '''

    def json(self):
        return json.dumps(self.d, indent=4)
