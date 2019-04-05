class ReadableDictAsAttribute(object):

    def __init__(self, obj: dict):
        self.__dict = obj
        if 'support_tables' in self.__dict:
            self.__dict['support_tables'] = SupportTables(self.__dict['support_tables'])

    @property
    def list(self):
        return list(self.__dict.keys())

    def __getattr__(self, name):
        return self.__dict[name]
    
    def __repr__(self):
        return json.dumps(self.__dict, indent=2)