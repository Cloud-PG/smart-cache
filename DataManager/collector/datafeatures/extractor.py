class CMSDataPopularity(object):

    def __init__(self, data):
        self.__data = data
        self.__features = {}
        self.__extract_features()
    
    def __extract_features(self):
        cur_file = self.__data['FileName']
        if cur_file != "unknown":
            logical_file_name = [
                elm for elm in cur_file.split("/") if elm]
            try:
                store_type, campain, process, file_type = logical_file_name[1:5]
                self.__features['store_type'] = store_type
                self.__features['campain'] = campain
                self.__features['process'] = process
                self.__features['file_type'] = file_type
            except ValueError:
                raise Exception("Cannot extract features from '{}'".format(cur_file))
    
    def features(self):
        pass