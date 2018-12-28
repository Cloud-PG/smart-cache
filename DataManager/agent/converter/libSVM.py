
from .common import DataConverter

__all__ = ['LibSVMConverter']


class LibSVMConverter(DataConverter):

    def __get_features(self, data, exclude=[]):
        feature_names = [key for key in sorted(
            data.keys()) if key not in exclude]
        features = ["{}:{}".format(
            idx, data[feature_name]
        ) for idx, feature_name in enumerate(feature_names, 1)]
        return " ".join(features)

    def row(self, data):
        return None

    def labeled_row(self, data, label='label'):
        if isinstance(data, dict):
            assert label in data, "Label have to be a valid field of data"
            label_val = data[label]
            features_val = self.__get_features(data, [label])
        else:
            raise Exception(
                "Type '{}' cannot be converted to libSVN".format(type(data)))

        return "{label_val} {features_val}".format(
            label_val=label_val,
            features_val=features_val
        )
