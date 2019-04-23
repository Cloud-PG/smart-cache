import json


class FeatureData(object):

    """A basic object that contains and manages features."""

    def __init__(self):
        self._features = {}

    def add_feature(self, name, value):
        """Insert a feature."""
        self._features[name] = value

    def __getstate__(self):
        """Make object serializable by pickle."""
        return self.to_dict()

    def __setstate__(self, state):
        """Make object loaded by pickle."""
        raise NotImplementedError

    def to_dict(self):
        """This function have to be implemented by the derived object."""
        raise NotImplementedError

    @property
    def features(self):
        """Generate the features list of tuple, ordered by feature name."""
        for feature in sorted(self._features):
            yield feature, self._features[feature]

    @property
    def feature_list(self):
        """Get the features as a list of tuple ordered by feature name."""
        return [
            (feature, self._features[feature])
            for feature in sorted(self._features)
        ]

    @property
    def feature(self):
        """Get the features dict."""
        return self._features

    @property
    def feature_dict(self):
        """Get the features dict.

        NOTE: Alias for feature, used just for sugar syntax
        """
        return self._features

    def features2array(self):
        """Get the feature values as numpy array, ordered by feature names."""
        tmp = []
        for feature in sorted(self._features):
            tmp.append(self._features[feature])
        return np.array(tmp)

    def __repr__(self):
        """Get the printable representation of feature object."""
        return json.dumps(list(self.features))
