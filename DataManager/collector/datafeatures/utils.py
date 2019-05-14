import hashlib
import json

import numpy as np


class FeatureData(object):

    """A basic object that contains and manages features."""

    def __init__(self):
        self._id = None
        self._features = {}

    def add_feature(self, name, value):
        """Insert a feature."""
        self._features[name] = value

    def __getstate__(self):
        """Make object serializable by pickle.

        Note: By default it takes the dict representation of the object.
        """
        return self.to_dict()

    def __setstate__(self, state):
        """Make object loaded by pickle."""
        raise NotImplementedError

    def to_dict(self):
        """This function have to be implemented by the derived object."""
        raise NotImplementedError

    def _gen_id(self):
        blake2s = hashlib.blake2s()
        blake2s.update(json.dumps(list(self.features)).encode("utf-8"))
        self._id = blake2s.hexdigest()

    @property
    def record_id(self) -> str:
        if self._id is None:
            self._gen_id()
        return self._id

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

    def dumps(self) -> str:
        return json.dumps(self.to_dict())

    def features2array(self):
        """Get the feature values as numpy array, ordered by feature names."""
        tmp = []
        for feature in sorted(self._features):
            tmp.append(self._features[feature])
        return np.array(tmp)

    def __repr__(self) -> str:
        """Get the printable representation of feature object."""
        return json.dumps(self._features)
