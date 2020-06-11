import modin.pandas as pd
import numpy as np
import plotly.express as px
from plotly.graph_objs import Layout

_LAYOUT = Layout(
    paper_bgcolor='rgb(255,255,255)',
    plot_bgcolor='rgb(255,255,255)',
    yaxis={'gridcolor': 'black'},
    xaxis={'gridcolor': 'black'},
)


class Features(object):

    def __init__(self, features: dict, df: 'pd.DataFrame'):
        self._df = df
        self._features = []
        for key, value in features.items():
            if value.get('type', False) == "float" and value.get('buckets', False):
                cur_values = [0.]
                cur_values.extend(value.get('keys'))
                self._features.append(key)
                setattr(self, key, cur_values)

    def check_bins_of(self, feature: str):
        if feature in self._features:
            if feature == 'size':
                return np.histogram(
                    self._df['Size'].to_numpy() / 1024**2,
                    bins=self.size,
                    # density=True
                )
        else:
            raise Exception(
                f"ERROR: feature {feature} has no bins or is not there..."
            )

    def plot_bins_of(self, np_hist):
        counts, bins = np_hist
        fig = px.bar(
            x=bins[1:], y=counts, labels={'x': 'bin', 'y': 'count'},
            # log_y=True,
        )
        fig.layout = _LAYOUT
        fig.show()
