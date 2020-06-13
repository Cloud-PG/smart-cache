# import modin.pandas as pd
import numpy as np
import pandas as pd
import plotly.express as px
from plotly.graph_objs import Layout
from tqdm import tqdm

from ..utils import STATUS_ARROW

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
                self._features.append(key)
                setattr(self, key, value.get('keys'))

    def check_all_features(self):
        for feature in tqdm(self._features, desc=f"{STATUS_ARROW}Check features",
                            ascii=True, position=1):
            np_hist = self.check_bins_of(feature)
            self.plot_bins_of(feature, np_hist)

    def check_bins_of(self, feature: str):
        if feature in self._features:
            if feature == 'size':
                sizes = (self._df['Size'] / 1024**2).astype(int)
                return np.histogram(
                    sizes.to_numpy(),
                    bins=self.size,
                    density=False
                )
            elif feature == 'numReq':
                files = self._df[['Filename', 'reqDay']]
                numReqXDay = np.array([])
                for _, day in tqdm(files.groupby('reqDay'),
                                   desc=f"{STATUS_ARROW}Calculate frequencies x day",
                                   ascii=True, position=0):
                    numReqXDay = np.concatenate([
                        numReqXDay, day.Filename.value_counts().to_numpy()
                    ])
                return np.histogram(
                    numReqXDay,
                    bins=self.numReq,
                    density=False
                )
            elif feature == 'deltaLastRequest':
                delta_files = []
                files = {}
                for idx, filename in tqdm(enumerate(self._df.Filename),
                                          desc=f"{STATUS_ARROW}Calculate delta times",
                                          ascii=True, position=0,
                                          total=len(self._df.index)):
                    if filename not in files:
                        files[filename] = idx
                    else:
                        cur_delta = idx - files[filename]
                        files[filename] = idx
                        delta_files.append(cur_delta)
                return np.histogram(
                    delta_files,
                    bins=self.deltaLastRequest,
                    density=False
                )
            else:
                raise Exception(
                    f"ERROR: feature {feature} can not be checked...")
        else:
            raise Exception(
                f"ERROR: feature {feature} has no bins or is not there..."
            )

    def plot_bins_of(self, feature: str, np_hist: tuple):
        counts, bins = np_hist
        fig = px.bar(
            x=[str(cur_bin) for cur_bin in bins],
            y=(counts / counts.sum()) * 100.,
            title=f"Feature {feature}",
        )
        # fig.update_xaxes(type="log")
        fig.update_yaxes(type="log")
        fig.update_layout(_LAYOUT)
        fig.update_layout(
            xaxis_title="bin",
            yaxis_title="%",
            xaxis={
                'type': "category",
            }
        )
        # fig.update_yaxes(type="linear")
        # fig.show()
        fig.write_image(f"feature_{feature}_bins.png")
