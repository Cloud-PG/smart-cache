# import modin.pandas as pd
from typing import List

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

    def __init__(self, features: dict, df: 'pd.DataFrame',
                 concatenated: bool = True):
        self._df = df
        self._concatenated = concatenated
        self._filter_data(concatenated)

        self._features = []
        for key, value in features.items():
            if value.get('type', False) == "float" and value.get('buckets', False):
                cur_values = []
                cur_values.extend(value.get('keys'))
                if value.get('bucket_open_right', False):
                    cur_values.extend([cur_values[-1]*2])
                self._features.append(key)
                setattr(self, key, cur_values)

    def _filter_data(self, concatenated: bool = True):
        print(f"{STATUS_ARROW}Filter DataType data and mc")
        if concatenated:
            self._df = self._df[
                (self._df.DataType == "data") | (self._df.DataType == "mc")
            ]
        else:
            for idx in tqdm(range(len(self._df))):
                cur_df = self._df[idx]
                self._df[idx] = cur_df[
                    (cur_df.DataType == "data") | (cur_df.DataType == "mc")
                ]

        print(f"{STATUS_ARROW}Filter success jobs")
        if concatenated:
            self._df = self._df[self._df.JobSuccess.astype(bool)]
        else:
            for idx in tqdm(range(len(self._df))):
                cur_df = self._df[idx]
                self._df[idx] = cur_df[cur_df.JobSuccess.astype(bool)]

    def check_all_features(self, features: List[str] = []):
        cur_features = []
        if features:
            cur_features.extend(features)
        else:
            cur_features.extend(self._features)
        for feature in tqdm(cur_features,
                            desc=f"{STATUS_ARROW}Check features",
                            ascii=True, position=1):
            np_hist = self.check_bins_of(feature)
            self.plot_bins_of(feature, np_hist)

    def check_bins_of(self, feature: str, n_bins: int = 6):
        cur_bins = getattr(
            self, feature) if feature in self._features else n_bins
        if feature == 'size':
            if self._concatenated:
                sizes = (self._df['Size'] / 1024**2).astype(int).to_numpy()
            else:
                sizes = np.array([])
                for cur_df in tqdm(
                        self._df,
                        desc=f"{STATUS_ARROW}Calculate sizes x day",
                        ascii=True, position=0):
                    sizes = np.concatenate([
                        sizes, (cur_df['Size'] / 1024 **
                                2).astype(int).to_numpy()
                    ])
            return np.histogram(
                sizes,
                bins=cur_bins,
                density=False
            )
        elif feature == 'numReq':
            files_x_day = None
            if self._concatenated:
                files = self._df[['Filename', 'reqDay']]
                files_x_day = files.groupby('reqDay')
            else:
                files_x_day = [
                    (idx, cur_df[['Filename', 'reqDay']].copy())
                    for idx, cur_df in enumerate(self._df)
                ]
            numReqXDay = np.array([])
            for _, day in tqdm(files_x_day,
                               desc=f"{STATUS_ARROW}Calculate frequencies x day",
                               ascii=True, position=0):
                numReqXDay = np.concatenate([
                    numReqXDay, day.Filename.value_counts().to_numpy()
                ])
            return np.histogram(
                numReqXDay,
                bins=cur_bins,
                density=False
            )
        elif feature == 'deltaLastRequest':
            delta_files = []
            files = {}
            all_files = None
            if self._concatenated:
                all_files = self._df.Filename
                tot_files = len(all_files.index)
            else:
                all_files = (cur_df.Filename for cur_df in self._df)
                tot_files = sum([len(elm.index) for elm in all_files])
            for idx, filename in tqdm(enumerate(all_files),
                                      desc=f"{STATUS_ARROW}Calculate delta times",
                                      ascii=True, position=0,
                                      total=tot_files):
                if filename not in files:
                    files[filename] = idx
                else:
                    cur_delta = idx - files[filename]
                    files[filename] = idx
                    delta_files.append(cur_delta)
            return np.histogram(
                delta_files,
                bins=cur_bins,
                density=False
            )
        else:
            raise Exception(
                f"ERROR: feature {feature} can not be checked...")

    def plot_bins_of(self, feature: str, np_hist: tuple):
        counts, bins = np_hist
        fig = px.bar(
            x=[str(cur_bin) for cur_bin in bins[:-1]],
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
