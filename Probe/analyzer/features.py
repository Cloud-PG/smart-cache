# import modin.pandas as pd
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
                 concatenated: bool = True,
                 output_folder: str = "analysis",
                 region: str = 'all',
                 group_by: str = 'd'):
        self._df: 'pd.DataFrame' = df
        self._concatenated: bool = concatenated
        self._region: str = region
        self._group_by: str = group_by
        self._filter_data(concatenated)
        self._add_group_column()

        self._output_folder = Path(output_folder)
        self._output_folder.mkdir(parents=True, exist_ok=True)

        self._features = []
        self._features_data = {}
        for key, value in features.items():
            if value.get('type', False) == "float" and value.get('buckets', False):
                cur_values = []
                cur_values.extend(value.get('keys'))
                if value.get('bucket_open_right', False):
                    cur_values.extend([cur_values[-1]*2])
                self._features.append(key)
                setattr(self, key, cur_values)

    def _add_group_column(self):
        if self._concatenated:
            self._df['datetime'] = pd.to_datetime(self._df.reqDay, unit='s')
            if self._group_by == 'd':
                self._df['day'] = self._df.datetime.dt.day
            elif self._group_by == 'w':
                self._df['week'] = self._df.datetime.dt.week
            elif self._group_by == 'm':
                self._df['month'] = self._df.datetime.dt.month
        else:
            for cur_df in self._df:
                cur_df['datetime'] = pd.to_datetime(cur_df.reqDay, unit='s')
                if self._group_by == 'd':
                    cur_df['day'] = cur_df.datetime.dt.day
                elif self._group_by == 'w':
                    cur_df['week'] = cur_df.datetime.dt.week
                elif self._group_by == 'm':
                    cur_df['month'] = cur_df.datetime.dt.month

    def _filter_data(self, concatenated: bool = True):
        print(f"{STATUS_ARROW}Filter DataType data and mc")
        if concatenated:
            if self._df.DataType.dtype == np.int64:
                if self._region == 'it':
                    self._df = self._df[
                        (self._df.DataType == 0) | (self._df.DataType == 1)
                    ]
                elif self._region == 'us':
                    self._df = self._df[
                        (self._df.DataType == 0) | (self._df.DataType == 3)
                    ]
            else:
                self._df = self._df[
                    (self._df.DataType == "data") | (self._df.DataType == "mc")
                ]
        else:
            for idx in tqdm(range(len(self._df))):
                cur_df = self._df[idx]
                if cur_df.DataType.dtype == np.int64:
                    if self._region == 'it':
                        self._df[idx] = cur_df[
                            (cur_df.DataType == 0) | (cur_df.DataType == 1)
                        ]
                    elif self._region == 'us':
                        self._df[idx] = cur_df[
                            (cur_df.DataType == 0) | (cur_df.DataType == 3)
                        ]
                else:
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
                            ascii=True):
            np_hist = self.check_bins_of(feature)
            self.plot_bins_of(feature, np_hist)
            self.plot_violin_of(feature, np_hist)

    def check_bins_of(self, feature: str, n_bins: int = 6):
        all_data = None
        if feature == 'size':
            if self._concatenated:
                sizes = (self._df['Size'] / 1024**2).astype(int).to_numpy()
            else:
                sizes = np.array([])
                for cur_df in tqdm(
                        self._df,
                        desc=f"{STATUS_ARROW}Calculate sizes x day",
                        ascii=True):
                    sizes = np.concatenate([
                        sizes, (cur_df['Size'] / 1024 **
                                2).astype(int).to_numpy()
                    ])
            self._features_data[feature] = sizes
            all_data = sizes
        elif feature == 'numReq':
            groups = None
            if self._concatenated:
                if self._group_by == 'd':
                    groups = self._df.groupby('reqDay')
                elif self._group_by == 'w':
                    groups = self._df.groupby('week')
                elif self._group_by == 'm':
                    groups = self._df.groupby('month')
            else:
                if self._group_by == 'd':
                    groups = [
                        (idx, cur_df)
                        for idx, cur_df in enumerate(self._df)
                    ]
                else:
                    if self._group_by == 'w':
                        group_by = 'week'
                    elif self._group_by == 'm':
                        group_by = 'month'
                    groups = {}
                    for cur_df in self._df:
                        for week, cur_week in cur_df.groupby(group_by):
                            if week not in groups:
                                groups[week] = cur_week
                            else:
                                groups[week] = pd.concat([
                                    groups[week],
                                    cur_week,
                                ], ignore_index=True)
                    groups = [
                        (group_key, groups[group_key])
                        for group_key in sorted(groups)
                    ]
            numReqXGroup = np.array([])
            for _, group in tqdm(groups,
                                 desc=f"{STATUS_ARROW}Calculate frequencies x day",
                                 ascii=True):
                numReqXGroup = np.concatenate([
                    numReqXGroup, group.Filename.value_counts().to_numpy()
                ])
            self._features_data[feature] = numReqXGroup
            all_data = numReqXGroup
        elif feature == 'deltaLastRequest':
            delta_files = []
            files = {}
            all_files = None
            if self._concatenated:
                all_files = self._df.Filename
                tot_files = len(all_files.index)
            else:
                all_files = np.concatenate([cur_df.Filename.to_numpy()
                                            for cur_df in self._df])
                tot_files = len(all_files)
            for idx, filename in tqdm(enumerate(all_files),
                                      desc=f"{STATUS_ARROW}Calculate delta times",
                                      ascii=True,
                                      total=tot_files):
                if filename not in files:
                    files[filename] = idx
                else:
                    cur_delta = idx - files[filename]
                    files[filename] = idx
                    delta_files.append(cur_delta)
            delta_files = np.array(delta_files)
            self._features_data[feature] = delta_files
            all_data = delta_files
        else:
            raise Exception(
                f"ERROR: feature {feature} can not be checked...")

        if feature in self._features:
            cur_bins = np.array(getattr(self, feature))
        else:
            _, cur_bins = np.histogram(
                sizes,
                bins=n_bins,
                density=False
            )

        prev_bin = 0.
        counts = []
        for bin_idx, cur_bin in enumerate(cur_bins):
            if bin_idx != cur_bins.shape[0] - 1:
                cur_count = all_data[
                    (all_data > prev_bin) &
                    (all_data <= cur_bin)
                ].shape[0]
            else:
                cur_count = all_data[
                    (all_data > prev_bin)
                ].shape[0]
            counts.append(cur_count)
            prev_bin = cur_bin

        counts = np.array(counts)
        return counts, cur_bins

    def plot_bins_of(self, feature: str, np_hist: tuple):
        counts, bins = np_hist
        # print(counts, bins)
        percentages = (counts / counts.sum()) * 100.
        percentages[np.isnan(percentages)] = 0.
        fig = px.bar(
            x=[str(cur_bin) for cur_bin in bins],
            y=percentages,
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
        # print(f"{STATUS_ARROW}Save bin plot of {feature} as png")
        # fig.write_image(
        #     self._output_folder.joinpath(
        #         f"feature_{feature}_bins.png"
        #     ).as_posix()
        # )
        print(f"{STATUS_ARROW}Save bin plot of {feature} as html")
        fig.write_html(
            self._output_folder.joinpath(
                f"feature_{feature}_bins.html"
            ).as_posix()
        )

    def plot_violin_of(self, feature: str, np_hist: tuple):
        _, bins = np_hist
        cur_feature_data = self._features_data[feature]
        fig = go.Figure()

        fig.add_trace(
            go.Violin(
                y=cur_feature_data,
                x0=0,
                name="global",
                box_visible=True,
                meanline_visible=True,
            )
        )
        prev_bin = 0.
        for bin_idx, cur_bin in enumerate(bins, 1):
            if bin_idx != bins.shape[0]:
                cur_data = cur_feature_data[
                    (cur_feature_data > prev_bin) &
                    (cur_feature_data <= cur_bin)
                ]
            else:
                cur_data = cur_feature_data[
                    (cur_feature_data > prev_bin)
                ]
            fig.add_trace(
                go.Violin(
                    y=cur_data,
                    x0=bin_idx,
                    name=str(cur_bin),
                    box_visible=True,
                    meanline_visible=True,
                    # points="all",
                )
            )
            prev_bin = cur_bin
        fig.update_layout(_LAYOUT)
        fig.update_layout({
            'title': f"Feature {feature}",
            'xaxis': {
                'tickmode': 'array',
                'tickvals': list(range(len(bins)+1)),
                'ticktext': ['global'] + [str(cur_bin) for cur_bin in bins]
            }
        })
        # fig.show()
        # print(f"{STATUS_ARROW}Save violin plot of {feature} as pnh")
        # fig.write_image(
        #     self._output_folder.joinpath(
        #         f"feature_{feature}_violin.png"
        #     ).as_posix()
        # )
        print(f"{STATUS_ARROW}Save violin plot of {feature} as html")
        fig.write_html(
            self._output_folder.joinpath(
                f"feature_{feature}_violin.html"
            ).as_posix()
        )
