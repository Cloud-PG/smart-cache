import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
from bokeh.io import export_png
from bokeh.layouts import column, row
from bokeh.models import BasicTickFormatter, ColumnDataSource, Span
from bokeh.palettes import Accent
from bokeh.plotting import figure, output_file, save, show
from bokeh.transform import dodge
from plotly.graph_objs import Layout
from tqdm import tqdm

from ..utils import STATUS_ARROW

LAYOUT = Layout(
    paper_bgcolor='rgb(255,255,255)',
    plot_bgcolor='rgb(255,255,255)',
    yaxis={'gridcolor': 'black'},
    xaxis={'gridcolor': 'black'},
)


class FileStats(object):

    def __init__(self, start_x: int = 0, start_size: int = 0,
                 start_delta_time: int = 0):
        self.x = [start_x]
        self.n_req = [0]
        self.n_users = [0]
        self.n_sites = [0]
        self.delta_times = [0]
        self.last_request = [start_delta_time]
        self.sizes = [start_size]
        self.users = set()
        self.sites = set()


def plot_global_upper_limits(df: 'pd.DataFrame',
                             output_filename: str = 'daily_upper_bounds',
                             output_type: str = 'show',
                             region: str = 'all',
                             concatenated: bool = False,
                             data_generator: bool = True,):

    if concatenated:
        print(f"{STATUS_ARROW}ERROR: you have to use not concatenated data...")
        exit(-1)

    if not data_generator:
        print(f"{STATUS_ARROW}ERROR: you have to use a data generator...")
        exit(-1)

    fig = go.Figure(layout=LAYOUT)

    x = []
    max_hit_rate_list = []
    max_read_on_hit_list = []
    cache_optimal_size = []

    for num, cur_df in enumerate(df, 1):
        print(f"{STATUS_ARROW}[{num:03d}] Filter DataType data and mc")
        cur_df = cur_df[(cur_df.DataType == "data")
                        | (cur_df.DataType == "mc")]

        print(f"{STATUS_ARROW}[{num:03d}] Filter success jobs")
        cur_df = cur_df[cur_df.JobSuccess.astype(bool)]

        print(f"{STATUS_ARROW}[{num:03d}] Calculate limits")

        sum_sizes = cur_df.Size.sum()
        file_sizes = cur_df[['Filename', 'Size']
                            ].drop_duplicates('Filename').Size.sum()
        num_req = len(cur_df.index)
        num_files = cur_df.Filename.nunique()

        max_hit_rate = ((num_req-num_files) / num_req) * 100.
        max_read_on_hit = ((sum_sizes - file_sizes) / sum_sizes) * 100.
        optimal_size = file_sizes

        x.append(pd.to_datetime(cur_df.reqDay.unique()[-1], unit='s'))
        max_hit_rate_list.append(max_hit_rate)
        max_read_on_hit_list.append(max_read_on_hit)
        cache_optimal_size.append(optimal_size / 1024**4)  # TB

    avg_hit_rate = sum(max_hit_rate_list) / len(max_hit_rate_list)
    avg_read_on_hit = sum(max_read_on_hit_list) / len(max_read_on_hit_list)
    avg_cache_opt_size = sum(cache_optimal_size) / len(cache_optimal_size)

    fig.add_trace(
        go.Scatter(
            x=x,
            y=max_hit_rate_list,
            mode='lines',
            name="Hit rate",
        ),
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=max_read_on_hit_list,
            mode='lines',
            name="Read on hit",
        ),
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=cache_optimal_size,
            mode='lines',
            name="Size (TB)",
        ),
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=[avg_hit_rate for _ in range(len(max_hit_rate_list))],
            mode='lines',
            name="Avg. Hit rate",
        ),
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=[avg_read_on_hit for _ in range(len(max_read_on_hit_list))],
            mode='lines',
            name="Read on hit",
        ),
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=[avg_cache_opt_size for _ in range(len(cache_optimal_size))],
            mode='lines',
            name="Avg. Size (TB)",
        ),
    )

    fig.update_layout(
        title="Upper bounds",
        xaxis_title='day',
        yaxis_title='%',
    )

    if output_type == 'html':
        fig.write_html(
            f"{output_filename}.html",
            include_plotlyjs=True,
        )
    elif output_type == 'show':
        fig.show()
    else:
        raise Exception(f"ERROR: output {output_type} not supported...")


def plot_global_stats(df: 'pd.DataFrame',
                      output_filename: str = 'yearstats',
                      output_type: str = 'show',
                      region: str = 'all',
                      concatenated: bool = True):
    ##
    # Howto to group by Week
    # pd.Grouper(key='Date', freq='W-MON')

    print(f"{STATUS_ARROW}Filter DataType data and mc")
    if concatenated:
        df = df[(df.DataType == "data") | (df.DataType == "mc")]
    else:
        for idx in tqdm(range(len(df))):
            cur_df = df[idx]
            df[idx] = cur_df[(cur_df.DataType == "data")
                             | (cur_df.DataType == "mc")]

    print(f"{STATUS_ARROW}Filter success jobs")
    if concatenated:
        df = df[df.JobSuccess.astype(bool)]
    else:
        for idx in tqdm(range(len(df))):
            cur_df = df[idx]
            df[idx] = cur_df[cur_df.JobSuccess.astype(bool)]

    print(f"{STATUS_ARROW}Add size in GigaBytes")
    if concatenated:
        df['size (GB)'] = df.Size / 1024**3
    else:
        for idx in tqdm(range(len(df))):
            cur_df = df[idx]
            df[idx]['size (GB)'] = cur_df.Size / 1024**3

    print(f"{STATUS_ARROW}Add month groups")
    if concatenated:
        df['month'] = df.groupby(pd.Grouper(key='day', freq='M')).ngroup()
        df = df[df.month > 0]

    if concatenated:
        print(f"{STATUS_ARROW}Group by day")
        grouped = df.groupby(by="day")

        print(f"{STATUS_ARROW}Get num. files x day")
        numFiles = grouped.Filename.nunique()
        print(f"{STATUS_ARROW}Get num. requests x day")
        numReq = grouped.Filename.count()
        print(f"{STATUS_ARROW}Get num. jobs x day")
        numJobs = grouped.JobID.nunique()
        print(f"{STATUS_ARROW}Get num. tasks x day")
        numTasks = grouped.TaskID.nunique()
        print(f"{STATUS_ARROW}Get num. users x day")
        numUsers = grouped.UserID.nunique()
        print(f"{STATUS_ARROW}Get num. sites x day")
        numSites = grouped.SiteName.nunique()
        print(f"{STATUS_ARROW}Get num. request x file")
        numReqXFile = grouped.Filename.value_counts()
        numReqXFileAvg = numReqXFile.groupby("day").mean()
        numReqXFileAvgG1 = numReqXFile[numReqXFile > 1].groupby("day").mean()
    else:
        day_indexs = [cur_df.day.iloc[0] for cur_df in df]
        print(f"{STATUS_ARROW}Get num. files x day")
        numFiles = pd.Series(
            data=[cur_df.Filename.nunique() for cur_df in df],
            index=day_indexs,
        )
        print(f"{STATUS_ARROW}Get num. requests x day")
        numReq = pd.Series(
            data=[cur_df.Filename.count() for cur_df in df],
            index=day_indexs,
        )
        print(f"{STATUS_ARROW}Get num. jobs x day")
        numJobs = pd.Series(
            data=[cur_df.JobID.nunique() for cur_df in df],
            index=day_indexs,
        )
        print(f"{STATUS_ARROW}Get num. tasks x day")
        numTasks = pd.Series(
            data=[cur_df.TaskID.nunique() for cur_df in df],
            index=day_indexs,
        )
        print(f"{STATUS_ARROW}Get num. users x day")
        numUsers = pd.Series(
            data=[cur_df.UserID.nunique() for cur_df in df],
            index=day_indexs,
        )
        print(f"{STATUS_ARROW}Get num. sites x day")
        numSites = pd.Series(
            data=[cur_df.SiteName.nunique() for cur_df in df],
            index=day_indexs,
        )
        print(f"{STATUS_ARROW}Get num. request x file")
        numReqXFile = [cur_df.Filename.value_counts() for cur_df in df]
        numReqXFileAvg = pd.Series(
            data=[cur_values.mean() for cur_values in numReqXFile],
            index=day_indexs,
        )
        numReqXFileAvgG1 = pd.Series(
            data=[cur_values[cur_values > 1].mean()
                  for cur_values in numReqXFile],
            index=day_indexs,
        )

    numFiles.rename("Files")
    numReq.rename("Requests")
    numJobs.rename("Jobs")
    numTasks.rename("Tasks")
    numUsers.rename("Users")
    numSites.rename("Sites")
    numReqXFileAvg.rename("Avg. num. req. x file")
    numReqXFileAvgG1.rename("Avg. num. req. x file (> 1)")

    figGeneral, axesGeneral = plt.subplots(
        nrows=2, ncols=2, sharex=True, figsize=(24, 16)
    )

    print(f"{STATUS_ARROW}Plot num. files x day")
    numFiles.plot(ax=axesGeneral[0, 0], kind="line", figsize=(
        12, 8), legend=True, logy=True).legend(loc='upper right')
    print(f"{STATUS_ARROW}Plot num. requests x day")
    numReq.plot(ax=axesGeneral[0, 0], kind="line", figsize=(
        12, 8), legend=True, logy=True).legend(loc='upper right')
    print(f"{STATUS_ARROW}Plot num. jobs x day")
    numJobs.plot(ax=axesGeneral[0, 1], kind="line", figsize=(
        12, 8), legend=True, logy=True).legend(loc='upper right')
    print(f"{STATUS_ARROW}Plot num. tasks x day")
    numTasks.plot(ax=axesGeneral[0, 1], kind="line", figsize=(
        12, 8), legend=True, logy=True).legend(loc='upper right')
    print(f"{STATUS_ARROW}Plot num. users x day")
    numUsers.plot(ax=axesGeneral[1, 0], kind="line", figsize=(
        12, 8), legend=True, logy=True).legend(loc='upper right')
    print(f"{STATUS_ARROW}Plot num. sites x day")
    numSites.plot(ax=axesGeneral[1, 0], kind="line", figsize=(
        12, 8), legend=True, logy=True).legend(loc='upper right')
    print(f"{STATUS_ARROW}Plot avg. num. req x file")
    numReqXFileAvg.plot(ax=axesGeneral[1, 1], kind="line", figsize=(
        12, 8), legend=True, logy=True).legend(loc='upper right')
    print(f"{STATUS_ARROW}Plot avg. num. req x file > 1")
    numReqXFileAvgG1.plot(ax=axesGeneral[1, 1], kind="line", figsize=(
        12, 8), legend=True, logy=True).legend(loc='upper right')

    for ax in axesGeneral.flatten():
        ax.grid(True)

    figGeneral.tight_layout()

    figFileSizes, axesFileSizes = plt.subplots(
        nrows=1, ncols=1, figsize=(16, 8))
    if concatenated:
        print(f"{STATUS_ARROW}Plot file sizes")
        sizes = df[['size (GB)', 'DataType', 'day']]
    else:
        print(f"{STATUS_ARROW}Plot file sizes")
        sizes = [
            cur_df[['size (GB)', 'DataType', 'day']].copy()
            for cur_df in df
        ]
        for cur_size in tqdm(sizes, desc="Add months"):
            cur_size['month'] = cur_size.day.apply(
                lambda elm: elm.month
            ).astype(int)
        sizes = pd.concat(sizes)

    sizes = sizes[sizes['size (GB)'] < 10.]
    sns.violinplot(
        ax=axesFileSizes, x="month", y="size (GB)",
        data=sizes, palette="Set2", figsize=(16, 8),
        bw=.2, hue="DataType", split=True,
        scale="count", scale_hue=True, cut=0,
    )
    figFileSizes.tight_layout()

    figFileTypes, axesFileTypes = plt.subplots(
        nrows=1, ncols=2, figsize=(16, 8))
    if concatenated:
        print(f"{STATUS_ARROW}Plot data types")
        cur_ax = df.DataType.value_counts().plot(
            ax=axesFileTypes[0], kind="pie", figsize=(6, 6),
            labels=None, autopct='%.2f', fontsize=6
        )
        cur_ax.legend(loc='upper right',
                      labels=df.DataType.index, fontsize=4.2)
        print(f"{STATUS_ARROW}Plot file types")
        fileTypes = df.FileType.value_counts()
        cur_types = fileTypes[(fileTypes / fileTypes.sum()) > 0.02]
        cur_types.plot(
            ax=axesFileTypes[1], kind="pie", figsize=(6, 6),
            labels=None, autopct='%.2f', fontsize=6
        )
        cur_ax.legend(loc='upper right', labels=cur_types.index, fontsize=4.2)
    else:
        print(f"{STATUS_ARROW}Plot data types")
        data_types = [cur_df.DataType.value_counts() for cur_df in df]
        data_types = pd.concat(
            [cur_df.DataType.value_counts() for cur_df in df]
        ).reset_index().rename(columns={'index': 'types'})
        data_types = data_types.groupby("types").sum()
        data_types = pd.Series(
            data_types.values.flatten(), index=data_types.index
        )
        data_types.name = "Data types"
        cur_ax = data_types.plot(
            ax=axesFileTypes[0], kind="pie", figsize=(6, 6),
            labels=None, autopct='%.2f', fontsize=6
        )
        cur_ax.legend(loc='upper right', labels=data_types.index, fontsize=4.2)
        print(f"{STATUS_ARROW}Plot file types")
        file_types = pd.concat(
            [cur_df.FileType.value_counts() for cur_df in df]
        ).reset_index().rename(columns={'index': 'types'})
        file_types = file_types.groupby("types").sum()
        file_types = pd.Series(
            file_types.values.flatten(), index=file_types.index
        )
        file_types.name = "File types"
        cur_types = file_types[(file_types / file_types.sum()) > 0.05]
        cur_ax = cur_types.plot(
            ax=axesFileTypes[1], kind="pie", figsize=(6, 6),
            labels=None, autopct='%.2f', fontsize=6
        )
        cur_ax.legend(loc='upper right', labels=cur_types.index, fontsize=4.2)
    figFileTypes.tight_layout()

    if output_type == 'show':
        print(f"{STATUS_ARROW}Show results")
        plt.show()
    elif output_type == 'png':
        filename = f"{output_filename}_{region}_general.png"
        print(f"{STATUS_ARROW}Save result PNG in: {filename}")
        figGeneral.savefig(
            filename,
            dpi=300,
            bbox_inches="tight",
            pad_inches=0.24
        )
        filename = f"{output_filename}_{region}_fileSizes.png"
        print(f"{STATUS_ARROW}Save result PNG in: {filename}")
        figFileSizes.savefig(
            filename,
            dpi=300,
            bbox_inches="tight",
            pad_inches=0.24
        )
        filename = f"{output_filename}_{region}_fileTypes.png"
        print(f"{STATUS_ARROW}Save result PNG in: {filename}")
        figFileTypes.savefig(
            filename,
            dpi=300,
            bbox_inches="tight",
            pad_inches=0.24
        )


def plot_daily_stats(df: 'pd.DataFrame',
                     output_filename: str = 'dailystats',
                     output_type: str = 'show',
                     reset_stat_days: int = 0):

    stats = []
    stats_1req = []
    _files = {}
    days = []
    resets = []

    last_day = None
    num_req = 1

    for df_row in tqdm(df.itertuples(),
                       total=df.shape[0],
                       desc=f"{STATUS_ARROW}Parse stats"):
        if last_day != df_row.reqDay:
            days.append(num_req)
            last_day = df_row.reqDay
            if reset_stat_days > 0 and len(days) % reset_stat_days == 0:
                resets.append(num_req)
                stats.append(_files)
                _files = {}

        filename = df_row.Filename
        if filename not in _files:
            _files[filename] = FileStats(
                num_req-1,
                int(df_row.Size / 1024**2),
                num_req,
            )

        cur_file = _files[filename]
        cur_file.x.append(num_req)
        cur_file.n_req.append(cur_file.n_req[-1] + 1)
        cur_file.delta_times.append(num_req - cur_file.last_request[-1])
        cur_file.last_request.append(num_req)

        if df_row.SiteName not in cur_file.sites:
            cur_file.sites |= set((df_row.SiteName, ))
        if df_row.UserID not in cur_file.users:
            cur_file.users |= set((df_row.UserID, ))

        cur_file.n_users.append(len(cur_file.users))
        cur_file.n_sites.append(len(cur_file.sites))
        cur_file.sizes.append(int(df_row.Size / 1024**2))

        num_req += 1
    else:
        days.append(num_req)
        stats.append(_files)
        _files = {}

    ##
    # Uncomment to get frequencies
    # frequencies = {}

    for period, files in enumerate(stats, 1):
        all_filenames = list(files.keys())
        _files = {}
        for filename in tqdm(all_filenames, desc=f"{STATUS_ARROW}Split 1 req files from period {period}"):
            ##
            # Uncomment to get frequencies
            # cur_file = files[filename]
            # num_req = cur_file.n_req[-1]
            # if num_req not in frequencies:
            #     frequencies[num_req] = 0
            # frequencies[num_req] += 1

            if len(files[filename].x) <= 2:
                _files[filename] = files[filename]
                del files[filename]
        else:
            stats_1req.append(_files)

        ##
        # Uncomment to get frequencies
        # frequencie_keys = list(frequencies.keys())
        # df = pd.DataFrame(data={'num.req': frequencie_keys, 'num.files': [
        #                   frequencies[key] for key in frequencie_keys]})
        # df.to_csv(f"week-{period:02d}.csv", index=False)

    fig_n_req = figure(plot_width=1600, plot_height=320,
                       title="Num. Requests", x_axis_label="n-th request")
    fig_n_users = figure(plot_width=1600, plot_height=320,
                         x_range=fig_n_req.x_range, title="Num. Users",
                         x_axis_label="n-th request")
    fig_n_sites = figure(plot_width=1600, plot_height=320,
                         x_range=fig_n_req.x_range, title="Num. Sites",
                         x_axis_label="n-th request")
    fig_delta_times = figure(plot_width=1600, plot_height=320,
                             x_range=fig_n_req.x_range, title="Delta times",
                             x_axis_label="n-th request")
    fig_sizes = figure(plot_width=1600, plot_height=320,
                       x_range=fig_n_req.x_range, title="File sizes",
                       x_axis_label="n-th request",
                       y_axis_label="size (MB)")
    fig_1req_sizes = figure(plot_width=1600, plot_height=320,
                            x_range=fig_n_req.x_range, title="1 req. file sizes",
                            x_axis_label="n-th request",
                            y_axis_label="size (MB)")
    fig_corr_numreqs_sizes = figure(plot_width=320, plot_height=320,
                                    title="correlation num. reqs. and file sizes",
                                    x_axis_label="num. req.",
                                    y_axis_label="size (MB)")
    fig_corr_numreqs_numusers = figure(plot_width=320, plot_height=320,
                                       title="correlation num. reqs. and num. users",
                                       x_axis_label="num. req.",
                                       y_axis_label="num. users")
    fig_corr_numreqs_numsites = figure(plot_width=320, plot_height=320,
                                       title="correlation num. reqs. and num. sites",
                                       x_axis_label="num. req.",
                                       y_axis_label="num. sites")
    fig_corr_numreqs_meandelta = figure(plot_width=320, plot_height=320,
                                        title="correlation num. reqs. and mean delta times",
                                        x_axis_label="num. req.",
                                        y_axis_label="mean delta time")
    fig_corr_meandelta_sizes = figure(plot_width=320, plot_height=320,
                                      title="correlation mean delta times. and sizes",
                                      x_axis_label="mean delta time",
                                      y_axis_label="size (MB)")

    buff_xs = []
    buff_n_req = []
    buff_n_users = []
    buff_n_sites = []
    buff_delta_times = []
    buff_sizes = []
    buff_1req_xs = []
    buff_1req_sizes = []
    buff_corr_numreqs_sizes = []
    buff_corr_numreqs_numusers = []
    buff_corr_numreqs_numsites = []
    buff_corr_numreqs_meandelta = []
    buff_corr_meandelta_sizes = []

    for period, files in enumerate(stats, 1):
        for _, stats in tqdm(files.items(), desc=f"{STATUS_ARROW}Collect lines of period {period}"):
            buff_xs += [stats.x[1:]]
            buff_n_req.append(stats.n_req[1:])
            buff_n_users.append(stats.n_users[1:])
            buff_n_sites.append(stats.n_sites[1:])
            buff_delta_times.append(stats.delta_times[1:])
            buff_sizes.append(stats.sizes[1:])
            buff_corr_numreqs_sizes.append(
                (stats.n_req[-1], stats.sizes[-1]))
            buff_corr_numreqs_numusers.append(
                (stats.n_req[-1], stats.n_users[-1]))
            buff_corr_numreqs_numsites.append(
                (stats.n_req[-1], stats.n_sites[-1]))
            buff_corr_numreqs_meandelta.append(
                (
                    stats.n_req[-1],
                    int(sum(stats.delta_times)/len(stats.delta_times)),
                )
            )
            buff_corr_meandelta_sizes.append(
                (
                    int(sum(stats.delta_times)/len(stats.delta_times)),
                    stats.sizes[-1]
                )
            )

    for period, files in enumerate(stats_1req, 1):
        for _, stats in tqdm(files.items(), desc=f"{STATUS_ARROW}Collect 1 req. file lines of period {period}"):
            buff_1req_xs += [stats.x[1:]]
            buff_1req_sizes.append(stats.sizes[1:])
            buff_corr_numreqs_sizes.append(
                (stats.n_req[-1], stats.sizes[-1]))
            buff_corr_numreqs_numusers.append(
                (stats.n_req[-1], stats.n_users[-1]))
            buff_corr_numreqs_numsites.append(
                (stats.n_req[-1], stats.n_sites[-1]))

    print(f"{STATUS_ARROW}Plot num requests")
    fig_n_req.multi_line(
        xs=buff_xs,
        ys=buff_n_req,
        line_color=['red' for _ in range(len(buff_xs))],
        line_width=2,
    )
    print(f"{STATUS_ARROW}Plot num. users")
    fig_n_users.multi_line(
        xs=buff_xs,
        ys=buff_n_users,
        line_color=['blue' for _ in range(len(buff_xs))],
        line_width=2,
    )
    print(f"{STATUS_ARROW}Plot num. sites")
    fig_n_sites.multi_line(
        xs=buff_xs,
        ys=buff_n_sites,
        line_color=['green' for _ in range(len(buff_xs))],
        line_width=2,
    )
    print(f"{STATUS_ARROW}Plot delta times")
    fig_delta_times.scatter(
        [val for points in buff_xs for val in points],
        [val for delta_times in buff_delta_times for val in delta_times],
        size=5
    )
    print(f"{STATUS_ARROW}Plot sizes")
    fig_sizes.scatter(
        [val for points in buff_xs for val in points],
        [val for sizes in buff_sizes for val in sizes],
        size=5
    )
    print(f"{STATUS_ARROW}Plot sizes of 1 req. files")
    fig_1req_sizes.scatter(
        [val for points in buff_1req_xs for val in points],
        [val for sizes in buff_1req_sizes for val in sizes],
        size=5
    )
    print(f"{STATUS_ARROW}Plot correlation of num. reqs. and sizes")
    fig_corr_numreqs_sizes.scatter(
        *zip(*buff_corr_numreqs_sizes),
        size=5
    )
    print(f"{STATUS_ARROW}Plot correlation of num. reqs. and num. users")
    fig_corr_numreqs_numusers.scatter(
        *zip(*buff_corr_numreqs_numusers),
        size=5
    )
    print(f"{STATUS_ARROW}Plot correlation of num. reqs. and num. sites")
    fig_corr_numreqs_numsites.scatter(
        *zip(*buff_corr_numreqs_numsites),
        size=5
    )
    print(f"{STATUS_ARROW}Plot correlation of num. reqs. and mean delta times")
    fig_corr_numreqs_meandelta.scatter(
        *zip(*buff_corr_numreqs_meandelta),
        size=5
    )
    print(f"{STATUS_ARROW}Plot correlation of mean delta times and sizes")
    fig_corr_meandelta_sizes.scatter(
        *zip(*buff_corr_meandelta_sizes),
        size=5
    )

    fig_delta_times.yaxis.formatter = BasicTickFormatter(use_scientific=False)

    for fig in [fig_n_req, fig_n_users, fig_n_sites, fig_sizes, fig_1req_sizes]:
        fig.renderers.extend([
            Span(
                location=day_req, dimension='height',
                line_color='lightgray', line_width=2
            )
            for day_req in days
        ])
        fig.renderers.extend([
            Span(
                location=day_reset, dimension='height',
                line_color='purple', line_width=2
            )
            for day_reset in resets
        ])
        fig.yaxis.formatter = BasicTickFormatter(use_scientific=False)
        # fig.y_range = Range1d(0, 42)

    plot = column(
        fig_n_req, fig_n_users, fig_n_sites, fig_delta_times,
        fig_sizes, fig_1req_sizes,
        row(
            fig_corr_numreqs_sizes,
            fig_corr_numreqs_numusers,
            fig_corr_numreqs_numsites,
            fig_corr_numreqs_meandelta,
            fig_corr_meandelta_sizes
        ),
    )

    if output_type == 'show':
        print(f"{STATUS_ARROW}Show results")
        show(plot)
    elif output_type == 'html':
        output_file(f"{output_filename}.html", mode="inline")
        print(f"{STATUS_ARROW}Save result HTML in: {output_filename}.html")
        save(plot)
    elif output_type == 'png':
        print(f"{STATUS_ARROW}Save result PNG in: {output_filename}.png")
        export_png(plot, filename=f"{output_filename}.png")


def plot_week_stats(df: 'pd.DataFrame',
                    output_filename: str = 'weekstats',
                    output_type: str = 'show',
                    reset_stat_days: int = 0):

    days = df["reqDay"].unique().tolist()
    weeks = []
    stats = []

    for idx in range(0, len(days), 7):
        cur_week = []
        num_days = min([7, len(days[idx:])])
        if num_days != 7:
            break
        for day_idx in range(num_days):
            cur_week.append(days[idx+day_idx])
        weeks.append(cur_week)

    for week in weeks:
        cur_week = df[df['reqDay'].isin(week)]
        reqXfile = cur_week.Filename.value_counts()
        stats.append({
            'num_users': len(cur_week.UserID.unique()),
            'num_tasks': len(cur_week.TaskID.unique()),
            'num_jobs': len(cur_week.JobID.unique()),
            'num_sites': len(cur_week.SiteName.unique()),
            'num_files': len(cur_week.Filename.unique()),
            'num_requests': len(cur_week.index),
            'num_reqXfile': reqXfile.mean(),
            'num_reqXfile_reqGr1': reqXfile[reqXfile.iloc[:] > 1].mean(),
        })

    all_weeks = [f"week {idx+1}" for idx in range(len(stats))]

    fig_general_stats = make_week_bars(
        "General week stats",
        all_weeks,
        ['num_users', 'num_sites', 'num_jobs', 'num_tasks'],
        ['Num. users', 'Num. sites', 'Num. jobs', 'Num. tasks'],
        stats,
    )

    fig_request_stats = make_week_bars(
        "Request stats",
        all_weeks,
        ['num_files', 'num_requests'],
        ['Num. files', 'Num. requests'],
        stats,
    )

    fig_avg_request_stats = make_week_bars(
        "Average request stats",
        all_weeks,
        ['num_reqXfile', 'num_reqXfile_reqGr1'],
        ['Avg. num. req. per file',
            'Avg. num. req. per file (files > 1 req.)'],
        stats,
    )

    plot = row(
        fig_general_stats,
        fig_request_stats,
        fig_avg_request_stats,
    )

    if output_type == 'show':
        print(f"{STATUS_ARROW}Show results")
        show(plot)
    elif output_type == 'html':
        output_file(f"{output_filename}.html", mode="inline")
        print(f"{STATUS_ARROW}Save result HTML in: {output_filename}.html")
        save(plot)
    elif output_type == 'png':
        print(f"{STATUS_ARROW}Save result PNG in: {output_filename}.png")
        export_png(plot, filename=f"{output_filename}.png")


def make_week_bars(title: str, weeks: list, categories: list, legends: list, stats: list):
    cur_data = {
        'weeks': weeks
    }

    for category in categories:
        cur_data[category] = []
        for stat in stats:
            cur_data[category].append(stat[category])

    source = ColumnDataSource(data=cur_data)

    bar_size = .8 / len(categories)

    cur_fig = figure(
        x_range=weeks,
        y_axis_type='log',
        plot_height=480,
        title=title,
    )

    for idx, category in enumerate(categories):
        cur_fig.vbar(
            x=dodge('weeks', idx*bar_size-0.42, range=cur_fig.x_range),
            top=category, bottom=1,
            width=bar_size, source=source,
            legend_label=legends[idx],
            color=Accent[8][idx]
        )

    cur_fig.x_range.range_padding = 0.1
    cur_fig.xgrid.grid_line_color = None
    cur_fig.legend.location = "bottom_right"
    cur_fig.legend.orientation = "horizontal"
    cur_fig.yaxis.formatter = BasicTickFormatter(use_scientific=False)

    return cur_fig
