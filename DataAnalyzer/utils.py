import pandas as pd
from bokeh.layouts import column
from bokeh.models import BasicTickFormatter, Range1d, Span
from bokeh.plotting import figure, output_file, save, show
from colorama import Fore, Style
from tqdm import tqdm

_STATUS = f"{Style.BRIGHT + Fore.MAGENTA}==> {Style.RESET_ALL}"


def sort_by_date(df: 'pd.DataFrame', column_name: str = "reqDay") -> 'pd.DataFrame':
    df.sort_values(by="reqDay", inplace=True)
    return df


class FileStats(object):

    def __init__(self, start_x: int = 0, size: int = 0):
        self.x = [start_x]
        self.n_req = [0]
        self.n_users = [0]
        self.n_sites = [0]
        self.sizes = [size]
        self.users = set()
        self.sites = set()


def plot_daily_stats(df: 'pd.DataFrame',
                     output_filename: str = 'dailystats.html',
                     reset_stat_days: int = 0):
    output_file(output_filename, mode="inline")

    stats = []
    stats_1req = []
    _files = {}
    days = []
    resets = []

    last_day = None
    num_req = 1

    for df_row in tqdm(df.itertuples(),
                       total=df.shape[0],
                       desc=f"{_STATUS}Parse stats"):
        if last_day != df_row.reqDay:
            days.append(num_req)
            last_day = df_row.reqDay
            if reset_stat_days > 0 and len(days) % reset_stat_days == 0:
                resets.append(num_req)
                stats.append(_files)
                _files = {}

        filename = df_row.Filename
        if filename not in _files:
            _files[filename] = FileStats(num_req-1, df_row.Size / 1024**2)

        cur_file = _files[filename]
        cur_file.x.append(num_req)
        cur_file.n_req.append(cur_file.n_req[-1] + 1)

        if df_row.SiteName not in cur_file.sites:
            cur_file.sites |= set((df_row.SiteName, ))
        if df_row.UserID not in cur_file.users:
            cur_file.users |= set((df_row.UserID, ))

        cur_file.n_users.append(len(cur_file.users))
        cur_file.n_sites.append(len(cur_file.sites))
        cur_file.sizes.append(df_row.Size / 1024**2)

        num_req += 1
    else:
        days.append(num_req)
        stats.append(_files)
        _files = {}

    fig_n_req = figure(plot_width=1280, plot_height=240,
                       title="Num. Requests", x_axis_label="n-th request")
    fig_n_users = figure(plot_width=1280, plot_height=240,
                         x_range=fig_n_req.x_range, title="Num. Users",
                         x_axis_label="n-th request")
    fig_n_sites = figure(plot_width=1280, plot_height=240,
                         x_range=fig_n_req.x_range, title="Num. Sites",
                         x_axis_label="n-th request")
    fig_sizes = figure(plot_width=1280, plot_height=240,
                       x_range=fig_n_req.x_range, title="File sizes",
                       x_axis_label="n-th request",
                       y_axis_label="size (MB)")
    fig_1req_sizes = figure(plot_width=1280, plot_height=240,
                            x_range=fig_n_req.x_range, title="1 req. file sizes",
                            x_axis_label="n-th request",
                            y_axis_label="size (MB)")

    for period, files in enumerate(stats, 1):
        all_filenames = list(files.keys())
        _files = {}
        for filename in tqdm(all_filenames, desc=f"{_STATUS}Split 1 req files from period {period}"):
            if len(files[filename].x) <= 2:
                _files[filename] = files[filename]
                del files[filename]
        else:
            stats_1req.append(_files)

    buffer_xs = []
    buffer_n_req = []
    buffer_n_users = []
    buffer_n_sites = []
    buffer_sizes = []
    buffer_1req_xs = []
    buffer_1req_sizes = []

    for period, files in enumerate(stats, 1):
        for _, stats in tqdm(files.items(), desc=f"{_STATUS}Collect lines of period {period}"):
            buffer_xs += [stats.x]
            buffer_n_req.append(stats.n_req)
            buffer_n_users.append(stats.n_users)
            buffer_n_sites.append(stats.n_sites)
            buffer_sizes.append(stats.sizes)

    for period, files in enumerate(stats_1req, 1):
        for _, stats in tqdm(files.items(), desc=f"{_STATUS}Collect lines of period {period}"):
            buffer_1req_xs += [stats.x]
            buffer_1req_sizes.append(stats.sizes)

    print(f"{_STATUS}Plot num requests")
    fig_n_req.multi_line(
        xs=buffer_xs,
        ys=buffer_n_req,
        line_color=['red' for _ in range(len(buffer_xs))],
        line_width=2,
    )
    print(f"{_STATUS}Plot num. users")
    fig_n_users.multi_line(
        xs=buffer_xs,
        ys=buffer_n_users,
        line_color=['blue' for _ in range(len(buffer_xs))],
        line_width=2,
    )
    print(f"{_STATUS}Plot num. sites")
    fig_n_sites.multi_line(
        xs=buffer_xs,
        ys=buffer_n_sites,
        line_color=['green' for _ in range(len(buffer_xs))],
        line_width=2,
    )
    print(f"{_STATUS}Plot sizes")
    fig_sizes.scatter(
        [val for points in buffer_xs for val in points],
        [val for sizes in buffer_sizes for val in sizes],
        size=5
    )
    print(f"{_STATUS}Plot sizes of 1 req. files")
    fig_1req_sizes.scatter(
        [val for points in buffer_1req_xs for val in points],
        [val for sizes in buffer_1req_sizes for val in sizes],
        size=5
    )

    for fig in [fig_n_req, fig_n_users, fig_n_sites]:
        fig.renderers.extend([
            Span(
                location=day_req, dimension='height',
                line_color='black', line_width=2
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

    # print(f"{_STATUS}Show results")
    # show(column(fig_n_req, fig_n_users, fig_n_sites, fig_sizes, fig_1req_sizes))
    print(f"{_STATUS}Save HTML results in: {output_filename}")
    save(column(fig_n_req, fig_n_users, fig_n_sites, fig_sizes, fig_1req_sizes))
