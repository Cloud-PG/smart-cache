import pandas as pd
from bokeh.io import export_png
from bokeh.layouts import column, row
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
            _files[filename] = FileStats(num_req-1, int(df_row.Size / 1024**2))

        cur_file = _files[filename]
        cur_file.x.append(num_req)
        cur_file.n_req.append(cur_file.n_req[-1] + 1)

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
        for filename in tqdm(all_filenames, desc=f"{_STATUS}Split 1 req files from period {period}"):
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
    fig_corr_numreqs_sizes = figure(plot_width=240, plot_height=240,
                                    title="correlation num. reqs. and file sizes",
                                    x_axis_label="num. req.",
                                    y_axis_label="size (MB)")
    fig_corr_numreqs_numusers = figure(plot_width=240, plot_height=240,
                                       title="correlation num. reqs. and num. users",
                                       x_axis_label="num. req.",
                                       y_axis_label="num. users")
    fig_corr_numreqs_numsites = figure(plot_width=240, plot_height=240,
                                       title="correlation num. reqs. and num. sites",
                                       x_axis_label="num. req.",
                                       y_axis_label="num. sites")

    buf_xs = []
    buf_n_req = []
    buf_n_users = []
    buf_n_sites = []
    buf_sizes = []
    buf_1req_xs = []
    buf_1req_sizes = []
    buf_corr_numreqs_sizes = []
    buf_corr_numreqs_numusers = []
    buf_corr_numreqs_numsites = []

    for period, files in enumerate(stats, 1):
        for _, stats in tqdm(files.items(), desc=f"{_STATUS}Collect lines of period {period}"):
            buf_xs += [stats.x]
            buf_n_req.append(stats.n_req)
            buf_n_users.append(stats.n_users)
            buf_n_sites.append(stats.n_sites)
            buf_sizes.append(stats.sizes)
            buf_corr_numreqs_sizes.append(
                (stats.n_req[-1], stats.sizes[-1]))
            buf_corr_numreqs_numusers.append(
                (stats.n_req[-1], stats.n_users[-1]))
            buf_corr_numreqs_numsites.append(
                (stats.n_req[-1], stats.n_sites[-1]))

    for period, files in enumerate(stats_1req, 1):
        for _, stats in tqdm(files.items(), desc=f"{_STATUS}Collect lines of period {period}"):
            buf_1req_xs += [stats.x]
            buf_1req_sizes.append(stats.sizes)
            buf_corr_numreqs_sizes.append(
                (stats.n_req[-1], stats.sizes[-1]))
            buf_corr_numreqs_numusers.append(
                (stats.n_req[-1], stats.n_users[-1]))
            buf_corr_numreqs_numsites.append(
                (stats.n_req[-1], stats.n_sites[-1]))

    print(f"{_STATUS}Plot num requests")
    fig_n_req.multi_line(
        xs=buf_xs,
        ys=buf_n_req,
        line_color=['red' for _ in range(len(buf_xs))],
        line_width=2,
    )
    print(f"{_STATUS}Plot num. users")
    fig_n_users.multi_line(
        xs=buf_xs,
        ys=buf_n_users,
        line_color=['blue' for _ in range(len(buf_xs))],
        line_width=2,
    )
    print(f"{_STATUS}Plot num. sites")
    fig_n_sites.multi_line(
        xs=buf_xs,
        ys=buf_n_sites,
        line_color=['green' for _ in range(len(buf_xs))],
        line_width=2,
    )
    print(f"{_STATUS}Plot sizes")
    fig_sizes.scatter(
        [val for points in buf_xs for val in points],
        [val for sizes in buf_sizes for val in sizes],
        size=5
    )
    print(f"{_STATUS}Plot sizes of 1 req. files")
    fig_1req_sizes.scatter(
        [val for points in buf_1req_xs for val in points],
        [val for sizes in buf_1req_sizes for val in sizes],
        size=5
    )
    print(f"{_STATUS}Plot correlation of num. reqs. and sizes")
    fig_corr_numreqs_sizes.scatter(
        *zip(*buf_corr_numreqs_sizes),
        size=5
    )
    print(f"{_STATUS}Plot correlation of num. reqs. and num. users")
    fig_corr_numreqs_numusers.scatter(
        *zip(*buf_corr_numreqs_numusers),
        size=5
    )
    print(f"{_STATUS}Plot correlation of num. reqs. and num. sites")
    fig_corr_numreqs_numsites.scatter(
        *zip(*buf_corr_numreqs_numsites),
        size=5
    )

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
        fig_n_req, fig_n_users, fig_n_sites,
        fig_sizes, fig_1req_sizes,
        row(
            fig_corr_numreqs_sizes,
            fig_corr_numreqs_numusers,
            fig_corr_numreqs_numsites
        ),
    )

    if output_type == 'show':
        print(f"{_STATUS}Show results")
        show(plot)
    elif output_type == 'html':
        output_file(f"{output_filename}.html", mode="inline")
        print(f"{_STATUS}Save result HTML in: {output_filename}")
        save(plot)
    elif output_type == 'png':
        print(f"{_STATUS}Save result PNG in: {output_filename}.png")
        export_png(plot, filename=f"{output_filename}.png")
