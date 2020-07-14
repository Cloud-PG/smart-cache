from itertools import cycle

import numpy as np
import pandas as pd
from bokeh.models import (BasicTickFormatter, FuncTickFormatter, Legend,
                          Range1d, SaveTool, Span)
from bokeh.palettes import Category20
from bokeh.plotting import figure

from .utils import get_cache_size

_LINE_WIDTH = 2.8

_DAY_SECONDS = 60. * 60. * 24.
_Band1Gbit = (1000. / 8.) * _DAY_SECONDS


def update_colors(new_names: str, color_table: dict):
    names = [
        name for name in list(color_table.keys())
        if name.find("_single") == -1
    ] + [new_names]
    colors = cycle(Category20[20])
    for name in sorted(names):
        color_table[name] = next(colors)
        color_table[f'{name}_single'] = next(colors)


def add_window_lines(cur_fig, dates: list, window_size: int):
    cur_fig.renderers.extend([
        Span(
            location=idx-0.5, dimension='height',
            line_color='black', line_width=0.42
        )
        for idx in range(0, len(dates), window_size)
    ])
    cur_fig.renderers.extend([
        Span(
            location=idx-0.5, dimension='height',
            line_color='gray', line_width=3.42
        )
        for idx in range(0, len(dates), window_size*4)
    ])


def filter_results(results: dict, key: str, filters: list):
    for cache_name, values in results[key].items():
        if filters:
            if all([
                cache_name.find(filter_) != -1
                for filter_ in filters
            ]):
                yield cache_name, values
        else:
            yield cache_name, values


def get_lru(results: dict, key: str):
    for cache_name, values in results[key].items():
        try:
            if cache_name.index("lru_") == 0:
                return values
        except ValueError:
            pass
    raise Exception(f"Cannot find lru cache values for '{key}''")


def collapse2numpoints(num_points, x=None, y=None):
    if x is not None:
        return np.array([
            group[0]
            for group in np.array_split(x, num_points)
        ])
    elif y is not None:
        return np.array([
            group.mean()
            for group in np.array_split(y, num_points)
        ])


def plot_column(tools: list,
                results: dict,
                dates: list,
                filters: list,
                color_table: dict,
                window_size: int,
                x_range=None,
                normalize: str = None,
                column: str = "hit rate",
                title: str = "Hit Rate",
                y_axis_label: str = "Hit rate %",
                run_type: str = "run_full_normal",
                datetimes: list = [],
                plot_width: int = 640,
                plot_height: int = 480,
                upper_bound: str = None,
                lower_bound: str = None,
                outer_legend: bool = False,
                num_points: int = -1,
                ) -> 'Figure':
    cur_fig = figure(
        tools=tools,
        title=title,
        x_axis_label="Day",
        y_axis_label=y_axis_label,
        x_range=x_range if x_range else dates if num_points == -
        1 else collapse2numpoints(num_points, x=dates),
        y_range=None,
        plot_width=plot_width,
        plot_height=plot_height,
    )

    if column == "hit rate":
        cur_fig.y_range = Range1d(0, 100)

    legend_items = []
    y_max_range = 100.

    upper_bound_points = None
    lower_bound_points = None

    for cache_name, values in filter_results(
        results, run_type, filters
    ):
        if column not in values.columns:
            continue
        if run_type == "run_full_normal":
            if normalize:
                points = (values[column] / values[normalize]) * 100.
            else:
                points = values[column]
            cur_line = cur_fig.line(
                dates if num_points == -
                1 else collapse2numpoints(num_points, x=dates),
                points if num_points == -
                1 else collapse2numpoints(num_points, y=points),
                color=color_table[cache_name],
                line_width=_LINE_WIDTH,
            )
            legend_items.append(
                (get_cache_legend_name(cache_name), [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = cur_fig.line(
                dates if num_points == -
                1 else collapse2numpoints(num_points, x=dates),
                [mean_point for _ in range(
                    len(dates) if num_points == -1 else num_points)],
                line_color=color_table[cache_name],
                line_dash="dashdot",
                line_width=3.,
            )
            legend_items.append(
                (f"Mean {get_cache_legend_name(cache_name)} -> {mean_point:0.2f}{'%' if normalize else ''}",
                 [cur_line])
            )
            if normalize:
                y_max_range = max([y_max_range] + points.to_list())
                cur_fig.y_range = Range1d(0, y_max_range)
            if upper_bound is not None and upper_bound_points is None and not np.array_equal(upper_bound_points, values[upper_bound].to_numpy()):
                points = upper_bound_points = values[upper_bound].to_numpy()
                if any(np.isnan(points)):
                    print("WARNING [Upper bound]: some points are NaN and will be put to 0...")
                    points[np.isnan(points)] = 0.0
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    points if num_points == -
                    1 else collapse2numpoints(num_points, y=points),
                    line_color="red",
                    line_dash="dotted",
                    line_width=2.4,
                )
                legend_items.append(("Upper Bound", [cur_line]))
                mean_point = sum(points) / len(points)
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    [mean_point for _ in range(
                        len(dates) if num_points == -1 else num_points)],
                    line_color="red",
                    line_dash="dotdash",
                    line_width=3.,
                )
                legend_items.append(
                    (f"Mean Upper Bound -> {mean_point:0.2f}{'%' if normalize else ''}",
                     [cur_line])
                )
            if lower_bound is not None and lower_bound_points is None and not np.array_equal(lower_bound_points, values[lower_bound].to_numpy()):
                points = lower_bound_points = values[lower_bound].to_numpy()
                if any(np.isnan(points)):
                    print("WARNING [Lower bound]: some points are NaN and will be put to 0...")
                    points[np.isnan(points)] = 0.0
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    points if num_points == -
                    1 else collapse2numpoints(num_points, y=points),
                    line_color="red",
                    line_dash="dotted",
                    line_width=2.4,
                )
                legend_items.append(("Lower Bound", [cur_line]))
                mean_point = sum(points) / len(points)
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    [mean_point for _ in range(
                        len(dates) if num_points == -1 else num_points)],
                    line_color="red",
                    line_dash="dotdash",
                    line_width=3.,
                )
                legend_items.append(
                    (f"Mean Lower Bound -> {mean_point:0.2f}{'%' if normalize else ''}",
                     [cur_line])
                )
        elif run_type == "run_single_window":
            points = results['run_full_normal'][cache_name][column]
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=_LINE_WIDTH,
            )
            legend_items.append((cache_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = cur_fig.line(
                dates,
                [mean_point for _ in range(len(dates))],
                line_color=color_table[cache_name],
                line_dash="dashdot",
                line_width=3.,
            )
            legend_items.append(
                (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
            )
            single_window_name = f'{cache_name} - single window'
            next_window_name = f'{cache_name} - next window'
            single_windows = pd.concat(
                [
                    window
                    for name, window in sorted(
                        values.items(),
                        key=lambda elm: elm[0],
                    )
                ]
            )
            points = single_windows.sort_values(by=['date'])[column]
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[f'{cache_name}_single'],
                line_width=_LINE_WIDTH,
            )
            legend_items.append((single_window_name, [cur_line]))
            next_windows = pd.concat(
                [
                    window
                    for name, window in sorted(
                        results['run_next_window'][cache_name].items(),
                        key=lambda elm: elm[0],
                    )
                ]
            ).sort_values(by=['date'])
            cur_dates = [
                elm.split(" ")[0]
                for elm
                in next_windows['date'].astype(str)
            ]
            points = next_windows[column]
            cur_line = cur_fig.line(
                cur_dates,
                points,
                line_color="red",
                line_alpha=0.9,
                line_width=_LINE_WIDTH,
                line_dash="dashed",
            )
            legend_items.append((next_window_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = cur_fig.line(
                cur_dates,
                [mean_point for _ in range(len(cur_dates))],
                line_color="red",
                line_dash="dashdot",
                line_width=3.,
            )
            legend_items.append(
                (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
            )
        elif run_type == "run_next_period":
            points = results['run_full_normal'][cache_name][column]
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=_LINE_WIDTH,
            )
            legend_items.append((cache_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = cur_fig.line(
                dates,
                [mean_point for _ in range(len(dates))],
                line_color=color_table[cache_name],
                line_dash="dashdot",
                line_width=3.,
            )
            legend_items.append(
                (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
            )
            single_window_name = f'{cache_name} - single window'
            single_windows = pd.concat(
                [
                    window
                    for name, window in sorted(
                        results['run_single_window'][cache_name].items(),
                        key=lambda elm: elm[0],
                    )
                ]
            )
            points = single_windows.sort_values(by=['date'])[column]
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[f'{cache_name}_single'],
                line_width=_LINE_WIDTH,
            )
            legend_items.append((single_window_name, [cur_line]))
            line_styles = cycle([
                'solid',
                'dashed',
                'dotted',
                'dotdash',
                'dashdot',
            ])
            for period, period_values in values.items():
                cur_period = period_values[
                    ['date', column]
                ][period_values.date.isin(datetimes)]
                cur_period_name = f"{cache_name} - from {period.split('-')[0]}"
                points = cur_period.sort_values(by=['date'])[column]
                cur_dates = [
                    elm.split(" ")[0]
                    for elm
                    in cur_period['date'].astype(str)
                ]
                cur_dates = [
                    cur_date for cur_date in cur_dates
                    if cur_date in dates
                ]
                cur_period = cur_period[~cur_period.date.isin(datetimes)]
                if len(cur_dates) > 0:
                    cur_line_style = next(line_styles)
                    cur_line = cur_fig.line(
                        cur_dates,
                        points,
                        line_color="red",
                        line_alpha=0.9,
                        line_width=_LINE_WIDTH,
                        line_dash=cur_line_style,
                    )
                    legend_items.append((cur_period_name, [cur_line]))
                    mean_point = sum(points) / len(points)
                    cur_line = cur_fig.line(
                        cur_dates,
                        [mean_point for _ in range(len(cur_dates))],
                        line_color="red",
                        line_dash="dashdot",
                        line_width=3.,
                    )
                    legend_items.append(
                        (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
                    )

    legend = Legend(items=legend_items, location=(0, 0))
    legend.location = "top_right"
    legend.click_policy = "hide"
    cur_fig.add_layout(legend, 'right' if outer_legend else 'center')
    cur_fig.yaxis.formatter = BasicTickFormatter(use_scientific=False)
    if num_points == -1:
        cur_fig.xaxis.formatter = FuncTickFormatter(code="""
        var day = parseInt(tick.split("-")[2], 10)
        if ( day%7 == 0 ) { return tick }
        else { return "" }
    """)
    cur_fig.xaxis.major_label_orientation = np.pi / 4.
    cur_fig.add_tools(SaveTool())
    # cur_fig.legend.label_text_font_size = '16pt'
    # cur_fig.xaxis.axis_label_text_font_size = "16pt"
    # cur_fig.xaxis.major_label_text_font_size = "16pt"
    # cur_fig.yaxis.axis_label_text_font_size = "16pt"
    # cur_fig.yaxis.major_label_text_font_size = "16pt"
    add_window_lines(cur_fig, dates, window_size)

    return cur_fig


def get_cache_legend_name(string: str):
    if string.find("weightFunLRU") != -1:
        return "_".join(string.split("_")[3:])
    return string


def plot_measure(tools: list,
                 results: dict,
                 dates: list,
                 filters: list,
                 color_table: dict,
                 window_size: int,
                 x_range=None,
                 y_axis_label: str = "MB",
                 y_axis_type: str = "auto",
                 read_on_hit: bool = True,
                 title: str = "Read on Write data",
                 run_type: str = "run_full_normal",
                 datetimes: list = [],
                 plot_width: int = 640,
                 plot_height: int = 480,
                 target: str = None,
                 bandwidth: int = 10,
                 outer_legend: bool = False,
                 num_points: int = -1,
                 ) -> 'Figure':
    cur_fig = figure(
        tools=tools,
        title=title,
        x_axis_label="Day",
        y_axis_label=y_axis_label,
        y_axis_type=y_axis_type,
        x_range=x_range if x_range else dates if num_points == -
        1 else collapse2numpoints(num_points, x=dates),
        plot_width=plot_width,
        plot_height=plot_height,
    )

    if target is not None and target.lower().find("vs") == -1 and target not in ['cpu_eff', 'network_in_saturation', 'network_out_saturation', 'cost', 'miss']:
        hline_1 = Span(
            location=1.0, dimension='width', line_dash="dashed",
            line_color="black", line_width=_LINE_WIDTH,
        )
        cur_fig.renderers.extend([hline_1])
    elif target is not None and (target.find("network_") != -1 or target.find("cost") != -1 or target.find("miss") != -1):
        hline_1 = Span(
            location=100.0, dimension='width', line_dash="dashed",
            line_color="black", line_width=_LINE_WIDTH,
        )
        cur_fig.renderers.extend([hline_1])

    read_data_type = 'read on hit data' if read_on_hit else 'read data'
    legend_items = []

    cur_band = _Band1Gbit * bandwidth

    for cache_name, values in filter_results(
        results, run_type, filters
    ):
        if run_type == "run_full_normal":
            if target == "additionActions":
                if "Action store" not in values.columns:
                    continue
                store_points = values['Action store']
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    store_points if num_points == -
                    1 else collapse2numpoints(num_points, y=store_points),
                    color="blue",
                    line_width=_LINE_WIDTH,
                )
                legend_items.append(
                    ("Action store", [cur_line]))
                not_store_points = values['Action not store']
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    not_store_points if num_points == -
                    1 else collapse2numpoints(num_points, y=not_store_points),
                    color="red",
                    line_width=_LINE_WIDTH,
                )
                legend_items.append(
                    ("Action not store", [cur_line]))
                continue
            elif target == "evictionActions":
                if "Action delete" not in values.columns:
                    continue
                delete_points = values['Action delete']
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    delete_points if num_points == -
                    1 else collapse2numpoints(num_points, y=delete_points),
                    color="red",
                    line_width=_LINE_WIDTH,
                )
                legend_items.append(
                    ("Action delete", [cur_line]))
                not_delete_points = values['Action not delete']
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    not_delete_points if num_points == -
                    1 else collapse2numpoints(num_points, y=not_delete_points),
                    color="blue",
                    line_width=_LINE_WIDTH,
                )
                legend_items.append(
                    ("Action not delete", [cur_line]))
                continue
            elif target == "sizePerc":
                cache_size = get_cache_size(cache_name)
                points = (values['size'] / cache_size) * 100.
            elif target == "avgFreeSpacePerc":
                cache_size = get_cache_size(cache_name)
                points = (values['avg free space'] / cache_size) * 100.
            elif target == "stdDevFreeSpace":
                points = values['std dev free space']
            elif target == "costFunction":
                cache_size = get_cache_size(cache_name)
                points = ((values['written data'] +
                           values['deleted data'] +
                           values['read on miss data']) / cache_size) * 100.
            elif target == "cacheCost":
                cache_size = get_cache_size(cache_name)
                points = ((values['written data'] +
                           values['deleted data']) / cache_size) * 100.
                # Old cost
                # points = values['written data'] + \
                #     values['deleted data'] + values['read on miss data']
            elif target == "costFunctionVs":
                for inner_cache_name, inner_values in filter_results(
                    results, run_type, filters
                ):
                    if inner_cache_name.find("lru_") != -1 and inner_cache_name.index("lru_") == 0:
                        lru_cost = inner_values['written data'] + \
                            inner_values['deleted data'] + \
                            inner_values['read on miss data']
                        break
                else:
                    continue
                points = values['written data'] + \
                    values['deleted data'] + values['read on miss data']
                points /= lru_cost
            elif target == "cacheCostVs":
                for inner_cache_name, inner_values in filter_results(
                    results, run_type, filters
                ):
                    if inner_cache_name.find("lru_") != -1 and inner_cache_name.index("lru_") == 0:
                        lru_cost = inner_values['written data'] + \
                            inner_values['deleted data']
                        break
                else:
                    continue
                points = values['written data'] + values['deleted data']
                points /= lru_cost
            elif target == "miss":
                cache_size = get_cache_size(cache_name)
                points = (
                    values['read on miss data'] - values['written data']
                ) / cache_size*100
            elif target == "throughput":
                points = (
                    (
                        values['read on hit data'] - values['written data']
                    ) / values['read data']
                ) * 100.
                # Old throughput
                # points = (values['read on hit data'] / values['written data']) * 100.
                # very Old throughput
                # points = values['read on hit data'] / values['written data']
            elif target == "throughputVs":
                for inner_cache_name, inner_values in filter_results(
                    results, run_type, filters
                ):
                    if inner_cache_name.find("lru_") != -1 and inner_cache_name.index("lru_") == 0:
                        lru_cost = inner_values['read on hit data'] - \
                            inner_values['written data']
                        break
                else:
                    continue
                points = values['read on hit data'] - values['written data']
                points /= lru_cost
            elif target == "network_in_saturation":
                points = (values['read on miss data'] / cur_band) * 100.
            elif target == "network_out_saturation":
                points = (values['read data'] / cur_band) * 100.
            elif target == "readOnHitRatio":
                points = (values['read on hit data'] /
                          values['read data']) * 100.
            elif target == "readOnMissRatio":
                points = (values['read on miss data'] /
                          values['read data']) * 100.
            elif target == "writtenRatio":
                points = (values['written data'] /
                          values['read data']) * 100.
            elif target == "deletedRatio":
                points = (values['deleted data'] /
                          values['read data']) * 100.
            else:
                raise Exception(f"Unknown target '{target}'...")

            cur_line = cur_fig.line(
                dates if num_points == -
                1 else collapse2numpoints(num_points, x=dates),
                points if num_points == -
                1 else collapse2numpoints(num_points, y=points),
                color=color_table[cache_name],
                line_width=_LINE_WIDTH,
            )
            legend_items.append(
                (get_cache_legend_name(cache_name), [cur_line]))
            mean_point = sum(points) / len(points)
            if mean_point != 1.0:
                cur_line = cur_fig.line(
                    dates if num_points == -
                    1 else collapse2numpoints(num_points, x=dates),
                    [mean_point for _ in range(
                        len(dates) if num_points == -1 else num_points)],
                    line_color=color_table[cache_name],
                    line_dash="dashdot",
                    line_width=3.,
                )
                legend_items.append(
                    (f"Mean {get_cache_legend_name(cache_name)} -> {mean_point:0.2f}{'%' if target == 'gain' else ''}",
                     [cur_line])
                )
        elif run_type == "run_single_window":
            points = results['run_full_normal'][cache_name][read_data_type] / \
                results['run_full_normal'][cache_name]['written data']
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=_LINE_WIDTH,
            )
            legend_items.append((cache_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = cur_fig.line(
                dates,
                [mean_point for _ in range(len(dates))],
                line_color=color_table[cache_name],
                line_dash="dashdot",
                line_width=3.,
            )
            legend_items.append(
                (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
            )
            single_window_name = f'{cache_name} - single window'
            next_window_name = f'{cache_name} - next window'
            single_windows = pd.concat(
                [
                    window
                    for name, window in sorted(
                        values.items(),
                        key=lambda elm: elm[0],
                    )
                ]
            ).sort_values(by=['date'])
            points = single_windows[read_data_type] / \
                single_windows['written data']
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[f'{cache_name}_single'],
                line_width=_LINE_WIDTH,
            )
            legend_items.append((single_window_name, [cur_line]))
            next_windows = pd.concat(
                [
                    window
                    for name, window in sorted(
                        results['run_next_window'][cache_name].items(),
                        key=lambda elm: elm[0],
                    )
                ]
            ).sort_values(by=['date'])
            cur_dates = [
                elm.split(" ")[0]
                for elm
                in next_windows['date'].astype(str)
            ]
            points = next_windows[read_data_type] / \
                next_windows['written data']
            cur_line = cur_fig.line(
                cur_dates,
                points,
                line_color="red",
                line_alpha=0.9,
                line_width=_LINE_WIDTH,
                line_dash="dashed",
            )
            legend_items.append((next_window_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = cur_fig.line(
                cur_dates,
                [mean_point for _ in range(len(cur_dates))],
                line_color="red",
                line_dash="dashdot",
                line_width=3.,
            )
            legend_items.append(
                (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
            )
        elif run_type == "run_next_period":
            points = results['run_full_normal'][cache_name][read_data_type] / \
                results['run_full_normal'][cache_name]['written data']
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=_LINE_WIDTH,
            )
            legend_items.append((cache_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = cur_fig.line(
                dates,
                [mean_point for _ in range(len(dates))],
                line_color=color_table[cache_name],
                line_dash="dashdot",
                line_width=3.,
            )
            legend_items.append(
                (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
            )
            single_window_name = f'{cache_name} - single window'
            single_windows = pd.concat(
                [
                    window
                    for name, window in sorted(
                        results['run_single_window'][cache_name].items(),
                        key=lambda elm: elm[0],
                    )
                ]
            )
            points = single_windows.sort_values(by=['date'])
            points = points[read_data_type] / points['written data']
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[f'{cache_name}_single'],
                line_width=_LINE_WIDTH,
            )
            legend_items.append((single_window_name, [cur_line]))
            line_styles = cycle([
                'solid',
                'dashed',
                'dotted',
                'dotdash',
                'dashdot',
            ])
            for period, period_values in values.items():
                cur_period = period_values[
                    ['date', read_data_type, 'written data']
                ][period_values.date.isin(datetimes)].sort_values(by=['date'])
                cur_period_name = f"{cache_name} - from {period.split('-')[0]}"
                points = cur_period[read_data_type] / \
                    cur_period['written data']
                cur_dates = [
                    elm.split(" ")[0]
                    for elm
                    in cur_period['date'].astype(str)
                ]
                cur_dates = [
                    cur_date for cur_date in cur_dates
                    if cur_date in dates
                ]
                if len(cur_dates) > 0:
                    cur_line_style = next(line_styles)
                    cur_line = cur_fig.line(
                        cur_dates,
                        points,
                        line_color="red",
                        line_alpha=0.9,
                        line_width=_LINE_WIDTH,
                        line_dash=cur_line_style,
                    )
                    legend_items.append((cur_period_name, [cur_line]))
                    mean_point = sum(points) / len(points)
                    cur_line = cur_fig.line(
                        cur_dates,
                        [mean_point for _ in range(len(cur_dates))],
                        line_color="red",
                        line_dash="dashdot",
                        line_width=3.,
                    )
                    legend_items.append(
                        (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
                    )

    legend = Legend(items=legend_items, location=(0, 0))
    legend.location = "top_right"
    legend.click_policy = "hide"
    cur_fig.add_layout(legend, 'right' if outer_legend else 'center')
    cur_fig.yaxis.formatter = BasicTickFormatter(
        use_scientific=False)
    cur_fig.xaxis.major_label_orientation = np.pi / 4.
    if num_points == -1:
        cur_fig.xaxis.formatter = FuncTickFormatter(code="""
        var day = parseInt(tick.split("-")[2], 10)
        if ( day%7 == 0 ) { return tick }
        else { return "" }
    """)
    cur_fig.add_tools(SaveTool())
    # cur_fig.legend.label_text_font_size = '16pt'
    # cur_fig.xaxis.axis_label_text_font_size = "16pt"
    # cur_fig.xaxis.major_label_text_font_size = "16pt"
    # cur_fig.yaxis.axis_label_text_font_size = "16pt"
    # cur_fig.yaxis.major_label_text_font_size = "16pt"
    add_window_lines(cur_fig, dates, window_size)

    return cur_fig
