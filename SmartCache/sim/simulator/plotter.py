import os
from itertools import cycle

import numpy as np
import pandas as pd
from bokeh.io import export_png
from bokeh.layouts import column, row
from bokeh.models import (BasicTickFormatter, BoxZoomTool, Legend, PanTool,
                          Range1d, ResetTool, SaveTool, Span, WheelZoomTool)
from bokeh.palettes import Accent
from bokeh.plotting import Figure, figure, output_file, save
from tqdm import tqdm


def update_colors(new_name: str, color_table: dict):
    names = list(color_table.keys()) + [new_name]
    colors = cycle(Accent[8])
    for name in sorted(names):
        cur_color = next(colors)
        color_table[name] = cur_color
    for name in sorted(names):
        cur_color = next(colors)
        color_table[f'{name}_single'] = cur_color


def add_window_lines(cur_fig, dates: list, window_size: int):
    cur_fig.renderers.extend([
        Span(
            location=idx+0.5, dimension='height',
            line_color='black', line_width=0.9
        )
        for idx in range(0, len(dates), window_size)
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


def plot_column(tools: list,
                results: dict,
                dates: list,
                filters: list,
                color_table: dict,
                window_size: int,
                x_range=None,
                column: str = "hit rate",
                title: str = "Hit Rate",
                y_axis_label: str = "Hit rate %",
                run_type: str = "run_full_normal",
                datetimes: list = [],
                plot_width: int = 640,
                plot_height: int = 480,
                ) -> 'Figure':
    cur_fig = figure(
        tools=tools,
        title=title,
        x_axis_label="Day",
        y_axis_label=y_axis_label,
        x_range=x_range if x_range else dates,
        plot_width=plot_width,
        plot_height=plot_height,
    )

    if column == "hit rate":
        cur_fig.y_range = Range1d(0, 100)

    legend_items = []

    for cache_name, values in filter_results(
        results, run_type, filters
    ):
        if run_type == "run_full_normal":
            points = values[column]
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=5.,
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
        elif run_type == "run_single_window":
            points = results['run_full_normal'][cache_name][column]
            cur_line = cur_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=5.,
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
                line_width=5.,
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
                line_width=5.,
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
                legend=cache_name,
                color=color_table[cache_name],
                line_width=5.,
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
                line_width=5.,
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
                        line_width=5.,
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
    cur_fig.add_layout(legend, 'right')
    cur_fig.yaxis.formatter = BasicTickFormatter(use_scientific=False)
    cur_fig.xaxis.major_label_orientation = np.pi / 4.
    cur_fig.add_tools(SaveTool())
    add_window_lines(cur_fig, dates, window_size)

    return cur_fig


def plot_read_on_write_data(tools: list,
                            results: dict,
                            dates: list,
                            filters: list,
                            color_table: dict,
                            window_size: int,
                            x_range=None,
                            read_on_hit: bool = True,
                            title: str = "Read on Write data",
                            run_type: str = "run_full_normal",
                            datetimes: list = [],
                            plot_width: int = 640,
                            plot_height: int = 480,
                            ) -> 'Figure':
    read_on_write_data_fig = figure(
        tools=tools,
        title=title,
        x_axis_label="Day",
        y_axis_label="Ratio (Read on hit/Write)" if read_on_hit else "Ratio (Read/Write)",
        y_axis_type="log",
        x_range=x_range if x_range else dates,
        plot_width=plot_width,
        plot_height=plot_height,
    )

    hline_1 = Span(
        location=1.0, dimension='width', line_dash="dashed",
        line_color="black", line_width=5.,
    )
    read_on_write_data_fig.renderers.extend([hline_1])

    read_data_type = 'read on hit data' if read_on_hit else 'read data'
    legend_items = []

    for cache_name, values in filter_results(
        results, run_type, filters
    ):
        if run_type == "run_full_normal":
            points = values[read_data_type] / values['written data']
            cur_line = read_on_write_data_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=5.,
            )
            legend_items.append((cache_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = read_on_write_data_fig.line(
                dates,
                [mean_point for _ in range(len(dates))],
                line_color=color_table[cache_name],
                line_dash="dashdot",
                line_width=3.,
            )
            legend_items.append(
                (f"Mean {cache_name} -> {mean_point:0.2f}", [cur_line])
            )
        elif run_type == "run_single_window":
            points = results['run_full_normal'][cache_name][read_data_type] / \
                results['run_full_normal'][cache_name]['written data']
            cur_line = read_on_write_data_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=5.,
            )
            legend_items.append((cache_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = read_on_write_data_fig.line(
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
            cur_line = read_on_write_data_fig.line(
                dates,
                points,
                color=color_table[f'{cache_name}_single'],
                line_width=5.,
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
            cur_line = read_on_write_data_fig.line(
                cur_dates,
                points,
                line_color="red",
                line_alpha=0.9,
                line_width=5.,
                line_dash="dashed",
            )
            legend_items.append((next_window_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = read_on_write_data_fig.line(
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
            cur_line = read_on_write_data_fig.line(
                dates,
                points,
                color=color_table[cache_name],
                line_width=5.,
            )
            legend_items.append((cache_name, [cur_line]))
            mean_point = sum(points) / len(points)
            cur_line = read_on_write_data_fig.line(
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
            cur_line = read_on_write_data_fig.line(
                dates,
                points,
                color=color_table[f'{cache_name}_single'],
                line_width=5.,
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
                    cur_line = read_on_write_data_fig.line(
                        cur_dates,
                        points,
                        line_color="red",
                        line_alpha=0.9,
                        line_width=5.,
                        line_dash=cur_line_style,
                    )
                    legend_items.append((cur_period_name, [cur_line]))
                    mean_point = sum(points) / len(points)
                    cur_line = read_on_write_data_fig.line(
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
    read_on_write_data_fig.add_layout(legend, 'right')
    read_on_write_data_fig.yaxis.formatter = BasicTickFormatter(
        use_scientific=False)
    read_on_write_data_fig.xaxis.major_label_orientation = np.pi / 4.
    read_on_write_data_fig.add_tools(SaveTool())
    add_window_lines(read_on_write_data_fig, dates, window_size)

    return read_on_write_data_fig


def plot_results(folder: str, results: dict,
                 filters: list = [], window_size: int = 1,
                 html: bool = True, png: bool = False,
                 plot_width: int = 640,
                 plot_height: int = 480,
                 read_on_hit: bool = True,
                 ):
    color_table = {}
    dates = []
    datetimes = []

    if html:
        output_file(
            os.path.join(
                folder,
                "results.html"
            ),
            "Results",
            mode="inline"
        )

    # Tools
    tools = [BoxZoomTool(dimensions='width'), PanTool(
        dimensions='width'), ResetTool()]

    # Update colors
    for cache_name, _ in filter_results(
        results, 'run_full_normal', filters
    ):
        update_colors(cache_name, color_table)

    # Get dates
    for cache_name, values in filter_results(
        results, 'run_full_normal', filters
    ):
        if not dates:
            dates = [
                elm.split(" ")[0]
                for elm
                in values['date'].astype(str)
            ]
            datetimes = values['date']
            break

    figs = []
    run_full_normal_hit_rate_figs = []
    run_full_normal_data_rw_figs = []
    run_full_normal_data_read_stats_figs = []
    run_single_window_figs = []
    run_next_period_figs = []

    pbar = tqdm(total=11, desc="Plot results", ascii=True)

    ###########################################################################
    # Hit Rate plot of full normal run
    ###########################################################################
    hit_rate_fig = plot_column(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        column="hit rate",
        title="Hit Rate - Full Normal Run",
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_full_normal_hit_rate_figs.append(hit_rate_fig)
    pbar.update(1)

    ###########################################################################
    # Read on Write data plot of full normal run
    ###########################################################################
    read_on_write_data_fig = plot_read_on_write_data(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        x_range=hit_rate_fig.x_range,
        title="Read on Write data - Full Normal Run",
        plot_width=plot_width,
        plot_height=plot_height,
        read_on_hit=read_on_hit,
    )
    run_full_normal_hit_rate_figs.append(read_on_write_data_fig)
    pbar.update(1)

    ###########################################################################
    # Written data plot of full normal run
    ###########################################################################
    written_data_fig = plot_column(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        column="written data",
        title="Written data - Full Normal Run",
        y_axis_label="Written data (MB)",
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_full_normal_data_rw_figs.append(written_data_fig)
    pbar.update(1)

    ###########################################################################
    # Read data plot of full normal run
    ###########################################################################
    read_data_fig = plot_column(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        column="read data",
        title="Read data - Full Normal Run",
        y_axis_label="Read data (MB)",
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_full_normal_data_rw_figs.append(read_data_fig)
    pbar.update(1)

    ###########################################################################
    # Deleted data plot of full normal run
    ###########################################################################
    deleted_data_fig = plot_column(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        column="deleted data",
        title="Deleted data - Full Normal Run",
        y_axis_label="Deleted data (MB)",
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_full_normal_data_rw_figs.append(deleted_data_fig)
    pbar.update(1)

    ###########################################################################
    # Read on hit data plot of full normal run
    ###########################################################################
    read_data_fig = plot_column(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        column="read on hit data",
        title="Read on hit data - Full Normal Run",
        y_axis_label="Read on hit data (MB)",
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_full_normal_data_read_stats_figs.append(read_data_fig)
    pbar.update(1)

    ###########################################################################
    # Read on miss data plot of full normal run
    ###########################################################################
    read_data_fig = plot_column(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        column="read on miss data",
        title="Read on miss data - Full Normal Run",
        y_axis_label="Read on miss data (MB)",
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_full_normal_data_read_stats_figs.append(read_data_fig)
    pbar.update(1)

    ###########################################################################
    # Hit Rate compare single and next window plot
    ###########################################################################
    hit_rate_comp_snw_fig = plot_column(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        x_range=hit_rate_fig.x_range,
        column="hit rate",
        title="Hit Rate - Compare single and next window",
        run_type="run_single_window",
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_single_window_figs.append(hit_rate_comp_snw_fig)
    pbar.update(1)

    ###########################################################################
    # Read on Write data data compare single and next window plot
    ###########################################################################
    ronwdata_comp_snw_fig = plot_read_on_write_data(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        x_range=hit_rate_fig.x_range,
        title="Read on Write data - Compare single and next window",
        run_type="run_single_window",
        plot_width=plot_width,
        plot_height=plot_height,
        read_on_hit=read_on_hit,
    )
    run_single_window_figs.append(ronwdata_comp_snw_fig)
    pbar.update(1)

    ###########################################################################
    # Hit Rate compare single window and next period plot
    ###########################################################################
    hit_rate_comp_swnp_fig = plot_column(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        x_range=hit_rate_fig.x_range,
        column="hit rate",
        title="Hit Rate - Compare single window and next period",
        run_type="run_next_period",
        datetimes=datetimes,
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_next_period_figs.append(hit_rate_comp_swnp_fig)
    pbar.update(1)

    ###########################################################################
    # Read on Write data data compare single window and next period plot
    ###########################################################################
    ronwdata_comp_swnp_fig = plot_read_on_write_data(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        x_range=hit_rate_fig.x_range,
        title="Read on Write data - Compare single window and next period",
        run_type="run_next_period",
        datetimes=datetimes,
        plot_width=plot_width,
        plot_height=plot_height,
        read_on_hit=read_on_hit,
    )
    run_next_period_figs.append(ronwdata_comp_swnp_fig)
    pbar.update(1)

    figs.append(column(
        row(*run_full_normal_hit_rate_figs),
        row(*run_full_normal_data_rw_figs),
        row(*run_full_normal_data_read_stats_figs),
        row(*run_single_window_figs),
        row(*run_next_period_figs),
    ))

    if html:
        save(column(*figs))
    if png:
        export_png(column(*figs), filename=os.path.join(
            folder, "results.png"))

    pbar.close()