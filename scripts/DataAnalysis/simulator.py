import argparse
import gzip
import os
import subprocess
from datetime import datetime
from itertools import cycle
from multiprocessing import Pool
from os import path, walk
from random import randint, random, seed

import numpy as np
import pandas as pd
from bokeh.io import export_png
from bokeh.layouts import column, row
from bokeh.models import (BasicTickFormatter, BoxZoomTool, Legend, PanTool,
                          Range1d, ResetTool, SaveTool, Span, WheelZoomTool)
from bokeh.palettes import Accent
from bokeh.plotting import Figure, figure, output_file, save
from tqdm import tqdm
from yaspin import yaspin
from yaspin.spinners import Spinners

from DataManager.collector.dataset.reader import SimulatorDatasetReader
from SmartCache.ai.models.generator import DonkeyModel
from SmartCache.sim import get_simulator_exe

# Set seed
seed(42)
np.random.seed(42)


def wait_jobs(processes):
    while job_run(processes):
        for _, process in processes:
            try:
                process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass


def read_output_last_line(output):
    buffer = ""
    cur_char = output.read(1).decode("ascii")
    while cur_char not in ["\r", "\n", '']:
        buffer += cur_char
        cur_char = output.read(1).decode("ascii")
    return buffer


def job_run(processes: list) -> bool:
    running_processes = []
    for task_name, process in processes:
        running = process.returncode is None
        running_processes.append(running)
        if running:
            print(
                f"[{process.pid}][RUNNING][{task_name}]{read_output_last_line(process.stdout)}\x1b[0K", flush=True)
        else:
            print(
                f"[{process.pid}][DONE][{task_name}][Return code -> {process.returncode}]\x1b[0K", flush=True)
            if process.returncode != 0:
                print(
                    f"[{process.pid}][DONE][{task_name}][Return code -> {process.returncode}]", flush=True)
                print(f"{process.stdout.read().decode('ascii')}", flush=True)
                print(f"{process.stderr.read().decode('ascii')}", flush=True)
                exit(process.returncode)

    print(f"\x1b[{len(processes)+1}F")

    return any(running_processes)


def get_result_section(cur_path: str, source_folder: str):
    head, tail = path.split(cur_path)
    section = []
    while head != source_folder:
        section.append(tail)
        head, tail = path.split(head)
    section.append(tail)
    return section


def load_results(folder: str) -> dict:
    results = {}
    for root, dirs, files in walk(folder):
        for file_ in files:
            _, ext = path.splitext(file_)
            if ext == ".csv":
                section = get_result_section(root, folder)
                cur_section = results
                while len(section) > 1:
                    part = section.pop()
                    if part not in cur_section:
                        cur_section[part] = {}
                    cur_section = cur_section[part]
                last_section = section.pop()
                file_path = path.join(root, file_)
                df = pd.read_csv(
                    file_path
                )
                cur_section[last_section] = df

    return results


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


def valid_individual(individual, dataframe, cache_size: float) -> bool:
    return indivudual_size(individual, dataframe) <= cache_size


def indivudual_size(individual, dataframe) -> float:
    cur_series = pd.Series(individual)
    cur_size = sum(dataframe[cur_series]['size'])
    return cur_size


def individual_fitness(individual, dataframe) -> float:
    cur_series = pd.Series(individual)
    cur_size = sum(dataframe[cur_series]['value'])
    return cur_size


def make_it_valid(individual, dataframe, cache_size: float):
    individual_size = indivudual_size(individual, dataframe)
    if individual_size > cache_size:
        nonzero = np.nonzero(individual)[0]
        np.random.shuffle(nonzero)
        sizes = dataframe.loc[nonzero]['size']
        to_false = []
        for cur_idx in nonzero.tolist():
            if individual_size <= cache_size:
                break
            to_false.append(cur_idx)
            individual_size -= sizes[cur_idx]
        if to_false:
            individual[to_false] = False
    return individual


def get_one_solution(gen_input):
    dataframe, cache_size = gen_input
    individual = np.random.randint(2, size=dataframe.shape[0], dtype=bool)
    individual = make_it_valid(individual, dataframe, cache_size)
    return individual


def get_best_configuration(dataframe, cache_size: float,
                           num_generations: int = 1000,
                           population_size: int = 42):
    population = []
    pool = Pool()
    for _, individual in tqdm(enumerate(
        pool.imap(
            get_one_solution,
            [
                (dataframe, cache_size)
                for _ in range(population_size)
            ]
        )
    ), desc="Create Population",
            total=population_size, ascii=True):
        population.append(individual)

    pool.close()
    pool.join()

    # print("[Create best individual with greedy method]")
    # # Create 1 best individual with greedy method
    # best_greedy = np.zeros(dataframe.shape[0], dtype=bool)
    # cur_size = 0.
    # cur_score = 0.

    # for idx, cur_row in enumerate(dataframe.itertuples()):
    #     file_size = cur_row.size
    #     if cur_size + file_size <= cache_size:
    #         cur_size += file_size
    #         cur_score += cur_row.value
    #         best_greedy[idx] = True
    #     else:
    #         break

    # population.append(best_greedy)

    best = evolve_with_genetic_algorithm(
        population, dataframe, cache_size, num_generations
    )

    return best


def cross(element, factor):
    if element >= factor:
        return 1
    return 0


V_CROSS = np.vectorize(cross)


def mutate(element, factor):
    if element <= factor:
        return 1
    return 0


V_MUTATE = np.vectorize(mutate)


def crossover(parent_a, parent_b) -> 'np.Array':
    """Perform and uniform corssover."""
    new_individual = np.zeros(len(parent_a)).astype(bool)
    uniform_crossover = np.random.rand(len(parent_a))
    cross_selection = V_CROSS(uniform_crossover, 0.75).astype(bool)
    new_individual[cross_selection] = parent_a[cross_selection]
    cross_selection = ~cross_selection
    new_individual[cross_selection] = parent_b[cross_selection]
    return new_individual


def mutation(individual) -> 'np.Array':
    """Bit Flip mutation."""
    flip_bits = np.random.rand(len(individual))
    mutant_selection = V_MUTATE(flip_bits, 0.1).astype(bool)
    individual[mutant_selection] = ~individual[mutant_selection]
    return individual


def generation(gen_input):
    best, individual, dataframe, cache_size = gen_input
    child_0 = crossover(best, individual)
    child_1 = ~child_0

    child_0 = mutation(child_0)
    child_0 = make_it_valid(
        child_0, dataframe, cache_size)
    child_0_fitness = individual_fitness(child_0, dataframe)

    child_1 = mutation(child_1)
    child_1 = make_it_valid(
        child_1, dataframe, cache_size)
    child_1_fitness = individual_fitness(child_1, dataframe)

    return (child_0, child_0_fitness, child_1, child_1_fitness)


def roulette_wheel(fitness: list, extractions: int = 1):
    cur_fitness = np.array(fitness)
    fitness_sum = np.sum(cur_fitness)
    probabilities = cur_fitness / fitness_sum
    probabilities = probabilities.tolist()

    for _ in range(extractions):
        candidates = []
        while(True):
            idx = randint(0, len(probabilities) - 1)
            cur_probability = probabilities[idx]
            if random() <= cur_probability:
                candidates.append(idx)
            if len(candidates) == 2:
                yield candidates
                break


def evolve_with_genetic_algorithm(population, dataframe,
                                  cache_size: float,
                                  num_generations: int
                                  ):
    cur_population = [elm for elm in population]
    pool = Pool()

    cur_fitness = []
    for indivudual in cur_population:
        cur_fitness.append(individual_fitness(indivudual, dataframe))

    for _ in tqdm(
        range(num_generations),
        desc="Evolution", ascii=True,
        position=0
    ):
        idx_best = np.argmax(cur_fitness)
        best = cur_population[idx_best]

        childrens = []
        childrens_fitness = []

        for cur_idx, (child_0, child_0_fitness, child_1, child_1_fitness) in tqdm(
                enumerate(
                    pool.imap(
                        generation,
                        [
                            (
                                cur_population[candidates[0]],
                                cur_population[candidates[1]],
                                dataframe,
                                cache_size
                            )
                            for candidates
                            in roulette_wheel(cur_fitness, len(population))
                        ]
                    )
                ),
                desc=f"Make new generation [Best: {cur_fitness[idx_best]:0.0f}][Mean: {np.mean(cur_fitness):0.0f}][Var: {np.var(cur_fitness):0.0f}]",
                ascii=True, position=1, leave=False,
                total=len(cur_population),
        ):
            childrens += [child_0, child_1]
            childrens_fitness += [child_0_fitness, child_1_fitness]

        new_population = cur_population + childrens
        new_fitness = cur_fitness + childrens_fitness

        for idx, real_idx in enumerate(reversed(np.argsort(new_fitness).tolist())):
            if idx < len(population):
                cur_population[idx] = new_population[real_idx]
                cur_fitness[idx] = new_fitness[real_idx]
            else:
                break

    pool.close()
    pool.join()

    idx_best = np.argmax(cur_fitness)
    return cur_population[idx_best]


def compare_greedy_solution(dataframe, cache_size):
    cur_size = 0.
    cur_score = 0.

    for cur_row in dataframe.itertuples():
        file_size = cur_row.size
        if cur_size + file_size <= cache_size:
            cur_size += file_size
            cur_score += cur_row.value
        else:
            break

    ga_size = sum(dataframe[dataframe['class']]['size'].to_list())
    ga_score = sum(dataframe[dataframe['class']]['value'].to_list())

    print("[Results]...")
    print(f"[Size: \t{cur_size:0.2f}][Score: \t{cur_score:0.2f}][Greedy]")
    print(f"[Size: \t{ga_size:0.2f}][Score: \t{ga_score:0.2f}][GA]")


def main():
    parser = argparse.ArgumentParser(
        "simulator", description="Simulation and result plotting")
    parser.add_argument('action', choices=['simulate', 'plot', 'train', 'create_dataset'],
                        default="simulate",
                        help='Action requested')
    parser.add_argument('source', type=str,
                        default="./results_8w_with_sizes_csv",
                        help='The folder where the json results are stored [DEFAULT: "./results_8w_with_sizes_csv"]')
    parser.add_argument('--cache-types', type=str,
                        default="lru,weightedLRU",
                        help='Comma separated list of cache to simulate [DEFAULT: "lru,weightedLRU"]')
    parser.add_argument('--out-folder', type=str,
                        default="simulation_results",
                        help='The folder where the simulation results will be stored [DEFAULT: "simulation_results"]')
    parser.add_argument('--read-on-hit', type=bool,
                        default=True,
                        help='Use read on hit data [DEFAULT: True]')
    parser.add_argument('-FEB', '--force-exe-build', type=bool,
                        default=True,
                        help='Force to build the simulation executable [DEFAULT: True]')
    parser.add_argument('-CS', '--cache-size', type=int,
                        default=104857600,
                        help='Size of the cache to simulate in Mega Bytes [DEFAULT: 104857600]')
    parser.add_argument('-R', '--region', type=str,
                        default="all",
                        help='Region of the data to simulate [DEFAULT: "all"]')
    parser.add_argument('-WS', '--window-size', type=int,
                        default=7,
                        help='Size of the window to simulate [DEFAULT: 7]')
    parser.add_argument('-WSTA', '--window-start', type=int,
                        default=0,
                        help='Window where to start from [DEFAULT: 0]')
    parser.add_argument('-WSTO', '--window-stop', type=int,
                        default=4,
                        help='Window where to stop [DEFAULT: 4]')
    parser.add_argument('--population-size', type=int,
                        default=10000,
                        help='Num. of individuals in the GA [DEFAULT: 100]')
    parser.add_argument('--num-generations', type=int,
                        default=1000,
                        help='Num. of generations of GA [DEFAULT: 200]')
    parser.add_argument('--out-html', type=bool,
                        default=True,
                        help='Plot the output as a html [DEFAULT: True]')
    parser.add_argument('--out-png', type=bool,
                        default=False,
                        help='Plot the output as a png (requires phantomjs-prebuilt installed with npm) [DEFAULT: False]')
    parser.add_argument('--plot-filters', type=str,
                        default="",
                        help='A comma separate string to search as filters')
    parser.add_argument('--only-CPU', type=bool,
                        default=True,
                        help='Force to use only CPU with TensorFlow [DEFAULT: True]')
    parser.add_argument('--plot-resolution', type=str,
                        default="640,480",
                        help='A comma separate string representing the target resolution of each plot [DEFAULT: 640,480]')
    parser.add_argument('--ai-model', type=str,
                        default="donkey_model.h5",
                        help='Ai Model file path [DEFAULT: "donkey_model.h5"]')

    args, _ = parser.parse_known_args()

    if args.only_CPU:
        os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
    else:
        # Make visible only first device
        os.environ['CUDA_VISIBLE_DEVICES'] = '0'

    if args.action == "simulate":
        simulator_exe = get_simulator_exe(force_creation=args.force_exe_build)
        cache_types = args.cache_types.split(",")

        base_dir = path.join(path.dirname(
            path.abspath(__file__)), args.out_folder)
        os.makedirs(base_dir, exist_ok=True)

        with open(path.join(base_dir, "simulator.version"), "w") as ver_file:
            output = subprocess.check_output(
                " ".join([simulator_exe, 'version']),
                shell=True,
            )
            ver_file.write(output.decode('ascii'))

        processes = []

        ##
        # Single Window runs
        single_window_run_dir = working_dir = path.join(
            base_dir,
            "run_single_window"
        )
        os.makedirs(single_window_run_dir, exist_ok=True)

        model_processes = []

        for window_idx in range(args.window_start, args.window_stop):
            for cache_type in cache_types:
                if cache_type == 'aiLRU':
                    cur_model = DonkeyModel()
                    cur_model.load(args.ai_model)
                    cur_model.add_feature_converter(
                        path.join(
                            path.dirname(args.ai_model),
                            "featureConverter.dump.pickle"
                        )
                    )
                    cur_model_port = 4200+window_idx
                    cur_model.serve(port=cur_model_port)
                    model_processes.append((cur_model, cur_model))

                working_dir = path.join(
                    single_window_run_dir,
                    f"{cache_type}_{int(args.cache_size/1024**2)}T_{args.region}",
                    f"window_{window_idx}",
                )
                os.makedirs(working_dir, exist_ok=True)
                exe_args = [
                    simulator_exe,
                    "simulate" if cache_type != 'aiLRU' else "testAI",
                    cache_type,
                    path.abspath(args.source),
                    f"--size={args.cache_size}",
                    f"--simRegion={args.region}",
                    f"--simWindowSize={args.window_size}",
                    f"--simStartFromWindow={window_idx}",
                    f"--simStopWindow={window_idx+1}",
                    "--simDump=true",
                    "--simDumpFileName=dump.json.gz",
                ]
                if cache_type == 'aiLRU':
                    exe_args.append("--aiHost=127.0.0.1")
                    exe_args.append(f"--aiPort={cur_model_port}")

                cur_process = subprocess.Popen(
                    " ".join(exe_args),
                    shell=True,
                    cwd=working_dir,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                processes.append(("Single Window", cur_process))

        wait_jobs(processes)

        ##
        # Normal runs
        normal_run_dir = working_dir = path.join(
            base_dir,
            "run_full_normal"
        )
        os.makedirs(normal_run_dir, exist_ok=True)

        for cache_type in cache_types:
            working_dir = path.join(
                normal_run_dir,
                f"{cache_type}_{int(args.cache_size/1024**2)}T_{args.region}"
            )
            os.makedirs(working_dir, exist_ok=True)
            exe_args = [
                simulator_exe,
                "simulate" if cache_type != 'aiLRU' else "testAI",
                cache_type,
                path.abspath(args.source),
                f"--size={args.cache_size}",
                f"--simRegion={args.region}",
                f"--simWindowSize={args.window_size}",
                f"--simStartFromWindow={args.window_start}",
                f"--simStopWindow={args.window_stop}",
            ]
            if cache_type == 'aiLRU':
                cur_model_port = 4200
                exe_args.append("--aiHost=127.0.0.1")
                exe_args.append(f"--aiPort={cur_model_port}")
            cur_process = subprocess.Popen(
                " ".join(exe_args),
                shell=True,
                cwd=working_dir,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            processes.append(("Full Run", cur_process))
            if cache_type == 'aiLRU':
                wait_jobs(processes)

        wait_jobs(processes)

        ##
        # Next windows
        nexxt_window_run_dir = working_dir = path.join(
            base_dir,
            "run_next_window"
        )
        os.makedirs(nexxt_window_run_dir, exist_ok=True)

        for window_idx in range(args.window_start, args.window_stop):
            for cache_type in cache_types:
                working_dir = path.join(
                    nexxt_window_run_dir,
                    f"{cache_type}_{int(args.cache_size/1024**2)}T_{args.region}",
                    f"window_{window_idx+1}",
                )
                dump_dir = path.join(
                    single_window_run_dir,
                    f"{cache_type}_{int(args.cache_size/1024**2)}T_{args.region}",
                    f"window_{window_idx}",
                )
                os.makedirs(working_dir, exist_ok=True)
                exe_args = [
                    simulator_exe,
                    "simulate" if cache_type != 'aiLRU' else "testAI",
                    cache_type,
                    path.abspath(args.source),
                    f"--size={args.cache_size}",
                    f"--simRegion={args.region}",
                    f"--simWindowSize={args.window_size}",
                    f"--simStartFromWindow={window_idx+1}",
                    f"--simStopWindow={window_idx+2}",
                    "--simLoadDump=true",
                    f"--simLoadDumpFileName={path.join(dump_dir, 'dump.json.gz')}",
                ]
                if cache_type == 'aiLRU':
                    cur_model_port = 4200+window_idx
                    exe_args.append("--aiHost=127.0.0.1")
                    exe_args.append(f"--aiPort={cur_model_port}")
                cur_process = subprocess.Popen(
                    " ".join(exe_args),
                    shell=True,
                    cwd=working_dir,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                processes.append(("Next Window", cur_process))

        wait_jobs(processes)

        ##
        # Next Period
        next_period_run_dir = working_dir = path.join(
            base_dir,
            "run_next_period"
        )
        os.makedirs(next_period_run_dir, exist_ok=True)

        for window_idx in range(args.window_start, args.window_stop):
            for cache_type in cache_types:
                working_dir = path.join(
                    next_period_run_dir,
                    f"{cache_type}_{int(args.cache_size/1024**2)}T_{args.region}",
                    f"windows_{window_idx+1}-{args.window_stop}",
                )
                dump_dir = path.join(
                    single_window_run_dir,
                    f"{cache_type}_{int(args.cache_size/1024**2)}T_{args.region}",
                    f"window_{window_idx}",
                )
                os.makedirs(working_dir, exist_ok=True)
                exe_args = [
                    simulator_exe,
                    "simulate" if cache_type != 'aiLRU' else "testAI",
                    cache_type,
                    path.abspath(args.source),
                    f"--size={args.cache_size}",
                    f"--simRegion={args.region}",
                    f"--simWindowSize={args.window_size}",
                    f"--simStartFromWindow={window_idx+1}",
                    f"--simStopWindow={args.window_stop+1}",
                    "--simLoadDump=true",
                    f"--simLoadDumpFileName={path.join(dump_dir, 'dump.json.gz')}",
                ]
                if cache_type == 'aiLRU':
                    cur_model_port = 4200+window_idx
                    exe_args.append("--aiHost=127.0.0.1")
                    exe_args.append(f"--aiPort={cur_model_port}")
                cur_process = subprocess.Popen(
                    " ".join(exe_args),
                    shell=True,
                    cwd=working_dir,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                if cache_type == 'aiLRU':
                    exe_args.append("--aiHost=127.0.0.1")
                    exe_args.append(f"--aiPort={cur_model_port}")
                processes.append(("Next Period", cur_process))

        wait_jobs(processes)

    elif args.action == "plot":
        filters = [elm for elm in args.plot_filters.split(",") if elm]
        results = load_results(args.source)
        plot_width, plot_height = [
            int(elm) for elm in args.plot_resolution.split(",")
            if elm
        ]
        plot_results(
            args.source, results,
            window_size=args.window_size,
            filters=filters,
            html=args.out_html,
            png=args.out_png,
            plot_width=plot_width,
            plot_height=plot_height,
            read_on_hit=args.read_on_hit,
        )

    elif args.action == "train":
        dataset = SimulatorDatasetReader(args.source)
        dataset.modify_column(
            'size',
            lambda column: (column / 1024**2)
        ).modify_column(
            'size',
            lambda column: (column / 1000).astype(int)
        ).modify_column(
            'avgTime',
            lambda column: (column / 100).astype(int)
        ).make_converter_map(
            [
                'class',
            ],
            unknown_values=False
        ).make_converter_map(
            [
                'size',
                'avgTime',
            ],
            sort_values=True
        ).make_converter_map(
            [
                'siteName',
                'userID',
                'fileType',
                'dataType'
            ]
        ).store_converter_map(
        ).make_data_and_labels(
            [
                'siteName',
                'userID',
                'fileType',
                'dataType',
                'numReq',
                'avgTime',
                'size',
            ],
            'class'
        ).save_data_and_labels()
        model = DonkeyModel()
        data, labels = dataset.data
        # print(data.shape)
        model.train(data, labels)
        model.save(path.join(
            path.dirname(args.source), "donkey_model"
        ))

    elif args.action == "create_dataset":
        base_dir = path.join(path.dirname(path.abspath(__file__)), "datasets")
        os.makedirs(base_dir, exist_ok=True)

        day_files = []
        for root, dirs, files in walk(args.source):
            for file_ in tqdm(sorted(files), desc="Search files", ascii=True):
                head, tail = path.splitext(file_)
                if tail == ".gz":
                    _, tail = path.splitext(head)
                    if tail == ".csv" or tail == ".feather":
                        day_files.append(
                            path.join(
                                root, file_
                            )
                        )

        windows = []
        cur_window = []
        for file_ in day_files:
            if len(cur_window) < args.window_size:
                cur_window.append(file_)
            else:
                windows.append(cur_window)
                cur_window = []
        else:
            if len(cur_window):
                windows.append(cur_window)
                cur_window = []

        for idx, window in enumerate(windows):
            list_df = []
            files = {}
            for file_ in tqdm(window, desc=f"Create window {idx} dataframe", ascii=True):
                head, _ = path.splitext(file_)
                _, tail = path.splitext(head)
                with gzip.GzipFile(file_, "rb") as cur_file:
                    if tail == ".csv":
                        df = pd.read_csv(cur_file)
                    elif tail == ".feather":
                        df = pd.read_feather(cur_file)
                    else:
                        raise Exception(
                            f"Error: extension '{tail}' not supported...")
                list_df.append(df)
            cur_df = pd.concat(list_df, ignore_index=True).dropna()
            # print(cur_df.shape)
            if args.region != 'all':
                cur_df = cur_df[cur_df['site_name'].str.contains(
                    f"_{args.region}_", case=False)
                ]
            # print(cur_df.shape)

            tick_counter = 1
            stat_avg_time = []
            stat_num_req = []
            max_history = 64

            for cur_row in tqdm(cur_df.itertuples(), total=cur_df.shape[0],
                                desc=f"Parse window {idx} dataframe",
                                ascii=True):
                cur_filename = cur_row.filename
                cur_size = cur_row.size
                if cur_filename not in files:
                    data_type, _, _, file_type = cur_filename.split("/")[2:6]
                    files[cur_filename] = {
                        'size': cur_size,
                        'totReq': 0,
                        'days': set([]),
                        'siteName': cur_row.site_name,
                        'userID': cur_row.user,
                        'reqHistory': [],
                        'lastReq': 0,
                        'fileType': file_type,
                        'dataType': data_type
                    }
                cur_time = datetime.fromtimestamp(cur_row.day)
                cur_file_stats = files[cur_filename]
                cur_file_stats['totReq'] += 1
                cur_file_stats['lastReq'] = cur_time
                if len(cur_file_stats['reqHistory']) > max_history:
                    cur_file_stats['reqHistory'].pop()

                cur_file_stats['reqHistory'].append(cur_time)

                assert cur_file_stats['size'] == cur_size, f"{cur_file_stats['size']} != {cur_size}"

                cur_file_stats['days'] |= set((cur_row.day, ))

                stat_num_req.append(cur_file_stats['totReq'])
                stat_avg_time.append(
                    sum([
                        (cur_file_stats['lastReq'] - elm).total_seconds() / 60.
                        for elm in cur_file_stats['reqHistory']
                    ]) / max_history
                )

                tick_counter += 1

            cur_df['avg_time'] = stat_avg_time
            cur_df['num_req'] = stat_num_req

            files_df = pd.DataFrame(
                data={
                    'filename': [filename
                                 for filename in files],
                    'size': [files[filename]['size']
                             for filename in files],
                    'totReq': [files[filename]['totReq']
                               for filename in files],
                    # 'days': [len(files[filename]['days'])
                    #          for filename in files],
                }
            )

            # Remove 1 request files
            # files_df = files_df.drop(files_df[files_df.totReq == 1].index)

            # TO Megabytes
            files_df['size'] = files_df['size'] / 1024**2

            # Add value
            files_df['value'] = (files_df['size'] *
                                 files_df['totReq'])  # * files_df['days']

            # Remove low value files
            # q1 = files_df.value.describe().quantile(0.25)
            # files_df = files_df.drop(files_df[files_df.value < q1].index)

            # Sort and reset indexes
            files_df = files_df.sort_values(by=['value'], ascending=False)
            files_df = files_df.reset_index(drop=True)
            # print(files_df)

            # print(sum(files_df['size']), args.cache_size, sum(files_df['size'])/args.cache_size)
            cache_size_factor = (sum(files_df['size'])/args.cache_size) / 2.

            best_files = get_best_configuration(
                files_df, args.cache_size*cache_size_factor,
                population_size=args.population_size,
                num_generations=args.num_generations,
            )

            files_df['class'] = best_files
            datest_out_file = path.join(
                base_dir,
                f"dataset_window_{idx:02d}.feather.gz"
            )

            compare_greedy_solution(
                files_df, args.cache_size*cache_size_factor
            )

            dataset_data = []
            len_dataset = int(len(cur_df) * 0.42)

            for cur_row in tqdm(cur_df.sample(len_dataset).itertuples(),
                                total=len_dataset,
                                desc=f"Create dataset {idx}", ascii=True):
                filename = cur_row.filename
                try:
                    cur_class = files_df.loc[
                        files_df.filename == filename, 'class'
                    ].to_list().pop()
                except IndexError:
                    cur_class = False
                dataset_data.append(
                    [
                        files[filename]['siteName'],
                        files[filename]['userID'],
                        cur_row.num_req,
                        cur_row.avg_time,
                        files[filename]['size'],
                        files[filename]['fileType'],
                        files[filename]['dataType'],
                        cur_class,
                    ]
                )

            dataset_df = pd.DataFrame(
                dataset_data, columns=(
                    'siteName', 'userID', 'numReq', 'avgTime',
                    'size', 'fileType', 'dataType', 'class'
                )
            )

            with yaspin(Spinners.bouncingBall,
                        text=f"[Store dataset][{datest_out_file}]"
                        ):
                with gzip.GzipFile(datest_out_file, "wb") as out_file:
                    dataset_df.to_feather(out_file)


if __name__ == "__main__":
    main()
