import argparse
import os
import subprocess
from itertools import cycle
from os import path, walk

import numpy as np
import pandas as pd
from bokeh.io import export_png
from bokeh.layouts import column, row
from bokeh.models import (BoxZoomTool, PanTool, ResetTool, SaveTool,
                          Span, WheelZoomTool)
from bokeh.palettes import Set1
from bokeh.plotting import figure, output_file, save

from SmartCache.sim import get_simulator_exe

CACHE_TYPES = {
    'lru': {},
    'weightedLRU': {},
}


def wait_jobs(processes):
    while job_run(processes):
        for process in processes:
            try:
                process.wait(timeout=0.5)
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
    for process in processes:
        running = process.returncode is None
        running_processes.append(running)
        if running:
            print(
                f"[{process.pid}][RUNNING]{read_output_last_line(process.stdout)}\x1b[0K")
        else:
            print(
                f"[{process.pid}][DONE][Return code -> {process.returncode}]\x1b[0K")

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
                cur_section[last_section] = pd.read_csv(
                    file_path
                )

    return results


def update_colors(new_name: str, color_table: dict):
    names = list(color_table.keys()) + [new_name]
    colors = cycle(Set1[9])
    for name in sorted(names):
        cur_color = next(colors)
        color_table[name] = cur_color
        single_w_name = f'{name} - single window'


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


def plot_results(folder: str, results: dict,
                 filters: list = [], window_size: int = 1,
                 html: bool = True, png: bool = False
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
    run_full_normal_figs = []
    run_single_window_figs = []
    run_next_period_figs = []

    ##
    # Hit Rate plot of full normal run
    hit_rate_fig = figure(
        tools=tools,
        title="Hit Rate - Full Normal Run",
        x_axis_label="Day",
        y_axis_label="Hit rate %",
        y_range=(0, 100),
        x_range=dates,
        plot_width=640,
        plot_height=480,
    )

    for cache_name, values in filter_results(
        results, 'run_full_normal', filters
    ):
        points = values['hit rate']
        hit_rate_fig.line(
            dates,
            points,
            legend=cache_name,
            color=color_table[cache_name],
            line_width=2.,
        )

    hit_rate_fig.legend.location = "top_left"
    hit_rate_fig.legend.click_policy = "hide"
    hit_rate_fig.xaxis.major_label_orientation = np.pi / 4.
    hit_rate_fig.add_tools(SaveTool())
    add_window_lines(hit_rate_fig, dates, window_size)
    run_full_normal_figs.append(hit_rate_fig)

    ##
    # Ratio plot of full normal run
    ratio_fig = figure(
        tools=tools,
        title="Ratio - Full Normal Run",
        x_axis_label="Day",
        y_axis_label="Ratio",
        x_range=hit_rate_fig.x_range,
        plot_width=640,
        plot_height=480,
    )

    for cache_name, values in filter_results(
        results, 'run_full_normal', filters
    ):
        points = values['read data'] / (values['written data'])
        ratio_fig.line(
            dates,
            points,
            legend=cache_name,
            color=color_table[cache_name],
            line_width=2.,
        )

    ratio_fig.legend.location = "top_left"
    ratio_fig.legend.click_policy = "hide"
    ratio_fig.xaxis.major_label_orientation = np.pi / 4.
    ratio_fig.add_tools(SaveTool())
    add_window_lines(ratio_fig, dates, window_size)
    run_full_normal_figs.append(ratio_fig)

    ##
    # Hit Rate compare single and next window plot
    hit_rate_comp_snw_fig = figure(
        tools=tools,
        title="Hit Rate - Compare single and next window",
        x_axis_label="Day",
        y_axis_label="Hit rate %",
        y_range=(0, 100),
        x_range=hit_rate_fig.x_range,
        plot_width=640,
        plot_height=480,
    )

    for cache_name, windows in filter_results(
        results, 'run_single_window', filters
    ):
        single_window_name = f'{cache_name} - single window'
        next_window_name = f'{cache_name} - next window'
        single_windows = pd.concat(
            [
                window
                for name, window in sorted(
                    windows.items(),
                    key=lambda elm: elm[0],
                )
            ]
        )
        next_windows = pd.concat(
            [
                window
                for name, window in sorted(
                    results['run_next_window'][cache_name].items(),
                    key=lambda elm: elm[0],
                )
            ]
        )
        points = single_windows['hit rate']
        hit_rate_comp_snw_fig.line(
            dates,
            points,
            legend=single_window_name,
            color=color_table[cache_name],
            line_width=2.,
        )
        # Merge next windows
        next_values = pd.merge(
            single_windows[['date', 'hit rate']],
            next_windows[['date', 'hit rate']],
            on=['date'],
            how='inner'
        ).sort_values(by=['date']).rename(
            columns={
                'hit rate_y': 'hit rate'
            }
        )[['date', 'hit rate']]
        first_part = single_windows[
            ['date', 'hit rate']
        ][~single_windows.date.isin(next_values.date)]
        next_windows = pd.concat([first_part, next_values])
        points = next_windows['hit rate']
        hit_rate_comp_snw_fig.line(
            dates,
            points,
            legend=next_window_name,
            line_color="red",
            line_alpha=0.9,
            line_width=2.,
            line_dash="dashed",
        )
    hit_rate_comp_snw_fig.legend.location = "top_left"
    hit_rate_comp_snw_fig.legend.click_policy = "hide"
    hit_rate_comp_snw_fig.xaxis.major_label_orientation = np.pi / 4.
    hit_rate_comp_snw_fig.add_tools(SaveTool())
    add_window_lines(hit_rate_comp_snw_fig, dates, window_size)
    run_single_window_figs.append(hit_rate_comp_snw_fig)

    ##
    # Ratio compare single and next window plot
    ratio_comp_snw_fig = figure(
        tools=tools,
        title="Ratio - Compare single and next window",
        x_axis_label="Day",
        y_axis_label="Ratio",
        x_range=hit_rate_fig.x_range,
        plot_width=640,
        plot_height=480,
    )

    for cache_name, windows in filter_results(
        results, 'run_single_window', filters
    ):
        single_window_name = f'{cache_name} - single window'
        next_window_name = f'{cache_name} - next window'
        single_windows = pd.concat(
            [
                window
                for name, window in sorted(
                    windows.items(),
                    key=lambda elm: elm[0],
                )
            ]
        )
        next_windows = pd.concat(
            [
                window
                for name, window in sorted(
                    results['run_next_window'][cache_name].items(),
                    key=lambda elm: elm[0],
                )
            ]
        )
        points = single_windows['read data'] / single_windows['written data']
        ratio_comp_snw_fig.line(
            dates,
            points,
            legend=single_window_name,
            color=color_table[cache_name],
            line_width=2.,
        )
        # Merge next windows
        next_values = pd.merge(
            single_windows[['date', 'read data', 'written data']],
            next_windows[['date', 'read data', 'written data']],
            on=['date'],
            how='inner'
        ).sort_values(by=['date']).rename(
            columns={
                'read data_y': 'read data',
                'written data_y': 'written data'
            }
        )[['date', 'read data', 'written data']]
        first_part = single_windows[
            ['date', 'read data', 'written data']
        ][~single_windows.date.isin(next_values.date)]
        next_windows = pd.concat([first_part, next_values])
        points = next_windows['read data'] / next_windows['written data']
        ratio_comp_snw_fig.line(
            dates,
            points,
            legend=next_window_name,
            line_color="red",
            line_alpha=0.9,
            line_width=2.,
            line_dash="dashed",
        )
    ratio_comp_snw_fig.legend.location = "top_left"
    ratio_comp_snw_fig.legend.click_policy = "hide"
    ratio_comp_snw_fig.xaxis.major_label_orientation = np.pi / 4.
    ratio_comp_snw_fig.add_tools(SaveTool())
    add_window_lines(ratio_comp_snw_fig, dates, window_size)
    run_single_window_figs.append(ratio_comp_snw_fig)

    ##
    # Hit Rate compare single window and next period plot
    hit_rate_comp_swnp_fig = figure(
        tools=tools,
        title="Hit Rate - Compare single window and next period",
        x_axis_label="Day",
        y_axis_label="Hit rate %",
        y_range=(0, 100),
        x_range=hit_rate_fig.x_range,
        plot_width=640,
        plot_height=480,
    )

    for cache_name, windows in filter_results(
        results, 'run_next_period', filters
    ):
        for period, values in windows.items():
            cur_period = values[
                ['date', 'hit rate']
            ][values.date.isin(datetimes)]
            cur_period_name = f"{cache_name} - from {period.split('-')[0]}"
            points = cur_period['hit rate']
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
                hit_rate_comp_swnp_fig.line(
                    cur_dates,
                    points,
                    legend=cur_period_name,
                    line_color=color_table[cache_name],
                    line_alpha=0.9,
                    line_width=2.,
                )
    hit_rate_comp_swnp_fig.legend.location = "top_left"
    hit_rate_comp_swnp_fig.legend.click_policy = "hide"
    hit_rate_comp_swnp_fig.xaxis.major_label_orientation = np.pi / 4.
    hit_rate_comp_swnp_fig.add_tools(SaveTool())
    add_window_lines(hit_rate_comp_swnp_fig, dates, window_size)
    run_next_period_figs.append(hit_rate_comp_swnp_fig)

    ##
    # Ratio compare single window and next period plot
    ratio_comp_swnp_fig = figure(
        tools=tools,
        title="Ratio - Compare single window and next period",
        x_axis_label="Day",
        y_axis_label="Ratio",
        x_range=hit_rate_fig.x_range,
        plot_width=640,
        plot_height=480,
    )

    for cache_name, windows in filter_results(
        results, 'run_next_period', filters
    ):
        for period, values in windows.items():
            cur_period = values[
                ['date', 'read data', 'written data']
            ][values.date.isin(datetimes)]
            cur_period_name = f"{cache_name} - from {period.split('-')[0]}"
            points = cur_period['read data'] / cur_period['written data']
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
                ratio_comp_swnp_fig.line(
                    cur_dates,
                    points,
                    legend=cur_period_name,
                    line_color=color_table[cache_name],
                    line_alpha=0.9,
                    line_width=2.,
                )
    ratio_comp_swnp_fig.legend.location = "top_left"
    ratio_comp_swnp_fig.legend.click_policy = "hide"
    ratio_comp_swnp_fig.xaxis.major_label_orientation = np.pi / 4.
    ratio_comp_swnp_fig.add_tools(SaveTool())
    add_window_lines(ratio_comp_swnp_fig, dates, window_size)
    run_next_period_figs.append(ratio_comp_swnp_fig)

    figs.append(column(
        row(*run_full_normal_figs),
        row(*run_single_window_figs),
        row(*run_next_period_figs),
    ))

    if html:
        save(column(*figs))
    if png:
        export_png(column(*figs), filename=os.path.join(
            folder, "results.png"))


def main():
    parser = argparse.ArgumentParser(
        "simulator", description="Simulation and result plotting")
    parser.add_argument('action', choices=['simulate', 'plot'],
                        default="simulate",
                        help='Action requested')
    parser.add_argument('source', type=str,
                        default="./results_8w_with_sizes_csv",
                        help='The folder where the json results are stored [DEFAULT: "./results_8w_with_sizes_csv"]')
    parser.add_argument('-FEB', '--force-exe-build', type=bool,
                        default=True,
                        help='Force to build the simulation executable [DEFAULT: True]')
    parser.add_argument('--gen-dataset', type=bool,
                        default=False,
                        help='Generate dataset during single simulation windows [DEFAULT: False]')
    parser.add_argument('-CS', '--cache-size', type=int,
                        default=10485760,
                        help='Size of the cache to simulate in Mega Bytes [DEFAULT: 10485760]')
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
    parser.add_argument('--out-html', type=bool,
                        default=True,
                        help='Plot the output as a html [DEFAULT: True]')
    parser.add_argument('--out-png', type=bool,
                        default=False,
                        help='Plot the output as a png (requires phantomjs-prebuilt installed with npm) [DEFAULT: False]')
    parser.add_argument('--plot-filters', type=str,
                        default="",
                        help='A comma separate string to search as filters')

    args, _ = parser.parse_known_args()

    if args.action == "simulate":
        simulator_exe = get_simulator_exe(force_creation=args.force_exe_build)

        base_dir = path.join(path.dirname(
            path.abspath(__file__)), "simulation_results")

        processes = []

        ##
        # Single Window runs
        single_window_run_dir = working_dir = path.join(
            base_dir,
            "run_single_window"
        )
        os.makedirs(single_window_run_dir, exist_ok=True)

        for window_idx in range(args.window_start, args.window_stop):
            for cache_type in CACHE_TYPES:
                working_dir = path.join(
                    single_window_run_dir,
                    f"{cache_type}_{int(args.cache_size/1024**2)}T_{args.region}",
                    f"window_{window_idx}",
                )
                os.makedirs(working_dir, exist_ok=True)
                exe_args = [
                        simulator_exe,
                        "simulate",
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
                if args.gen_dataset:
                    exe_args.append("--simGenDataset=true")
                    exe_args.append("--simGenDatasetName=dataset.csv.gz")

                print(" ".join(exe_args))
                cur_process = subprocess.Popen(
                    " ".join(exe_args),
                    shell=True,
                    cwd=working_dir,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                processes.append(cur_process)

        wait_jobs(processes)

        ##
        # Normal runs
        normal_run_dir = working_dir = path.join(
            base_dir,
            "run_full_normal"
        )
        os.makedirs(normal_run_dir, exist_ok=True)

        for cache_type in CACHE_TYPES:
            working_dir = path.join(
                normal_run_dir,
                f"{cache_type}_{int(args.cache_size/1024**2)}T_{args.region}"
            )
            os.makedirs(working_dir, exist_ok=True)
            cur_process = subprocess.Popen(
                " ".join([
                    simulator_exe,
                    "simulate",
                    cache_type,
                    path.abspath(args.source),
                    f"--size={args.cache_size}",
                    f"--simRegion={args.region}",
                    f"--simWindowSize={args.window_size}",
                    f"--simStartFromWindow={args.window_start}",
                    f"--simStopWindow={args.window_stop}",
                ]),
                shell=True,
                cwd=working_dir,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            processes.append(cur_process)

        ##
        # Next windows
        nexxt_window_run_dir = working_dir = path.join(
            base_dir,
            "run_next_window"
        )
        os.makedirs(nexxt_window_run_dir, exist_ok=True)

        for window_idx in range(args.window_start, args.window_stop):
            for cache_type in CACHE_TYPES:
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
                cur_process = subprocess.Popen(
                    " ".join([
                        simulator_exe,
                        "simulate",
                        cache_type,
                        path.abspath(args.source),
                        f"--size={args.cache_size}",
                        f"--simRegion={args.region}",
                        f"--simWindowSize={args.window_size}",
                        f"--simStartFromWindow={window_idx+1}",
                        f"--simStopWindow={window_idx+2}",
                        "--simLoadDump=true",
                        f"--simLoadDumpFileName={path.join(dump_dir, 'dump.json.gz')}",
                    ]),
                    shell=True,
                    cwd=working_dir,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                processes.append(cur_process)

        ##
        # Next Period
        next_period_run_dir = working_dir = path.join(
            base_dir,
            "run_next_period"
        )
        os.makedirs(next_period_run_dir, exist_ok=True)

        for window_idx in range(args.window_start, args.window_stop):
            for cache_type in CACHE_TYPES:
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
                cur_process = subprocess.Popen(
                    " ".join([
                        simulator_exe,
                        "simulate",
                        cache_type,
                        path.abspath(args.source),
                        f"--size={args.cache_size}",
                        f"--simRegion={args.region}",
                        f"--simWindowSize={args.window_size}",
                        f"--simStartFromWindow={window_idx+1}",
                        f"--simStopWindow={args.window_stop+1}",
                        "--simLoadDump=true",
                        f"--simLoadDumpFileName={path.join(dump_dir, 'dump.json.gz')}",
                    ]),
                    shell=True,
                    cwd=working_dir,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                processes.append(cur_process)

        wait_jobs(processes)

    elif args.action == "plot":
        # TODO: plot of results
        filters = [elm for elm in args.plot_filters.split(",") if elm]
        results = load_results(args.source)
        plot_results(
            args.source, results,
            window_size=args.window_size,
            filters=filters,
            html=args.out_html, png=args.out_png
        )


if __name__ == "__main__":
    main()
