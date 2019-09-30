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
from bokeh.palettes import Category20c
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
    colors = cycle(Category20c[20])
    for name in sorted(names):
        cur_color = next(colors)
        color_table[name] = cur_color
        single_w_name = f'{name} - single window'
        cur_color = next(colors)
        color_table[single_w_name] = cur_color
        single_w_name = f'{name} - next window'
        cur_color = next(colors)
        color_table[single_w_name] = cur_color
        single_w_name = f'{name} - next period'
        cur_color = next(colors)
        color_table[single_w_name] = cur_color


def add_window_lines(cur_fig, dates: list, window_size: int):
    cur_fig.renderers.extend([
        Span(
            location=idx+0.5, dimension='height',
            line_color='black', line_width=0.9
        )
        for idx in range(0, len(dates), window_size)
    ])


def plot_results(folder: str, results: dict,
                 html: bool = True, png: bool = False,
                 window_size: int = 1):
    color_table = {}
    dates = []

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
    for cache_name in results['run_full_normal']:
        update_colors(cache_name, color_table)

    # Get dates
    for cache_name, values in results['run_full_normal'].items():
        if not dates:
            dates = [
                elm.split(" ")[0]
                for elm
                in values['date'].astype(str)
            ]
            break

    figs = []
    run_full_normal_figs = []
    run_single_window_figs = []

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

    for cache_name, values in results['run_full_normal'].items():
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

    for cache_name, values in results['run_full_normal'].items():
        points = values['read on hit'] / values['written data']
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
    # Hit Rate compare plot
    hit_rate_compare_fig = figure(
        tools=tools,
        title="Hit Rate - Compare single and next window",
        x_axis_label="Day",
        y_axis_label="Hit rate %",
        y_range=(0, 100),
        x_range=hit_rate_fig.x_range,
        plot_width=640,
        plot_height=480,
    )

    for cache_name, windows in results['run_single_window'].items():
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
        hit_rate_compare_fig.line(
            dates,
            points,
            legend=single_window_name,
            color=color_table[single_window_name],
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
        hit_rate_compare_fig.line(
            dates,
            points,
            legend=next_window_name,
            color=color_table[next_window_name],
            line_width=2.,
        )
    hit_rate_compare_fig.legend.location = "top_left"
    hit_rate_compare_fig.legend.click_policy = "hide"
    hit_rate_compare_fig.xaxis.major_label_orientation = np.pi / 4.
    hit_rate_compare_fig.add_tools(SaveTool())
    add_window_lines(hit_rate_compare_fig, dates, window_size)
    run_single_window_figs.append(hit_rate_compare_fig)

    ##
    # Ratio compare plot
    ratio_compare_fig = figure(
        tools=tools,
        title="Ratio - Compare single and next window",
        x_axis_label="Day",
        y_axis_label="Ratio",
        x_range=hit_rate_fig.x_range,
        plot_width=640,
        plot_height=480,
    )

    for cache_name, windows in results['run_single_window'].items():
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
        points = single_windows['read on hit'] / single_windows['written data']
        ratio_compare_fig.line(
            dates,
            points,
            legend=single_window_name,
            color=color_table[single_window_name],
            line_width=2.,
        )
        # Merge next windows
        next_values = pd.merge(
            single_windows[['date', 'read on hit', 'written data']],
            next_windows[['date', 'read on hit', 'written data']],
            on=['date'],
            how='inner'
        ).sort_values(by=['date']).rename(
            columns={
                'read on hit_y': 'read on hit',
                'written data_y': 'written data'
            }
        )[['date', 'read on hit', 'written data']]
        first_part = single_windows[
            ['date', 'read on hit', 'written data']
        ][~single_windows.date.isin(next_values.date)]
        next_windows = pd.concat([first_part, next_values])
        points = next_windows['read on hit'] / next_windows['written data']
        ratio_compare_fig.line(
            dates,
            points,
            legend=next_window_name,
            color=color_table[next_window_name],
            line_width=2.,
        )
    ratio_compare_fig.legend.location = "top_left"
    ratio_compare_fig.legend.click_policy = "hide"
    ratio_compare_fig.xaxis.major_label_orientation = np.pi / 4.
    ratio_compare_fig.add_tools(SaveTool())
    add_window_lines(ratio_compare_fig, dates, window_size)
    run_single_window_figs.append(ratio_compare_fig)

    figs.append(column(
        row(*run_full_normal_figs),
        row(*run_single_window_figs)
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
    parser.add_argument('-CS', '--cache-size', type=int,
                        default=10485760,
                        help='Size of the cache to simulate [DEFAULT: 10485760]')
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
                cur_process = subprocess.Popen(
                    " ".join([
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
                    ]),
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
        results = load_results(args.source)
        plot_results(
            args.source, results,
            window_size=args.window_size,
            html=args.out_html, png=args.out_png
        )


if __name__ == "__main__":
    main()
