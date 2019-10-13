import argparse
import gzip
import os
import subprocess
from itertools import cycle
from math import isnan
from multiprocessing import Pool
from os import path, walk
from random import randint, seed

import numpy as np
import pandas as pd
from bokeh.io import export_png
from bokeh.layouts import column, row
from bokeh.models import (BoxZoomTool, PanTool, ResetTool, SaveTool, Span,
                          WheelZoomTool)
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
                    f"[{process.pid}][DONE][{task_name}][Return code -> {process.returncode.read().decode('ascii')}]\n{process.stderr}\x1b[0K", flush=True)

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
    colors = cycle(Accent[8])
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


def plot_hit_rate(tools: list,
                  results: dict,
                  dates: list,
                  filters: list,
                  color_table: dict,
                  window_size: int,
                  x_range=None,
                  title: str = "Hit Rate",
                  run_type: str = "run_full_normal",
                  datetimes: list = [],
                  plot_width: int = 640,
                  plot_height: int = 480,
                  ) -> 'Figure':
    hit_rate_fig = figure(
        tools=tools,
        title=title,
        x_axis_label="Day",
        y_axis_label="Hit rate %",
        y_range=(0, 100),
        x_range=x_range if x_range else dates,
        plot_width=plot_width,
        plot_height=plot_height,
    )

    for cache_name, values in filter_results(
        results, run_type, filters
    ):
        if run_type == "run_full_normal":
            points = values['hit rate']
            hit_rate_fig.line(
                dates,
                points,
                legend=cache_name,
                color=color_table[cache_name],
                line_width=3.,
            )
        elif run_type == "run_single_window":
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
            points = single_windows.sort_values(by=['date'])['hit rate']
            hit_rate_fig.line(
                dates,
                points,
                legend=single_window_name,
                color=color_table[cache_name],
                line_width=3.,
            )
            next_windows = pd.concat(
                [
                    window
                    for name, window in sorted(
                        results['run_next_window'][cache_name].items(),
                        key=lambda elm: elm[0],
                    )
                ]
            ).sort_values(by=['date'])
            points = pd.concat(
                [
                    single_windows.loc[
                        ~single_windows.date.isin(next_windows.date),
                        ['date', 'hit rate']
                    ],
                    next_windows.loc[
                        next_windows.date.isin(single_windows.date),
                        ['date', 'hit rate']
                    ]
                ]
            ).sort_values(by=['date'])['hit rate']
            hit_rate_fig.line(
                dates,
                points,
                legend=next_window_name,
                line_color="red",
                line_alpha=0.9,
                line_width=3.,
                line_dash="dashed",
            )
        elif run_type == "run_next_period":
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
            points = single_windows.sort_values(by=['date'])['hit rate']
            hit_rate_fig.line(
                dates,
                points,
                legend=single_window_name,
                color=color_table[cache_name],
                line_width=3.,
            )
            line_styles = cycle([
                'solid',
                'dashed',
                'dotted',
                'dotdash',
                'dashdot',
            ])
            for period, period_values in values.items():
                cur_period = period_values[
                    ['date', 'hit rate']
                ][period_values.date.isin(datetimes)]
                cur_period_name = f"{cache_name} - from {period.split('-')[0]}"
                points = cur_period.sort_values(by=['date'])['hit rate']
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
                    hit_rate_fig.line(
                        cur_dates,
                        points,
                        legend=cur_period_name,
                        line_color="red",
                        line_alpha=0.9,
                        line_width=3.,
                        line_dash=cur_line_style,
                    )

    hit_rate_fig.legend.location = "top_left"
    hit_rate_fig.legend.click_policy = "hide"
    hit_rate_fig.xaxis.major_label_orientation = np.pi / 4.
    hit_rate_fig.add_tools(SaveTool())
    add_window_lines(hit_rate_fig, dates, window_size)

    return hit_rate_fig


def plot_read_on_write_data(tools: list,
                            results: dict,
                            dates: list,
                            filters: list,
                            color_table: dict,
                            window_size: int,
                            x_range=None,
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
        y_axis_label="Ratio",
        y_axis_type="log",
        x_range=x_range if x_range else dates,
        plot_width=plot_width,
        plot_height=plot_height,
    )

    for cache_name, values in filter_results(
        results, run_type, filters
    ):
        if run_type == "run_full_normal":
            points = values['read data'] / values['written data']
            read_on_write_data_fig.line(
                dates,
                points,
                legend=cache_name,
                color=color_table[cache_name],
                line_width=3.,
            )
        elif run_type == "run_single_window":
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
            points = single_windows['read data'] / \
                single_windows['written data']
            read_on_write_data_fig.line(
                dates,
                points,
                legend=single_window_name,
                color=color_table[cache_name],
                line_width=3.,
            )
            next_windows = pd.concat(
                [
                    window
                    for name, window in sorted(
                        results['run_next_window'][cache_name].items(),
                        key=lambda elm: elm[0],
                    )
                ]
            ).sort_values(by=['date'])
            tmp = pd.concat(
                [
                    single_windows.loc[
                        ~single_windows.date.isin(next_windows.date),
                        ['date', 'read data', 'written data']
                    ],
                    next_windows.loc[
                        next_windows.date.isin(single_windows.date),
                        ['date', 'read data', 'written data']
                    ]
                ]
            ).sort_values(by=['date'])[['read data', 'written data']]
            points = tmp['read data'] / tmp['written data']
            read_on_write_data_fig.line(
                dates,
                points,
                legend=next_window_name,
                line_color="red",
                line_alpha=0.9,
                line_width=3.,
                line_dash="dashed",
            )
        elif run_type == "run_next_period":
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
            points = points['read data'] / points['written data']
            read_on_write_data_fig.line(
                dates,
                points,
                legend=single_window_name,
                color=color_table[cache_name],
                line_width=3.,
            )
            line_styles = cycle([
                'solid',
                'dashed',
                'dotted',
                'dotdash',
                'dashdot',
            ])
            for period, period_values in values.items():
                cur_period = period_values[
                    ['date', 'read data', 'written data']
                ][period_values.date.isin(datetimes)].sort_values(by=['date'])
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
                    cur_line_style = next(line_styles)
                    read_on_write_data_fig.line(
                        cur_dates,
                        points,
                        legend=cur_period_name,
                        line_color="red",
                        line_alpha=0.9,
                        line_width=3.,
                        line_dash=cur_line_style,
                    )

    read_on_write_data_fig.legend.location = "top_left"
    read_on_write_data_fig.legend.click_policy = "hide"
    read_on_write_data_fig.xaxis.major_label_orientation = np.pi / 4.
    read_on_write_data_fig.add_tools(SaveTool())
    add_window_lines(read_on_write_data_fig, dates, window_size)

    return read_on_write_data_fig


def plot_results(folder: str, results: dict,
                 filters: list = [], window_size: int = 1,
                 html: bool = True, png: bool = False,
                 plot_width: int = 640,
                 plot_height: int = 480,
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

    pbar = tqdm(total=6, desc="Plot results", ascii=True)

    ###########################################################################
    # Hit Rate plot of full normal run
    ###########################################################################
    hit_rate_fig = plot_hit_rate(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        title="Hit Rate - Full Normal Run",
        plot_width=plot_width,
        plot_height=plot_height,
    )
    run_full_normal_figs.append(hit_rate_fig)
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
    )
    run_full_normal_figs.append(read_on_write_data_fig)
    pbar.update(1)

    ###########################################################################
    # Hit Rate compare single and next window plot
    ###########################################################################
    hit_rate_comp_snw_fig = plot_hit_rate(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        x_range=hit_rate_fig.x_range,
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
    )
    run_single_window_figs.append(ronwdata_comp_snw_fig)
    pbar.update(1)

    ###########################################################################
    # Hit Rate compare single window and next period plot
    ###########################################################################
    hit_rate_comp_swnp_fig = plot_hit_rate(
        tools,
        results,
        dates,
        filters,
        color_table,
        window_size,
        x_range=hit_rate_fig.x_range,
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
    )
    run_next_period_figs.append(ronwdata_comp_swnp_fig)
    pbar.update(1)

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

    pbar.close()


def valid_individual(individual, dataframe, cache_size: float) -> bool:
    return indivudual_size(individual, dataframe) <= cache_size


def indivudual_size(individual, dataframe) -> float:
    cur_series = pd.Series(individual, name='bools')
    cur_size = sum(dataframe[cur_series]['size'])
    return cur_size


def indivudual_fitness(individual, dataframe) -> float:
    cur_series = pd.Series(individual, name='bools')
    cur_size = sum(dataframe[cur_series]['value'])
    return cur_size


def make_it_valid(individual, dataframe, cache_size: float):
    nonzero = np.nonzero(individual)[0]
    cur_idx = len(nonzero) - 1
    while not valid_individual(individual, dataframe, cache_size):
        for _ in range(randint(1, cur_idx)):
            individual[nonzero[cur_idx]] = False
            cur_idx -= 1
    return individual


def get_one_solution(dataframe, cache_size: float):
    individual = np.random.randint(2, size=dataframe.shape[0], dtype=bool)
    individual = make_it_valid(individual, dataframe, cache_size)
    return individual


def get_best_configuration(dataframe, cache_size: float,
                           num_generation: int = 1000,
                           population_size=100):
    population = []
    for _ in tqdm(range(population_size), desc="Create Population",
                  total=population_size, ascii=True):
        population.append(get_one_solution(dataframe, cache_size))

    evolve_with_genetic_algorithm(
        population, dataframe, cache_size, num_generation
    )


def crossover(parent_a, parent_b) -> 'np.Array':
    """Perform and uniform corssover."""
    uniform_crossover = np.random.rand(len(parent_a))
    child = []
    for idx, cross in enumerate(uniform_crossover):
        if cross > 0.75:
            child.append(parent_b[idx])
        else:
            child.append(parent_a[idx])
    return np.array(child, dtype=bool)


def mutation(individual) -> 'np.Array':
    """Bit Flip mutation."""
    flip_bits = np.random.rand(len(individual))
    mutant = []
    for idx, flip in enumerate(flip_bits):
        if flip > 0.9:
            mutant.append(not individual[idx])
        else:
            mutant.append(individual[idx])
    return np.array(mutant, dtype=bool)


def generation(gen_input):
    best, individual, dataframe, cache_size = gen_input
    new_individual = crossover(best, individual)
    new_individual = mutation(new_individual)
    new_individual = make_it_valid(
        new_individual, dataframe, cache_size)
    new_fitness = indivudual_fitness(new_individual, dataframe)
    return (new_individual, new_fitness)


def evolve_with_genetic_algorithm(population, dataframe,
                                  cache_size: float,
                                  num_generation: int
                                  ):
    cur_population = population
    new_population = []
    pool = Pool()

    for _ in tqdm(
        range(num_generation),
        desc="Evolution", ascii=True,
        position=0
    ):
        cur_fitness = []
        for indivudual in population:
            cur_fitness.append(indivudual_fitness(indivudual, dataframe))

        idx_best = np.argmax(cur_fitness)
        best = cur_population[idx_best]
        mean = sum(cur_fitness) / len(cur_fitness)
        for cur_idx, (new_individual, new_fitness) in tqdm(
                enumerate(
                    pool.imap(
                        generation,
                        [
                            (best, individual, dataframe, cache_size)
                            for individual in cur_population
                        ]
                    )
                ),
                desc=f"Make new generation [Best: {cur_fitness[idx_best]:0.0f}][Mean: {mean:0.0f}]",
                ascii=True, position=1, leave=False,
                total=len(cur_population),
        ):
            new_population.append(cur_population[cur_idx])
        else:
            cur_population = [individual for individual in new_population]
            new_population = []

    idx_best = np.argmax(cur_fitness)
    return cur_population[idx_best]


def main():
    parser = argparse.ArgumentParser(
        "simulator", description="Simulation and result plotting")
    parser.add_argument('action', choices=['simulate', 'plot', 'train', 'create_dataset'],
                        default="simulate",
                        help='Action requested')
    parser.add_argument('source', type=str,
                        default="./results_8w_with_sizes_csv",
                        help='The folder where the json results are stored [DEFAULT: "./results_8w_with_sizes_csv"]')
    parser.add_argument('--cacheTypes', type=str,
                        default="lru,weightedLRU",
                        help='Comma separated list of cache to simulate [DEFAULT: "lru,weightedLRU"]')
    parser.add_argument('--out-folder', type=str,
                        default="simulation_results",
                        help='The folder where the simulation results will be stored [DEFAULT: "simulation_results"]')
    parser.add_argument('-FEB', '--force-exe-build', type=bool,
                        default=True,
                        help='Force to build the simulation executable [DEFAULT: True]')
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
    parser.add_argument('--only-CPU', type=bool,
                        default=True,
                        help='Force to use only CPU with TensorFlow [DEFAULT: True]')
    parser.add_argument('--plot-resolution', type=str,
                        default="640,480",
                        help='A comma separate string representing the target resolution of each plot [DEFAULT: 640,480]')

    args, _ = parser.parse_known_args()

    if args.only_CPU:
        os.environ['CUDA_VISIBLE_DEVICES'] = '-1'

    if args.action == "simulate":
        simulator_exe = get_simulator_exe(force_creation=args.force_exe_build)
        cache_types = args.cacheTypes.split(",")

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
                    model_path = path.join(
                        single_window_run_dir,
                        f"weightedLRU_{int(args.cache_size/1024**2)}T_{args.region}",
                        f"window_{window_idx}",
                    )
                    cur_model = DonkeyModel()
                    cur_model.load(
                        path.join(model_path, "donkey_model")
                    )
                    cur_model.add_feature_converter(
                        path.join(model_path, "featureConverter.dump.pickle")
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
        )
    elif args.action == "train":
        dataset = SimulatorDatasetReader(path.join(
            args.source, "run_single_window"
        ))
        dataset.modify_column(
            'cacheCapacity',
            lambda column: (column / 5.).astype(int)
        ).make_converter_for(
            [
                'cacheLastFileHit',
                'cacheCapacity',
                'class',
            ],
            unknown_value=False
        ).make_converter_for(
            [
                'siteName',
                # 'taskID',
                # 'jobID',
                'userID',
            ]
        ).make_data_and_labels(
            [
                'siteName',
                # 'taskID',
                # 'jobID',
                'userID',
                'cacheCapacity',
                'cacheLastFileHit',
                'fileSize',
                'fileTotRequests',
                'fileNHits',
                'fileNMiss',
                'fileMeanTimeReq',
            ],
            'class'
        ).save_data_and_labels()
        model = DonkeyModel()
        for target_dir, data, labels in dataset.get_data():
            model.train(data, labels)
            model.save(path.join(
                target_dir, "donkey_model"
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

        windows_requests = []

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
            cur_df = pd.concat(list_df, ignore_index=True)
            # print(cur_df.shape)
            windows_requests.append(cur_df)
            for cur_row in tqdm(cur_df.itertuples(), total=cur_df.shape[0],
                                desc=f"Parse window {idx} dataframe", ascii=True):
                cur_filename = cur_row.filename
                cur_size = cur_row.size
                if not isnan(cur_size):
                    if cur_filename not in files:
                        files[cur_filename] = {'size': cur_size, 'totReq': 0}
                    files[cur_filename]['totReq'] += 1
                    assert files[cur_filename]['size'] == cur_size, f"{files[cur_filename]['size']} != {cur_size}"
            files_df = pd.DataFrame(
                data={
                    'filename': [filename
                                 for filename in files],
                    'size': [files[filename]['size']
                             for filename in files],
                    'totReq': [files[filename]['totReq']
                               for filename in files],
                }
            )
            # Remove 1 request files
            files_df = files_df.drop(files_df[files_df.totReq == 1].index)
            # TO Megabytes
            files_df['size'] = files_df['size'] / 1024**2
            # Remove low value files
            files_df['value'] = files_df['size'] * files_df['totReq']
            q1 = files_df.value.describe().quantile(0.25)
            files_df = files_df.drop(files_df[files_df.value < q1].index)
            # Sort and reset indexes
            files_df = files_df.sort_values(by=['value'], ascending=False)
            files_df = files_df.reset_index(drop=True)
            # print(files_df)
            best_files = get_best_configuration(files_df, args.cache_size)

            files_df['class'] = best_files
            files_df.to_feather(path.join(
                base_dir,
                f"dataset_window_{idx:02d}.feather"
            ))


if __name__ == "__main__":
    main()
