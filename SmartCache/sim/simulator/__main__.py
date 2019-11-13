import argparse
import gzip
import os
import subprocess
from datetime import datetime, timedelta
from os import path, walk

import pandas as pd
from tqdm import tqdm
from yaspin import yaspin
from yaspin.spinners import Spinners

from DataManager.collector.dataset.reader import SimulatorDatasetReader
from SmartCache.ai.models.generator import DonkeyModel
from SmartCache.sim import get_simulator_exe

from .ga import compare_greedy_solution, get_best_configuration
from .plotter import plot_results
from .utils import load_results, wait_jobs


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
    parser.add_argument('--simulation-steps', type=str,
                        default='single,normal,nextW,nextP',
                        help='Select the simulation steps [DEFAULT: "single,normal,nextW,next"]')
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
                        default=2000,
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
    parser.add_argument('--insert-best-greedy', type=bool,
                        default=False,
                        help='Force to use insert 1 individual equal to the greedy composition [DEFAULT: False]')
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
        simulation_steps = args.simulation_steps.split(",")

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
        single_window_run_dir = path.join(
            base_dir,
            "run_single_window"
        )
        os.makedirs(single_window_run_dir, exist_ok=True)

        if 'single' in simulation_steps:
            for window_idx in range(args.window_start, args.window_stop):
                for cache_type in cache_types:
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
                        feature_map_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model),
                                "featureConverter.json.gzip"
                            )
                        )
                        model_weights_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model),
                                "modelWeightsDump.json.gzip"
                            )
                        )
                        exe_args.append("--aiHost=127.0.0.1")
                        exe_args.append(f"--aiPort=4242")
                        exe_args.append(f"--aiFeatureMap={feature_map_file}")
                        exe_args.append(f"--aiModel={model_weights_file}")
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
        normal_run_dir = path.join(
            base_dir,
            "run_full_normal"
        )
        os.makedirs(normal_run_dir, exist_ok=True)

        if 'normal' in simulation_steps:
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
                    feature_map_file = path.abspath(
                        path.join(
                            path.dirname(args.ai_model),
                            "featureConverter.json.gzip"
                        )
                    )
                    model_weights_file = path.abspath(
                        path.join(
                            path.dirname(args.ai_model),
                            "modelWeightsDump.json.gzip"
                        )
                    )
                    exe_args.append("--aiHost=127.0.0.1")
                    exe_args.append(f"--aiPort=4242")
                    exe_args.append(f"--aiFeatureMap={feature_map_file}")
                    exe_args.append(f"--aiModel={model_weights_file}")
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
        nexxt_window_run_dir = path.join(
            base_dir,
            "run_next_window"
        )
        os.makedirs(nexxt_window_run_dir, exist_ok=True)

        if 'nextW' in simulation_steps:
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
                        feature_map_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model),
                                "featureConverter.json.gzip"
                            )
                        )
                        model_weights_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model),
                                "modelWeightsDump.json.gzip"
                            )
                        )
                        exe_args.append("--aiHost=127.0.0.1")
                        exe_args.append(f"--aiPort=4242")
                        exe_args.append(f"--aiFeatureMap={feature_map_file}")
                        exe_args.append(f"--aiModel={model_weights_file}")
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
        next_period_run_dir = path.join(
            base_dir,
            "run_next_period"
        )
        os.makedirs(next_period_run_dir, exist_ok=True)

        if 'nextP' in simulation_steps:
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
                        feature_map_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model),
                                "featureConverter.json.gzip"
                            )
                        )
                        model_weights_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model),
                                "modelWeightsDump.json.gzip"
                            )
                        )
                        exe_args.append("--aiHost=127.0.0.1")
                        exe_args.append(f"--aiPort=4242")
                        exe_args.append(f"--aiFeatureMap={feature_map_file}")
                        exe_args.append(f"--aiModel={model_weights_file}")
                    cur_process = subprocess.Popen(
                        " ".join(exe_args),
                        shell=True,
                        cwd=working_dir,
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
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
        dataset = SimulatorDatasetReader().load_data_and_labels(args.source)
        model = DonkeyModel()
        data, labels = dataset.data
        # print(data.shape)
        model.train(data, labels)
        out_path = path.join(
            path.dirname(args.source), "donkey_model"
        )
        model.save(out_path).export_weights(out_path)

    elif args.action == "create_dataset":
        base_dir = path.join(
            path.dirname(path.abspath(args.source)), "datasets"
        )
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

        for winIdx, window in enumerate(windows):
            list_df = []
            files = {}
            for file_ in tqdm(window, desc=f"Create window {winIdx} dataframe",
                              ascii=True):
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

            stat_avg_time = []
            stat_num_req = []
            max_history = 64

            for cur_row in tqdm(cur_df.itertuples(), total=cur_df.shape[0],
                                desc=f"Parse window {winIdx} dataframe",
                                ascii=True):
                cur_filename = cur_row.filename
                cur_size = cur_row.size
                if cur_filename not in files:
                    data_type, _, _, file_type = cur_filename.split("/")[2:6]
                    files[cur_filename] = {
                        'size': cur_size,
                        'totReq': 0,
                        'days': [],
                        'reqHistory': [],
                        'lastReq': 0,
                        'fileType': file_type,
                        'dataType': data_type,
                        'maxStrike': 1
                    }
                cur_time = datetime.fromtimestamp(cur_row.day)
                cur_file_stats = files[cur_filename]
                cur_file_stats['totReq'] += 1
                cur_file_stats['lastReq'] = cur_time
                if len(cur_file_stats['reqHistory']) > max_history:
                    cur_file_stats['reqHistory'].pop()

                cur_file_stats['reqHistory'].append(cur_time)

                assert cur_file_stats['size'] == cur_size, f"{cur_file_stats['size']} != {cur_size}"

                if cur_row.day not in cur_file_stats['days']:
                    cur_file_stats['days'].append(cur_row.day)

                stat_num_req.append(cur_file_stats['totReq'])
                stat_avg_time.append(
                    sum([
                        (cur_file_stats['lastReq'] - elm).total_seconds() / 60.
                        for elm in cur_file_stats['reqHistory']
                    ]) / max_history
                )

            for file_, stats in tqdm(files.items(),
                                     desc=f"Parse file stats",
                                     ascii=True):
                strike = 1
                if len(stats['days']) > 1:
                    for dayIdx in range(len(stats['days'])-1):
                        cur_day = datetime.fromtimestamp(
                            stats['days'][dayIdx]
                        )
                        next_day = datetime.fromtimestamp(
                            stats['days'][dayIdx+1])
                        if next_day - cur_day == timedelta(days=1):
                            strike += 1
                        else:
                            strike = 1
                        if strike > stats['maxStrike']:
                            stats['maxStrike'] = strike

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
                    'maxStrike': [files[filename]['maxStrike']
                                  for filename in files],
                }
            )

            # Remove 1 request files
            # files_df = files_df.drop(files_df[files_df.totReq == 1].index)

            # TO Megabytes
            files_df['size'] = files_df['size'] / 1024**2

            # Add value
            files_df['value'] = ((files_df['size'] *
                                  files_df['totReq']
                                  ) / args.window_size) * files_df['maxStrike']

            # Remove low value files
            # q1 = files_df.value.describe().quantile(0.25)
            # files_df = files_df.drop(files_df[files_df.value < q1].index)

            # Sort and reset indexes
            files_df['greedyValue'] = files_df['value'] / files_df['size']
            files_df = files_df.sort_values(
                by=['greedyValue'], ascending=False)
            files_df = files_df.reset_index(drop=True)
            # print(files_df)

            # print(
            #   sum(files_df['size']), args.cache_size,
            #   sum(files_df['size'])/args.cache_size
            # )
            cache_size_factor = (sum(files_df['size'])/args.cache_size) / 2.

            best_files = get_best_configuration(
                files_df, args.cache_size*cache_size_factor,
                population_size=args.population_size,
                num_generations=args.num_generations,
                insert_best_greedy=args.insert_best_greedy,
            )

            files_df['class'] = best_files

            dataset_labels_out_file = path.join(
                base_dir,
                f"dataset_labels-window_{winIdx:02d}.feather.gz"
            )

            compare_greedy_solution(
                files_df, args.cache_size*cache_size_factor
            )

            dataset_data = []
            len_dataset = int(cur_df.shape[0] * 0.2)

            for cur_row in tqdm(
                cur_df.sample(n=len_dataset, random_state=42).itertuples(),
                total=len_dataset,
                desc=f"Create labeleled stage dataset {winIdx}",
                ascii=True
            ):
                filename = cur_row.filename
                cur_class = files_df.loc[
                    files_df.filename == filename, 'class'
                ].to_list().pop()
                dataset_data.append(
                    [
                        cur_row.site_name,
                        cur_row.user,
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
                        text=f"[Store labeleled stage dataset][{dataset_labels_out_file}]"
                        ):
                with gzip.GzipFile(dataset_labels_out_file, "wb") as out_file:
                    dataset_df.to_feather(out_file)

            # Prepare dataset
            print(f"[Prepare dataset][Using: '{dataset_labels_out_file}']")
            dataset = SimulatorDatasetReader(dataset_labels_out_file)
            dataset.modify_column(
                'size',
                lambda column: (column / 1024**2)
            ).make_converter_map(
                [
                    'class',
                ],
                map_type=bool,
                sort_values=True,
            ).make_converter_map(
                [
                    'size',
                ],
                map_type=int,
                sort_values=True,
                buckets=[50, 100, 500, 1000, 2000, 4000, '...'],
            ).make_converter_map(
                [
                    'avgTime',
                ],
                map_type=int,
                sort_values=True,
                buckets=list(range(0, 300*1000, 1000)) + ['...'],
            ).make_converter_map(
                [
                    'siteName',
                    'userID',
                    'fileType',
                    'dataType'
                ],
                unknown_values=True,
                map_type=str,
            ).store_converter_map(
                f"featureConverter-window_{winIdx:02d}"
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
            ).save_data_and_labels(
                f"dataset_converted-window_{winIdx:02d}"
            )
            print(
                f"[Dataset created][Name: 'dataset_converted-window_{winIdx:02d}']"
            )


if __name__ == "__main__":
    main()
