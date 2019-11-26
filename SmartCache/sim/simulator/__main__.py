import argparse
import gzip
import json
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
from .greedy import get2PTAS
from .plotter import plot_results
from .utils import load_results, str2bool, wait_jobs


def main():
    parser = argparse.ArgumentParser(
        "simulator", description="Simulation and result plotting")

    parser.register('type', 'bool', str2bool)  # add type keyword to registries

    parser.add_argument('action', choices=['simulate', 'testAI', 'testDataset', 'plot', 'train', 'create_dataset'],
                        default="simulate",
                        help='Action requested')
    parser.add_argument('source', type=str,
                        default="./results_8w_with_sizes_csv",
                        help='The folder where the json results are stored [DEFAULT: "./results_8w_with_sizes_csv"]')
    parser.add_argument('--cache-types', type=str,
                        default="lru,weightedLRU",
                        help='Comma separated list of cache to simulate [DEFAULT: "lru,weightedLRU"]')
    parser.add_argument('--out-folder', type=str,
                        default="./simulation_results",
                        help='The folder where the simulation results will be stored [DEFAULT: "simulation_results"]')
    parser.add_argument('--read-on-hit', type='bool',
                        default=True,
                        help='Use read on hit data [DEFAULT: True]')
    parser.add_argument('--simulation-steps', type=str,
                        default='single,normal,nextW,nextP',
                        help='Select the simulation steps [DEFAULT: "single,normal,nextW,next"]')
    parser.add_argument('-FEB', '--force-exe-build', type='bool',
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
    parser.add_argument('--out-html', type='bool',
                        default=True,
                        help='Plot the output as a html [DEFAULT: True]')
    parser.add_argument('--out-png', type='bool',
                        default=False,
                        help='Plot the output as a png (requires phantomjs-prebuilt installed with npm) [DEFAULT: False]')
    parser.add_argument('--plot-filters', type=str,
                        default="",
                        help='A comma separate string to search as filters')
    parser.add_argument('--only-CPU', type='bool',
                        default=True,
                        help='Force to use only CPU with TensorFlow [DEFAULT: True]')
    parser.add_argument('--insert-best-greedy', type='bool',
                        default=False,
                        help='Force to use insert 1 individual equal to the greedy composition [DEFAULT: False]')
    parser.add_argument('--dataset-creation-method', type=str,
                        choices=['greedy', 'ga'], default="greedy",
                        help='The method used to create the dataset [DEFAULT: "greedy"]')
    parser.add_argument('--dataset-folder', type=str,
                        default="./datasets",
                        help='Folder where datasets are stored [DEFAULT: "./datasets"]')
    parser.add_argument('--dataset-prefix', type=str,
                        default="dataset_best_solution",
                        help='The dataset file name prefix [DEFAULT: "dataset_best_solution"]')
    parser.add_argument('--plot-resolution', type=str,
                        default="800,600",
                        help='A comma separate string representing the target resolution of each plot [DEFAULT: 640,480]')
    parser.add_argument('--ai-model-basename', type=str,
                        default="./models/donkey_model",
                        help='Ai Model basename and path [DEFAULT: "./models/donkey_model"]')
    parser.add_argument('--feature-prefix', type=str,
                        default="featureConverter",
                        help='Ai Model feature converter name prefix [DEFAULT: "featureConverter"]')

    args, _ = parser.parse_known_args()

    if args.only_CPU:
        os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
    else:
        # Make visible only first device
        os.environ['CUDA_VISIBLE_DEVICES'] = '0'

    if args.action in ["simulate", "testAI", "testDataset"]:
        if not os.path.exists(args.source):
            print(f"Path '{args.source}' does not exist!")
            exit(-1)

        simulator_exe = get_simulator_exe(force_creation=args.force_exe_build)
        cache_types = args.cache_types.split(",")
        simulation_steps = args.simulation_steps.split(",")

        base_dir = path.abspath(path.join(os.getcwd(), args.out_folder))
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
                    # Create base command
                    exe_args = [
                        simulator_exe,
                        args.action,
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
                    # Add custom cache parameters
                    if cache_type == 'aiLRU':
                        feature_map_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model_basename),
                                f"{args.feature_prefix}-window_{window_idx:02d}.json.gz"
                            )
                        )
                        model_weights_file = path.abspath(
                            f"{args.ai_model_basename.split('.h5')[0]}-window_{window_idx:02d}.dump.json.gz"
                        )
                        exe_args.append("--aiHost=127.0.0.1")
                        exe_args.append(f"--aiPort=4242")
                        exe_args.append(f"--aiFeatureMap={feature_map_file}")
                        exe_args.append(f"--aiModel={model_weights_file}")
                    elif cache_type == 'lruDatasetVerifier':
                        dataset_file = path.abspath(
                            path.join(
                                args.dataset_folder,
                                f"{args.dataset_prefix}-window_{window_idx:02d}.json.gz"
                            )
                        )
                        exe_args.append(f"--dataset2TestPath={dataset_file}")
                    # Create the task
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
                # Create base command
                exe_args = [
                    simulator_exe,
                    args.action,
                    cache_type,
                    path.abspath(args.source),
                    f"--size={args.cache_size}",
                    f"--simRegion={args.region}",
                    f"--simWindowSize={args.window_size}",
                    f"--simStartFromWindow={args.window_start}",
                    f"--simStopWindow={args.window_stop}",
                ]
                # Add custom cache parameters
                if cache_type == 'aiLRU':
                    feature_map_file = path.abspath(
                        path.join(
                            path.dirname(args.ai_model_basename),
                            f"{args.feature_prefix}-window_00.json.gz"
                        )
                    )
                    model_weights_file = path.abspath(
                        f"{args.ai_model_basename.split('.h5')[0]}-window_00.dump.json.gz"
                    )
                    exe_args.append("--aiHost=127.0.0.1")
                    exe_args.append(f"--aiPort=4242")
                    exe_args.append(f"--aiFeatureMap={feature_map_file}")
                    exe_args.append(f"--aiModel={model_weights_file}")
                elif cache_type == 'lruDatasetVerifier':
                    dataset_file = path.abspath(
                        path.join(
                            args.dataset_folder,
                            f"{args.dataset_prefix}-window_00.json.gz"
                        )
                    )
                    exe_args.append(f"--dataset2TestPath={dataset_file}")
                cur_process = subprocess.Popen(
                    " ".join(exe_args),
                    shell=True,
                    cwd=working_dir,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                processes.append(("Full Run", cur_process))
                # Add custom cache parameters
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
                    # Create base command
                    exe_args = [
                        simulator_exe,
                        args.action,
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
                    # Add custom cache parameters
                    if cache_type == 'aiLRU':
                        feature_map_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model_basename),
                                f"{args.feature_prefix}-window_{window_idx:02d}.json.gz"
                            )
                        )
                        model_weights_file = path.abspath(
                            f"{args.ai_model_basename.split('.h5')[0]}-window_{window_idx:02d}.dump.json.gz"
                        )
                        exe_args.append("--aiHost=127.0.0.1")
                        exe_args.append(f"--aiPort=4242")
                        exe_args.append(f"--aiFeatureMap={feature_map_file}")
                        exe_args.append(f"--aiModel={model_weights_file}")
                    elif cache_type == 'lruDatasetVerifier':
                        dataset_file = path.abspath(
                            path.join(
                                args.dataset_folder,
                                f"{args.dataset_prefix}-window_{window_idx:02d}.json.gz"
                            )
                        )
                        exe_args.append(f"--dataset2TestPath={dataset_file}")
                    # Create the task
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
                    # Create base command
                    exe_args = [
                        simulator_exe,
                        args.action,
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
                    # Add custom cache parameters
                    if cache_type == 'aiLRU':
                        feature_map_file = path.abspath(
                            path.join(
                                path.dirname(args.ai_model_basename),
                                f"{args.feature_prefix}-window_{window_idx:02d}.json.gz"
                            )
                        )
                        model_weights_file = path.abspath(
                            f"{args.ai_model_basename.split('.h5')[0]}-window_{window_idx:02d}.dump.json.gz"
                        )
                        exe_args.append("--aiHost=127.0.0.1")
                        exe_args.append(f"--aiPort=4242")
                        exe_args.append(f"--aiFeatureMap={feature_map_file}")
                        exe_args.append(f"--aiModel={model_weights_file}")
                    elif cache_type == 'lruDatasetVerifier':
                        dataset_file = path.abspath(
                            path.join(
                                args.dataset_folder,
                                f"{args.dataset_prefix}-window_{window_idx:02d}.json.gz"
                            )
                        )
                        exe_args.append(f"--dataset2TestPath={dataset_file}")
                    # Create the task
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
        datasets = []
        for root, dirs, files in walk(args.source):
            for file_ in files:
                head, tail = path.splitext(file_)
                if tail == ".npz":
                    datasets.append(
                        path.join(root, file_)
                    )

        for dataset_file in datasets:
            print(f"[Start training][Dataset: {dataset_file}]")
            dataset = SimulatorDatasetReader(
            ).load_data_and_labels(dataset_file)
            window_num = dataset_file.split("-window_")[1].split(".")[0]
            model = DonkeyModel()
            data, labels = dataset.data
            # print(data.shape)
            model.train(data, labels)
            out_path = path.join(
                path.dirname(dataset_file), f"donkey_model-window_{window_num}"
            )
            model.save(out_path).export_weights(out_path)
            print(f"[Model saved][Output: {out_path}...]")

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
            if winIdx == args.window_stop:
                break

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
                    data_type, campain, process, file_type = cur_filename.split("/")[2:6]
                    files[cur_filename] = {
                        'size': cur_size,
                        'totReq': 0,
                        'days': [],
                        'campain': campain,
                        'process': process,
                        'reqHistory': [],
                        'lastReq': 0,
                        'fileType': file_type,
                        'dataType': data_type,
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
                    'fileType': [files[filename]['fileType']
                                 for filename in files],
                    'dataType': [files[filename]['dataType']
                                 for filename in files],
                    'campain': [files[filename]['campain']
                               for filename in files],
                    'process': [files[filename]['process']
                               for filename in files],
                }
            )

            # Remove 1 request files
            # files_df = files_df.drop(files_df[files_df.totReq == 1].index)

            # TO Megabytes
            files_df['size'] = files_df['size'] / 1024**2

            # Add value
            files_df['value'] = (
                files_df['size'] *
                files_df['totReq']
            ) / args.window_size

            # Remove low value files
            # q1 = files_df.value.describe().quantile(0.25)
            # files_df = files_df.drop(files_df[files_df.value < q1].index)

            # Sort and reset indexes
            # Note: greedyValue is prepared for 2 PTAS algorithm
            files_df['greedyValue'] = files_df['value'] / files_df['size']
            files_df = files_df.sort_values(
                by=['greedyValue'], ascending=False)
            files_df = files_df.reset_index(drop=True)
            # print(files_df)

            # print(
            #   sum(files_df['size']), args.cache_size,
            #   sum(files_df['size'])/args.cache_size
            # )

            greedy_solution = get2PTAS(
                files_df, args.cache_size
            )

            if args.dataset_creation_method == "ga":
                best_selection = get_best_configuration(
                    files_df, args.cache_size,
                    population_size=args.population_size,
                    num_generations=args.num_generations,
                    insert_best_greedy=args.insert_best_greedy,
                )
                compare_greedy_solution(
                    files_df, args.cache_size, greedy_solution,
                )
            else:
                best_selection = greedy_solution
                gr_size = sum(files_df[best_selection]['size'].to_list())
                gr_score = sum(files_df[best_selection]['value'].to_list())
                print("---[Results]---")
                print(
                    f"[Size: \t{gr_size:0.2f}][Score: \t{gr_score:0.2f}][Greedy]")

            files_df['class'] = best_selection

            dataset_labels_out_file = path.join(
                base_dir,
                f"dataset_labels-window_{winIdx:02d}.feather.gz"
            )

            dataset_best_solution_out_file = path.join(
                base_dir,
                f"dataset_best_solution-window_{winIdx:02d}.json.gz"
            )

            # get 42% of the requests
            len_dataset = int(cur_df.shape[0] * 0.30)

            sample = cur_df.sample(n=len_dataset, random_state=42)
            sample.rename(columns={'size': 'fileSize'}, inplace=True)

            dataset_df = pd.merge(sample, files_df, on='filename')
            dataset_df = dataset_df[
                ['site_name', 'user', 'num_req', 'avg_time',
                 'size', 'fileType', 'dataType', 
                 'campain', 'process', 'class']
            ]
            dataset_df.rename(
                columns={
                    'site_name': "siteName",
                    'user': "userID",
                    'num_req': "numReq",
                    'avg_time': "avgTime",
                },
                inplace=True
            )

            with yaspin(
                Spinners.bouncingBall,
                text=f"[Store labeleled stage dataset][{dataset_labels_out_file}]"
            ):
                with gzip.GzipFile(dataset_labels_out_file, "wb") as out_file:
                    dataset_df.to_feather(out_file)

            with yaspin(
                Spinners.bouncingBall,
                text=f"[Store best stolution][{dataset_best_solution_out_file}]"
            ):
                with gzip.GzipFile(dataset_best_solution_out_file, "wb") as out_file:
                    out_file.write(
                        json.dumps({
                            'selected_files': files_df[
                                files_df['class'] == True
                            ]['filename'].to_list()
                        }).encode("utf-8")
                    )

            # Get some stats
            # print(dataset_df.describe())

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
                sort_keys=True,
            ).make_converter_map(
                [
                    'size',
                ],
                map_type=int,
                sort_keys=True,
                buckets=[50, 100, 250, 500, 1000, 2000, 4000, 10000, '...'],
            ).make_converter_map(
                [
                    'numReq',
                ],
                map_type=int,
                sort_keys=True,
                buckets=[1, 2, 3, 4, 5, 10, 25, 50, 75, 100, 200, '...'],
            ).make_converter_map(
                [
                    'avgTime',
                ],
                map_type=int,
                sort_keys=True,
                buckets=list(range(0, 6*1000, 100)) + ['...'],
            ).make_converter_map(
                [
                    'siteName',
                    'userID',
                    'fileType',
                    'dataType',
                    'campain',
                    'process',
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
                    'campain',
                    'process',
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
