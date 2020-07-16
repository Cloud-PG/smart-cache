import logging
import select
import subprocess
from contextlib import contextmanager
from os import path, walk
from pathlib import Path

import coloredlogs
import pandas as pd
from tqdm import tqdm


def get_logger(filename: str = __name__, level: str = 'INFO') -> 'logger.Logger':
    # Get the top-level logger object
    logger = logging.getLogger(filename)
    # make it print to the console.
    console = logging.StreamHandler()
    format_str = '%(asctime)s\t%(levelname)s -- %(processName)s %(filename)s:%(lineno)s -- %(message)s'
    console.setFormatter(logging.Formatter(format_str))

    logger.addHandler(console)
    coloredlogs.install(level=level, logger=logger)

    return logger


@contextmanager
def ignored(*exceptions):
    try:
        yield
    except KeyError as exc:
        if exc.args[0] not in ["run_single_window", "run_next_period"]:
            raise


def str2bool(v):
    return v.lower() in ("yes", "true", "True", "t", "1")


def wait_jobs(processes):
    while job_run(processes):
        for _, process in processes:
            try:
                process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass


def read_output_last_line(stdout, stderr):
    buffer = ""
    read_list, _, _ = select.select([stdout, stderr], [], [], 600.0)
    for descriptor in read_list:
        cur_char = descriptor.read(1).decode("utf-8")
        while cur_char not in ["\r", "\n", '']:
            buffer += cur_char
            cur_char = descriptor.read(1).decode("utf-8")
    return buffer


def job_run(processes: list) -> bool:
    running_processes = []
    for task_name, process in processes:
        try:
            process.wait(timeout=0.1)
        except subprocess.TimeoutExpired:
            pass
        running = process.poll() is None
        if running:
            current_output = read_output_last_line(process.stdout, process.stderr)
            if current_output:
                cur_output_string = f"[{process.pid}][RUNNING][{task_name}]{current_output}"
                print(
                    cur_output_string,
                    flush=True
                )
                running_processes.append(running)
        else:
            print(
                f"[{process.pid}][DONE][{task_name}][Return code -> {process.returncode}]", flush=True)
            if process.returncode != 0:
                print(
                    f"[{process.pid}][DONE][{task_name}][Return code -> {process.returncode}]", flush=True)
                print(f"{process.stdout.read().decode('utf-8')}", flush=True)
                exit(process.returncode)

    return len(running_processes) > 0


def get_cache_size(cache_name):
    if cache_name.find("T_") != -1:
        cache_size = float(cache_name.split("T_")
                           [0].rsplit("_", 1)[-1])
        return float(cache_size * 1024**2)
    elif cache_name.find("G_") != -1:
        cache_size = float(cache_name.split("G_")
                           [0].rsplit("_", 1)[-1])
        return float(cache_size * 1024)
    elif cache_name.find("M_") != -1:
        cache_size = float(cache_name.split("M_")
                           [0].rsplit("_", 1)[-1])
        return float(cache_size * 1024)
    else:
        raise Exception(
            f"Error: '{cache_name}' cache name with unspecified size...")


def load_results(folder: str, top: int = 0, top_table_output: bool = False,
                 group_by: str = "family", table_type: str = "leaderboard",
                 bandwidth: float = 10.
                 ) -> dict:
    results = {}
    res_len = -1
    for root, _, files in tqdm(walk(folder), desc="Search and open files"):
        for file_ in files:
            cur_path = Path(path.join(root, file_))
            if cur_path.suffix == ".csv" and \
                cur_path.name.find("_results") != -1 and \
                    cur_path.name.find("top_") == -1:
                section = cur_path.parent.parent.name
                target = cur_path.parent.name
                file_path = path.join(cur_path.resolve().as_posix())
                df = pd.read_csv(
                    file_path
                )
                if res_len == -1:
                    res_len = len(df.index)
                if len(df.index) != res_len:
                    print(
                        f"Warning: '{path.abspath(file_)}' has a different number of results ad will not be counted..."
                    )
                else:
                    if section not in results:
                        results[section] = {}
                    results[section][target] = df

    if top != 0:
        if 'run_full_normal' in results:
            leaderboard = []
            for cache_name, df in tqdm(results['run_full_normal'].items(), desc="Create stats for top results"):
                cache_size = get_cache_size(cache_name)
                cache_cost = (
                    (df['written data'] + df['deleted data']) / cache_size) * 100.
                # Old cost
                # cache_cost = df['written data'] + \
                #     df['deleted data'] + df['read on miss data']
                throughput = (df['read on hit data'] /
                              df['written data']) * 100.
                # Old Throughput
                # throughput = df['read on hit data'] / df['written data']
                read_on_hit_ratio = (df['read on hit data'] /
                                     df['read data']) * 100.
                # read_on_hit_ratio = df['read on hit data']
                values = [
                    cache_name,
                    throughput.mean(),
                    cache_cost.mean(),
                    read_on_hit_ratio.mean(),
                ]

                if table_type == "leaderboard":
                    # New Throughput
                    values[1] = (
                        ((
                            df['read on hit data'] - df['written data']
                        ) / df['read data'])*100.
                    ).mean()
                    # Bandwidth
                    values.append(
                        ((df['read on miss data'] / ((1000. / 8.) * 60. * 60. * 24. * bandwidth)) * 100.).mean())
                    # AvgFreeSpace
                    values.append(
                        (df['avg free space'].mean() / cache_size) * 100.,
                    )
                    # StdDevFreeSpace
                    values.append(
                        (df['std dev free space'].mean() / cache_size) * 100.,
                    )
                    # Hit Rate
                    values.append(df['hit rate'].mean())
                    leaderboard.append(values)
                elif table_type == "old_leaderboard":
                    values.append(
                        ((df['read on miss data'] / ((1000. / 8.) * 60. * 60. * 24. * bandwidth)) * 100.).mean())
                    values.append(df['CPU efficiency'].mean())
                    values.append(df['hit rate'].mean())
                    leaderboard.append(values)
                elif table_type == "weight":
                    if cache_name.find("weigh") != -1 and cache_name.index("weigh") == 0:
                        cache_type, size, region, family, alpha, beta, gamma = cache_name.split(
                            "_")
                        values.append(family)
                        values.append(alpha)
                        values.append(beta)
                        values.append(gamma)
                    else:
                        for _ in range(3):
                            values.append(0)
                        values.append("")

                    leaderboard.append(values)

            if table_type == "leaderboard":
                top_df = pd.DataFrame(
                    leaderboard,
                    columns=[
                        "cacheName", "throughput", "cacheCost",
                        "readOnHitRatio", "bandSaturation",
                        "avgFreeSpace", "stdDevFreeSpace",
                        "hitRate",
                    ]
                )
                top_df = top_df.sort_values(
                    by=["throughput", "cacheCost", "readOnHitRatio", "hitRate"],
                    ascending=[False, False, True, False]
                )
            elif table_type == "old_leaderboard":
                top_df = pd.DataFrame(
                    leaderboard,
                    columns=[
                        "cacheName", "throughput", "cacheCost",
                        "readOnHitRatio", "bandSaturation", "cpuEff", "hitRate"
                    ]
                )
                top_df = top_df.sort_values(
                    by=["throughput", "cacheCost", "readOnHitRatio", "cpuEff"],
                    ascending=[False, False, True, False]
                )
            elif table_type == "weight":
                top_df = pd.DataFrame(
                    leaderboard,
                    columns=[
                        "cacheName", "throughput", "cacheCost",
                        "readOnHitRatio",
                        "family", "alpha", "beta", "gamma"
                    ]
                )
                top_df = top_df.sort_values(
                    by=["throughput", "cacheCost", "readOnHitRatio"],
                    ascending=[False, True, False]
                )

            topResults = top_df.cacheName.head(top).values.tolist()
            if len(topResults) < len(top_df.index):
                to_delete = []
                for cache_name, _ in tqdm(results['run_full_normal'].items(), desc="Filter top 10 results"):
                    if cache_name.lower().find("lru_") == -1 or cache_name.lower().index("lru_") != 0:
                        if cache_name not in topResults:
                            to_delete.append(cache_name)
                for cache_name in tqdm(to_delete, desc="Remove lower results"):
                    del results['run_full_normal'][cache_name]

            if top_table_output:
                csv_dest_folder = Path(folder)
                if group_by == "family":
                    cur_output = top_df.groupby("family").head(top).sort_values(
                        by=["family", "throughput",
                            "cacheCost", "readOnHitRatio"],
                        ascending=[True, False, True, False]
                    )
                    cur_output.to_csv(
                        csv_dest_folder.joinpath(f"top_{top}_results.csv"), 
                        index=False, float_format='%.2f'
                    )
                    cur_output.to_latex(
                        csv_dest_folder.joinpath(f"top_{top}_results.tex"), 
                        index=False, float_format='%.2f'
                    )
                else:
                    sort_by = ["throughput", "cacheCost",
                               "readOnHitRatio", ]
                    if table_type == "old_leaderboard":
                        sort_by.append("cpuEff")
                    elif table_type == "leaderboard":
                        sort_by.insert(0, "hitRate")
                    cur_output = top_df.head(top).sort_values(
                        by=sort_by,
                        ascending=[False, True, False, False]
                    )
                    cur_output.to_csv(
                        csv_dest_folder.joinpath(f"top_{top}_results.csv"), 
                        index=False, float_format='%.2f'
                    )
                    cur_output.to_latex(
                        csv_dest_folder.joinpath(f"top_{top}_results.tex"), 
                        index=False, float_format='%.2f'
                    )
                    cur_output.to_html(
                        csv_dest_folder.joinpath(f"top_{top}_results.html"), 
                        index=False, float_format='%.2f'
                    )

    return results
