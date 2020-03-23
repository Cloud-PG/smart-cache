import logging
import select
import subprocess
from contextlib import contextmanager
from os import path, walk

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


def read_output_last_line(output):
    buffer = ""
    read_list, _, _ = select.select([output], [], [], 600.0)
    if output in read_list:
        cur_char = output.read(1).decode("ascii")
        while cur_char not in ["\r", "\n", '']:
            buffer += cur_char
            cur_char = output.read(1).decode("ascii")
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
            current_output = read_output_last_line(process.stdout)
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
                print(f"{process.stdout.read().decode('ascii')}", flush=True)
                exit(process.returncode)

    return len(running_processes) > 0


def get_result_section(cur_path: str, source_folder: str):
    section = []
    target = path.dirname(source_folder) or path.basename(source_folder)
    head = cur_path
    while head != target:
        head, tail = path.split(head)
        section.append(tail)
    return section


def load_results(folder: str, top: int = 0, top_table_output: bool = False,
                 group_by: str = "family", table_type: str = "leaderboard"
                 ) -> dict:
    results = {}
    res_len = -1
    for root, _, files in tqdm(walk(folder), desc="Search and open files"):
        for file_ in files:
            head, ext = path.splitext(file_)
            if ext == ".csv" and head.find("_results") != -1:
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
                if res_len == -1:
                    res_len = len(df.index)
                assert len(
                    df.index) == res_len, f"Error: '{file_}' has a different number of results..."
                cur_section[last_section] = df

    if top != 0:
        if 'run_full_normal' in results:
            leaderboard = []
            for cache_name, df in tqdm(results['run_full_normal'].items(), desc="Create stats for top results"):
                cost = df['written data'] + \
                    df['deleted data'] + df['read on miss data']
                throughput = df['read on hit data'] / df['written data']
                values = [
                    cache_name,
                    throughput.mean(),
                    int(cost.mean()),
                    int(df['read on hit data'].mean()),
                ]

                if table_type == "leaderboard":
                    values.insert(
                        1, ((df['read on miss data'] / ((10000. / 8.) * 60. * 60. * 24.)) * 100.).mean())
                    values.insert(1, df['hit rate'].mean())
                    values.insert(1, df['CPU efficiency'].mean())
                    leaderboard.append(values)
                elif table_type == "weight":
                    if cache_name.find("weigh") != -1 and cache_name.index("weigh") == 0:
                        cache_type, size, region, family, alpha, beta, gamma = cache_name.split(
                            "_")
                        values.insert(1, gamma)
                        values.insert(1, beta)
                        values.insert(1, alpha)
                        values.insert(1, family)
                    else:
                        for _ in range(3):
                            values.insert(1, 0)
                        values.insert(1, "")

                    leaderboard.append(values)

            if table_type == "leaderboard":
                top_df = pd.DataFrame(
                    leaderboard,
                    columns=[
                        "cacheName", "cpuEff", "hitRate", "network",
                        "throughput", "cost", "readOnHit"
                    ]
                )
                top_df = top_df.sort_values(
                    by=["cpuEff", "throughput", "cost", "readOnHit"],
                    ascending=[False, False, True, False]
                )
            elif table_type == "weight":
                top_df = pd.DataFrame(
                    leaderboard,
                    columns=[
                        "cacheName", "family", "alpha", "beta", "gamma",
                        "throughput", "cost", "readOnHit"
                    ]
                )
                top_df = top_df.sort_values(
                    by=["throughput", "cost", "readOnHit"],
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
                if group_by == "family":
                    top_df.groupby("family").head(top).sort_values(
                        by=["family", "throughput", "cost", "readOnHit"],
                        ascending=[True, False, True, False]
                    ).to_csv(f"top_{top}_results.csv", index=False)
                else:
                    top_df.head(top).sort_values(
                        by=["cpuEff", "throughput", "cost", "readOnHit"],
                        ascending=[False, False, True, False]
                    ).to_csv(f"top_{top}_results.csv", index=False)

    return results
