import argparse
import os
import subprocess
from os import path

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


def main():
    parser = argparse.ArgumentParser(
        "simulation", description="Simulation and result plotting")
    parser.add_argument('source', type=str,
                        default="./results_8w_with_sizes_csv",
                        help='The folder where the json results are stored')
    parser.add_argument('-FEB', '--force-exe-build', type=bool,
                        default=True,
                        help='Force to build the simulation executable')
    parser.add_argument('-CS', '--cache-size', type=int,
                        default=10485760,
                        help='Size of the cache to simulate')
    parser.add_argument('-R', '--region', type=str,
                        default="all",
                        help='Region of the data to simulate')
    parser.add_argument('-WS', '--window-size', type=int,
                        default=7,
                        help='Size of the window to simulate')
    parser.add_argument('-WSTA', '--window-start', type=int,
                        default=0,
                        help='Window where to start from')
    parser.add_argument('-WSTO', '--window-stop', type=int,
                        default=4,
                        help='Window where to stop')

    args, _ = parser.parse_known_args()


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


if __name__ == "__main__":
    main()
