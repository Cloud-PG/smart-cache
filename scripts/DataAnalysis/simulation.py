# import argparse
import os
import subprocess
from os import path

from SmartCache.sim import get_simulator_exe

START_FROM_WINDOW = 0
STOP_WINDOW = 2
WINDOW_SIZE = 1
REGION = "it"
CACHE_SIZE = 10485760
RESULT_FOLDER = "results_8w_with_sizes_csv"
CACHE_TYPES = {
    'lru': {},
    'weightedLRU': {},
}
FORCE_CREATION = True


def read_output_last_line(output):
    buffer = ""
    cur_char = output.read(1).decode("ascii")
    while cur_char not in ["\r", "\n"]:
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
    simulator_exe = get_simulator_exe(force_creation=FORCE_CREATION)

    base_dir = path.join(path.dirname(
        path.abspath(__file__)), "simulation_results")

    processes = []

    # Normal run
    normal_run_dir = working_dir = path.join(
        base_dir,
        "run_full_normal"
    )
    os.makedirs(normal_run_dir, exist_ok=True)

    for cache_type in CACHE_TYPES:
        working_dir = path.join(
            normal_run_dir,
            f"{cache_type}_{CACHE_SIZE}_{REGION}"
        )
        os.makedirs(working_dir, exist_ok=True)
        cur_process = subprocess.Popen(
            " ".join([
                simulator_exe,
                "simulate",
                cache_type,
                path.abspath(RESULT_FOLDER),
                f"--size={CACHE_SIZE}",
                f"--simRegion={REGION}",
                f"--simWindowSize={WINDOW_SIZE}",
                f"--simStartFromWindow={START_FROM_WINDOW}",
                f"--simStopWindow={STOP_WINDOW}",
            ]),
            shell=True,
            cwd=working_dir,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        processes.append(cur_process)

    while job_run(processes):
        for process in processes:
            try:
                process.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                pass


if __name__ == "__main__":
    main()
