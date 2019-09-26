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
CACHE_TYPES = [
    'lru',
    'weightedLRU'
]
FORCE_CREATION = True


def main():
    simulator_exe = get_simulator_exe(force_creation=FORCE_CREATION)
    print(simulator_exe)
    base_dir = path.join(path.dirname(
        path.abspath(__file__)), "simulation_results")

    # Normal run
    for cache_type in CACHE_TYPES:
        working_dir = path.join(
            base_dir,
            f"run_full_normal_{cache_type}_{CACHE_SIZE}_{REGION}"
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
            cwd=working_dir
        )
        cur_process.wait()


if __name__ == "__main__":
    main()
