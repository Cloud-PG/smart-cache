import subprocess
import sys
from os import path

SIM_NAME = "goCacheSim"


def get_simulator_exe(force_creation: bool = False) -> str:
    cur_dir = path.dirname(path.abspath(__file__))
    sim_path = path.join(cur_dir, SIM_NAME)
    if force_creation or not path.exists(sim_path) or not path.isfile(sim_path):
        try:
            subprocess.check_output(
                f"./build_sim.sh", shell=True, cwd=cur_dir,
                stderr=subprocess.STDOUT
            )
        except subprocess.CalledProcessError as err:
            print(f"[BUILD ERROR]:\n{err.output.decode('ascii')}")
            exit(-1)
    return sim_path
