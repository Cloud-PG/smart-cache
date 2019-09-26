from os import path
import subprocess

SIM_NAME = "goCacheSim"


def get_simulator_exe() -> str:
    cur_dir = path.dirname(path.abspath(__file__))
    sim_path = path.join(cur_dir, SIM_NAME)
    if not path.exists(sim_path) or not path.isfile(sim_path):
        subprocess.check_call(
            f"go build -o {SIM_NAME} main.go", shell=True, cwd=cur_dir)
    return sim_path
