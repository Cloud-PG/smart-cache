import logging
import subprocess
import sys
from os import path

import coloredlogs

SIM_NAME = "simulator"


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


def get_simulator_exe(force_creation: bool = False) -> str:
    logger = get_logger(__name__)
    cur_dir = path.dirname(path.abspath(__file__))
    sim_path = path.join(cur_dir, 'bin', SIM_NAME)
    logger.debug(f"[BUILD]->[Sim Path][{sim_path}]")
    if force_creation or not path.exists(sim_path) or not path.isfile(sim_path):
        try:
            logger.debug(f"[BUILD]->[RUN]")
            subprocess.check_output(
                f"./build_sim.sh", shell=True, cwd=cur_dir,
                stderr=subprocess.STDOUT
            )
        except subprocess.CalledProcessError as err:
            logger.debug(f"[BUILD]->[ERROR]{err.output.decode('ascii')}")
            print(f"[BUILD ERROR]:\n{err.output.decode('ascii')}")
            exit(-1)
    return sim_path
