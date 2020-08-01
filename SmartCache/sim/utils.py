import logging
import subprocess
import sys
from os import path
import argparse
import pathlib

import coloredlogs

from .simulator.utils import str2bool

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


def get_simulator_exe(force_creation: bool = False, release: bool=False) -> str:
    logger = get_logger(__name__)
    cur_dir = pathlib.Path(__file__).parent.absolute()
    sim_path = cur_dir.joinpath('bin', SIM_NAME)
    logger.debug(f"[BUILD]->[Sim Path][{sim_path}]")
    if force_creation or not path.exists(sim_path) or not path.isfile(sim_path):
        try:
            logger.debug(f"[BUILD]->[RUN]")
            print(subprocess.check_output(
                "./build_sim.sh" if not release else "./build_sim_release.sh",
                shell=True, cwd=cur_dir,
                stderr=subprocess.STDOUT
            ).decode("utf-8"), end="")
        except subprocess.CalledProcessError as err:
            logger.debug(f"[BUILD]->[ERROR]{err.output.decode('utf-8')}")
            print(f"[BUILD ERROR]:\n{err.output.decode('utf-8')}")
            exit(-1)
    return sim_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Go Simulator Utils')
    
    parser.register('type', 'bool', str2bool)  # add type keyword to registries
    
    parser.add_argument(
        'command', type=str, choices=["compile"],
        help='the command to execute'
    )
    parser.add_argument(
        '--release', type='bool', default=False,
        help='build the release binary'
    )

    args = parser.parse_args()
    if args.command == "compile":
        get_simulator_exe(True, args.release)
