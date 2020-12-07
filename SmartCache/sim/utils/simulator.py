import logging
import subprocess
import sys
from os import path
import argparse
import pathlib

import coloredlogs

from .utils import str2bool

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


def get_simulator_path() -> tuple['pathlib.Path', 'pathlib.Path']:
    cur_dir = pathlib.Path(__file__).parent.parent.absolute()
    sim_path = cur_dir.joinpath('bin', SIM_NAME)
    return cur_dir, sim_path


def get_simulator_exe(force_creation: bool = False, release: bool = False, fast: bool = False) -> str:
    logger = get_logger(__name__)
    cur_dir, sim_path = get_simulator_path()
    logger.info(f"[BUILD]->[Sim Path][{sim_path}]")
    if force_creation or not path.exists(sim_path) or not path.isfile(sim_path):
        command = """go build -race {}-v -o bin -ldflags "{}-X main.buildstamp=`date -u '+%Y-%m-%d_%I:%M:%S%p'` -X main.githash=`git rev-parse HEAD`" ./..."""
        try:
            comiled_command = command.format(
                "" if fast else "-a ",
                "-s -w " if release else "",
            )
            print(comiled_command)
            logger.info(f"[BUILD]-> Command: {comiled_command}")
            logger.info(f"[BUILD]-> Running...")
            print(subprocess.check_output(
                comiled_command,
                shell=True, cwd=cur_dir,
                stderr=subprocess.STDOUT
            ).decode("utf-8"), end="")
            logger.info(f"[BUILD]-> Done!")
        except subprocess.CalledProcessError as err:
            logger.info(f"[BUILD]->[ERROR]{err.output.decode('utf-8')}")
            exit(-1)
    return sim_path.as_posix()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Go Simulator Utils')

    parser.register('type', 'bool', str2bool)  # add type keyword to registries

    parser.add_argument(
        'command', type=str, choices=["compile", "simPath"],
        help='the command to execute'
    )
    parser.add_argument(
        '--release', type='bool', default=False,
        help='build the release binary'
    )
    parser.add_argument(
        '--fast', type='bool', default=False,
        help='build only modified files'
    )

    args = parser.parse_args()
    if args.command == "compile":
        get_simulator_exe(True, args.release, args.fast)
    elif args.command == "simPath":
        _, simPath = get_simulator_path()
        print(simPath.as_posix())
