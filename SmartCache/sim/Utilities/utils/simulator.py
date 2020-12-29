import logging
import subprocess
from os import path
import pathlib

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


def get_simulator_path() -> tuple:
    cur_dir = pathlib.Path(__file__).parent.parent.parent.absolute()
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
