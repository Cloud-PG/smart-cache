from enum import Enum

import typer

from .simulator import get_simulator_exe, get_simulator_path
from .utils import str2bool


class PlotType(str, Enum):
    afterdelete = "AFTERDELETE"
    missfreq = "MISSFREQ"


app = typer.Typer(name="utils", add_completion=False)


@app.command()
def sim_path():
    _, simPath = get_simulator_path()
    print(simPath.as_posix())


@app.command()
def compile(release: bool = typer.Option(False, "--release"),
            fast: bool = typer.Option(False, "--fast")):
    get_simulator_exe(True, release, fast)


if __name__ == "__main__":
    app(prog_name="utils")
