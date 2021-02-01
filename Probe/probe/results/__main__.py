from enum import Enum
from os import path
from typing import List

import typer
from colorama import init

from ..utils import STATUS_ARROW
from .dashboard.function import service
from .data import aggregate_results, parse_simulation_report
from .plotters import metric_corr, plot_miss_freq, plot_num_miss_after_del


class PlotType(str, Enum):
    afterdelete = "AFTERDELETE"
    missfreq = "MISSFREQ"


app = typer.Typer(name="probe.results", add_completion=False)


@app.command()
def dashboard(folders: "List[str]", dash_ip: str = "localhost"):
    service(folders, dash_ip)


@app.command()
def plot(
    folders: List[str],
    p_type: PlotType = PlotType.afterdelete,
    output_filename: str = "",
):
    init()

    print(f"{STATUS_ARROW}Aggregate results...")
    results = aggregate_results(folders)

    if p_type.value == "AFTERDELETE":
        plot_num_miss_after_del(
            parse_simulation_report(
                results.get_all(),
                path.commonprefix(results.files),
                generator=True,
                target=p_type.value,
            ),
            output_filename=output_filename,
        )
    elif p_type.value == "MISSFREQ":
        plot_miss_freq(
            parse_simulation_report(
                results.get_all(),
                path.commonprefix(results.files),
                generator=True,
                target=p_type.value,
            ),
            output_filename=output_filename,
        )


@app.command()
def plot_corr(
    folders: List[str],
):
    print(f"{STATUS_ARROW}Aggregate results...")
    results = aggregate_results(folders)

    print(f"{STATUS_ARROW}Calculate and plot correlation matrix...")
    metric_corr(results.get_all_df())


if __name__ == "__main__":
    app(prog_name="probe.results")
