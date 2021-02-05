import os
import pathlib
import pickle
from os import path
from shutil import rmtree
from typing import Any, Tuple

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html

# Create random data with numpy
import pandas as pd
import plotly.express as px

# import plotly.express as px
import plotly.graph_objects as go
import numpy as np

from ..data import SIM_RESULT_FILENAME
from .vars import PLOT_LAYOUT


class DashCacheManager:
    def __init__(self, dirs: list):
        self._main_dir = pathlib.Path(".").resolve().joinpath(".cache")
        self._dirs = dirs

    def init(self):
        rmtree(self._main_dir.as_posix(), ignore_errors=True)
        for dir_ in self._dirs:
            dir_path = self._main_dir.joinpath(dir_)
            os.makedirs(dir_path, exist_ok=True)
        return self

    def _filename(self, folder: str, hash_args: tuple = (), hash_: str = ""):
        if hash_ == "":
            cur_hash = selection2hash(*hash_args)
            return self._main_dir.joinpath(folder, cur_hash)
        return self._main_dir.joinpath(folder, hash_)

    def check(self, folder: str, hash_args: tuple = (), hash_: str = "") -> bool:
        return self._filename(folder, hash_args, hash_).exists()

    def path(self, folder: str, hash_args: tuple = (), hash_: str = "") -> str:
        return self._filename(folder, hash_args, hash_).as_posix()

    def set(
        self, folder: str, hash_args: tuple = (), hash_: str = "", data: "Any" = None
    ):
        filename = self._filename(folder, hash_args, hash_)
        with open(filename, "wb") as target_file:
            target_file.write(pickle.dumps(data, pickle.HIGHEST_PROTOCOL))
        return self

    def get(self, folder: str, hash_args: tuple = (), hash_: str = "") -> "Any":
        filename = self._filename(folder, hash_args, hash_)
        with open(filename, "rb") as target_file:
            return pickle.loads(target_file.read())


def parse_simulation_report_stuff(
    delEvaluators: list, tot_results: int
) -> Tuple[list, list]:
    figs = []
    tables = []

    fig = go.Figure(layout=PLOT_LAYOUT)
    for name, evaluators in delEvaluators.items():

        x = [evaluator.tick for evaluator in evaluators]
        fig.add_trace(
            go.Scatter(
                x=x,
                y=[evaluator.num_deleted_files for evaluator in evaluators],
                mode="lines+markers",
                name=f"{name} - # del. files",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=[
                    int(evaluator.total_size_deleted_files / 1024.0)
                    for evaluator in evaluators
                ],
                mode="lines+markers",
                name=f"{name} - tot. Size (GB)",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=[evaluator.total_num_req_after_delete for evaluator in evaluators],
                mode="lines+markers",
                name=f"{name} - # req. after del.",
            )
        )
    fig.update_layout(
        title="Report",
        xaxis_title="tick",
        yaxis_title="",
        autosize=True,
        # width=1920,
        height=480,
    )

    figs.append(dcc.Graph(figure=fig))
    figs.append(html.Hr())

    table_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("Tick"),
                    html.Th("Event"),
                    html.Th("# Del. Files"),
                    html.Th("Tot. Size (GB)"),
                    html.Th("Tot. # req after del."),
                    html.Th("Cache Size"),
                    html.Th("Cache Occupancy"),
                ]
            )
        )
    ]

    tables.append(html.Hr())
    tables.append(html.H1("Tables"))

    for idx, sim in enumerate(delEvaluators):
        evaluators = delEvaluators[sim]
        tables.append(
            dbc.CardHeader(
                html.H2(
                    dbc.Button(
                        f"[{idx}] -> {sim}",
                        color="link",
                        id=f"group-{idx}-toggle",
                    )
                )
            )
        )
        cur_rows = []
        for evaluator in evaluators:
            cur_rows.append(
                html.Tr(
                    [
                        html.Td(evaluator.tick),
                        html.Td(evaluator.event),
                        html.Td(evaluator.num_deleted_files),
                        html.Td(int(evaluator.total_size_deleted_files / 1024.0)),
                        html.Td(evaluator.total_num_req_after_delete),
                        html.Td(evaluator.on_delete_cache_size),
                        html.Td(evaluator.on_delete_cache_occupancy),
                    ]
                )
            )
        table_body = [html.Tbody(cur_rows)]
        tables.append(
            dbc.Collapse(
                dbc.CardBody(
                    [
                        dbc.Table(
                            # using the same table as in the above example
                            table_header + table_body,
                            bordered=True,
                            hover=True,
                            responsive=True,
                            striped=True,
                        )
                    ]
                ),
                id=f"collapse-{idx}",
            )
        )
        tables.append(html.Hr())

    # to satisfy filtered evaluator callbacks
    for idx in range(len(delEvaluators), tot_results):
        tables.append(
            dbc.CardHeader(
                html.H2(
                    dbc.Button(
                        color="link",
                        id=f"group-{idx}-toggle",
                    )
                ),
                style={"display": "none"},
            )
        )
        tables.append(
            dbc.Collapse(
                dbc.CardBody(f"This is the content of group {idx}..."),
                id=f"collapse-{idx}",
                style={"display": "none"},
            )
        )

    return figs, tables


def get_prefix(files2plot: list) -> str:
    """Check the prefix of list of files to plot

    :param files2plot: list of files and dataframes to plot
    :type files2plot: list
    :return: the commond prefix of the list of files
    :rtype: str
    """
    return path.commonprefix([file_ for file_, *_ in files2plot])


def _reduce(vector: "np.array", size_cluster: int, method: str = "avg") -> "list":
    res = []
    partials = [vector[0]]

    for idx in range(len(vector)):
        if idx % size_cluster == 0:
            if method == "avg":
                res.append(sum(partials) / len(partials))
                partials = []
            else:
                res.append(partials.pop())
        else:
            partials.append(vector[idx])

    return res


def make_line_figures(
    files2plot: list,
    prefix: str,
    title: str,
    function: callable = None,
    column: str = "",
    additional_traces: list = [],
    binning_size: int = 1,
) -> "go.Figure":
    """Make measure plots

    :param files2plot: list of files to plot with their dataframes
    :type files2plot: list
    :param prefix: the files' prefix
    :type prefix: str
    :param title: the title of the current figure
    :type title: str
    :param function: the measure function to call, defaults to None
    :type function: callable
    :param column: the column to select from the dataframe, defaults to ""
    :type column: str
    :return: a plot figure
    :rtype: go.Figure
    """
    fig = go.Figure(layout=PLOT_LAYOUT)

    if len(additional_traces) > 0:
        for trace in additional_traces:
            fig.add_trace(trace)

    for file_, df in files2plot:
        name = file_.replace(prefix, "").replace(SIM_RESULT_FILENAME, "")
        if function is not None:
            y_ax = function(df)
        elif column != "":
            y_ax = df[column]
        x_ax = df["date"]

        if binning_size > 1:
            x_ax = _reduce(x_ax.to_list(), binning_size, None)
            y_ax = _reduce(y_ax.to_list(), binning_size)

        fig.add_trace(
            go.Scatter(
                x=x_ax,
                y=y_ax,
                mode="lines",
                name=name,
            )
        )
    fig.update_layout(
        title=title,
        xaxis_title="day",
        yaxis_title=title,
        autosize=True,
        # width=1920,
        height=800,
    )
    return fig


def get_files2plot(
    results: "Results",
    files: list,
    filters_all: list,
    filters_any: list,
    column: str = "",
    agents: bool = False,
    with_log: bool = False,
) -> list:
    """Returns a filtered list of files to plot (name and dataframe)

    :param results: the result object with simulation data
    :type results: Results
    :param files: list of current file selection
    :type files: list
    :param filters_all: filters to apply for all file
    :type filters_all: list
    :param filters_any: filters to apply not exclusively
    :type filters_any: list
    :param column: column to plot, defaults to ""
    :type column: str, optional
    :param agents: search for agents, defaults to False
    :type agents: bool, optional
    :return: a list of files and dataframes
    :rtype: list
    """
    files2plot = []
    for file_ in files:
        df = results.get_df(file_, filters_all, filters_any)
        if df is not None:
            if column != "":
                if column in df.columns:
                    if with_log:
                        log_df = results.get_log(file_, filters_all, filters_any)
                        if log_df is not None:
                            files2plot.append((file_, df, log_df))
                    else:
                        files2plot.append((file_, df))
            elif agents:
                if "Addition epsilon" in df.columns:
                    if with_log:
                        log_df = results.get_log(file_, filters_all, filters_any)
                        if log_df is not None:
                            files2plot.append((file_, df, log_df))
                    else:
                        files2plot.append((file_, df))
            else:
                if with_log:
                    log_df = results.get_log(file_, filters_all, filters_any)
                    if log_df is not None:
                        files2plot.append((file_, df, log_df))
                else:
                    files2plot.append((file_, df))
    return files2plot


def selection2hash(
    files: list,
    filters_all: list,
    filters_any: list,
    num_of_results: int,
    extended: bool = False,
    sort_by_roh_first: bool = False,
    new_metrics: bool = True,
    columns_binning_size: int = 1,
    measures_binning_size: int = 1,
) -> str:
    return str(
        hash(
            " ".join(
                files
                + filters_all
                + filters_any
                + [
                    str(num_of_results),
                    str(extended),
                    str(sort_by_roh_first),
                    str(new_metrics),
                    str(columns_binning_size),
                    str(measures_binning_size),
                ]
            )
        )
    )


def make_agent_figures(files2plot: list, prefix: str) -> list:
    """Prepare agent plot figures

    :param files2plot: list of files and dataframes to plot
    :type files2plot: list
    :param prefix: prefix string of files
    :type prefix: str
    :return: list of figure elements
    :rtype: list
    """
    figures = []
    _AGENT_COLUMNS = {
        "Epsilon": ["Addition epsilon", "Eviction epsilon"],
        "QValue": [
            "Addition qvalue function",
            "Eviction qvalue function",
        ],
        "Eviction calls": ["Eviction calls", "Eviction forced calls"],
        "Eviction categories": [
            "Eviction mean num categories",
            "Eviction std dev num categories",
        ],
        "Addition actions": ["Action store", "Action not store"],
        "Eviction actions": [
            "Action delete all",
            "Action delete half",
            "Action delete quarter",
            "Action delete one",
            "Action not delete",
        ],
    }
    for plot, columns in _AGENT_COLUMNS.items():
        fig_epsilon = go.Figure(layout=PLOT_LAYOUT)
        for file_, df in files2plot:
            name = file_.replace(prefix, "").replace(SIM_RESULT_FILENAME, "")
            for column in columns:
                _add_columns(fig_epsilon, df, name, column)
        fig_epsilon.update_layout(
            title=plot,
            xaxis_title="day",
            yaxis_title=plot,
            autosize=True,
            # width=1920,
            height=800,
        )
        figures.append(dcc.Graph(figure=fig_epsilon))
        figures.append(html.Hr())

    return figures


def _add_columns(fig: "go.Figure", df: "pd.DataFrame", name: str, column: str):
    """Add a specific column to plot as trace line

    :param fig: the figure where insert the trace
    :type fig: go.Figure
    :param df: the dataframe
    :type df: pd.DataFrame
    :param name: name of the file
    :type name: str
    :param column: name of the column
    :type column: str
    """
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df[column],
            mode="lines",
            name=f"{name}[{column}]",
        )
    )
