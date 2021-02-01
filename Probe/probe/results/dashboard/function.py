from typing import List
import dash
import dash_bootstrap_components as dbc

# import dash_core_components as dcc
# import dash_daq as daq
import dash_html_components as html

# Create random data with numpy
import pandas as pd
from colorama import init

# import plotly.express as px
# import plotly.graph_objects as go
from dash.dependencies import Input, Output, State
from flask import send_file

from ..data import Results, aggregate_results
from . import view
from .callbacks import (
    compare_results,
    show_value,
    switch_tab,
    toggle_collapse_table,
    unselect_all_files,
)
from .utils import DashCacheManager
from .vars import DASH_CACHE_DIRS, STATUS_ARROW

_EXTERNAL_STYLESHEETS = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]


def service(folders: "List[str]", dash_ip: str = "localhost"):
    init()

    print(f"{STATUS_ARROW}Aggregate results...")
    results = aggregate_results(folders)

    print(f"{STATUS_ARROW}Start dashboard...")
    create(results, dash_ip)


def create(results: "Results", server_ip: str = "localhost"):

    cache_manager = DashCacheManager(DASH_CACHE_DIRS)
    cache_manager.init().set("results", hash_="data", data=results)

    app = dash.Dash(
        "Result Dashboard",
        external_stylesheets=[_EXTERNAL_STYLESHEETS, dbc.themes.BOOTSTRAP],
        suppress_callback_exceptions=True,  # For dynamic callback attachment
    )

    @app.server.route("/table/csv")
    def download_csv():
        return send_file(
            cache_manager.path("results", hash_="table.csv"),
            mimetype="text/csv",
            attachment_filename="table.csv",
            as_attachment=True,
        )

    @app.server.route("/table/tex")
    def download_tex():
        return send_file(
            cache_manager.path("results", hash_="table.tex"),
            mimetype="text/plain",
            attachment_filename="table.tex",
            as_attachment=True,
        )

    @app.server.route("/table/html")
    def download_html():
        return send_file(
            cache_manager.path("results", hash_="table.html"),
            mimetype="text/plain",
            attachment_filename="table.html",
            as_attachment=True,
        )

    _TAB_FILES = view.files(results)
    _TAB_FILTERS = view.filters(results)
    _TAB_COLUMNS = view.columns()
    _TAB_MEASURES = view.measures()
    _TAB_AGENTS = view.agents()
    _TAB_TABLE = view.table()
    _TAB_COMPARE = view.compare()

    _TABS = dbc.Tabs(
        [
            dbc.Tab(_TAB_FILES, label="Files", tab_id="tab-files"),
            dbc.Tab(_TAB_FILTERS, label="Filters", tab_id="tab-filters"),
            dbc.Tab(_TAB_COLUMNS, label="Columns", tab_id="tab-columns"),
            dbc.Tab(_TAB_MEASURES, label="Measures", tab_id="tab-measures"),
            dbc.Tab(_TAB_AGENTS, label="Agents", tab_id="tab-agents"),
            dbc.Tab(_TAB_TABLE, label="Table", tab_id="tab-table"),
            dbc.Tab(_TAB_COMPARE, label="Compare eviction", tab_id="tab-compare"),
        ],
        id="tabs",
    )

    app.layout = html.Div(
        children=[
            html.H1(children="Result Dashboard"),
            _TABS,
        ],
        style={"padding": "1em"},
    )

    app.callback(
        dash.dependencies.Output("toggle-extended-table-output", "children"),
        [dash.dependencies.Input("toggle-extended-table", "value")],
    )(show_value("Extended table:"))

    app.callback(
        dash.dependencies.Output("toggle-sort-by-roh-first-output", "children"),
        [dash.dependencies.Input("toggle-sort-by-roh-first", "value")],
    )(show_value("Sort by read on hit:"))

    app.callback(
        dash.dependencies.Output("toggle-new-metrics-output", "children"),
        [dash.dependencies.Input("toggle-new-metrics", "value")],
    )(show_value("Use new metrics:"))

    app.callback(
        [
            Output("sel-sim-compare-plot-actions", "children"),
            Output("sel-sim-compare-plot-actions-hist-numReq", "children"),
            Output("sel-sim-compare-plot-actions-hist-size", "children"),
            Output("sel-sim-compare-plot-actions-hist-deltaT", "children"),
            Output("sel-sim-compare-plot-after", "children"),
        ],
        [Input("sel-num-sim", "value"), Input("sel-tick", "value")],
        [
            State("selected-files", "value"),
            State("selected-filters-all", "value"),
            State("selected-filters-any", "value"),
            State("num-of-results", "value"),
        ],
    )(compare_results)

    app.callback(
        [Output(f"collapse-{i}", "is_open") for i in range(len(results))],
        [Input(f"group-{i}-toggle", "n_clicks") for i in range(len(results))],
        [State(f"collapse-{i}", "is_open") for i in range(len(results))],
    )(toggle_collapse_table)

    app.callback(
        dash.dependencies.Output("selected-files", "value"),
        [
            dash.dependencies.Input("unselect-files", "n_clicks"),
            dash.dependencies.Input("select-files", "n_clicks"),
        ],
    )(unselect_all_files)

    app.callback(
        [
            Output("graphs-columns", "children"),
            Output("graphs-measures", "children"),
            Output("graphs-agents", "children"),
            Output("table", "children"),
            Output("compare", "children"),
            Output("compare-tables", "children"),
        ],
        [
            Input("tabs", "active_tab"),
            Input("toggle-extended-table", "value"),
            Input("toggle-sort-by-roh-first", "value"),
            Input("toggle-new-metrics", "value"),
        ],
        [
            State("selected-files", "value"),
            State("selected-filters-all", "value"),
            State("selected-filters-any", "value"),
            State("num-of-results", "value"),
        ],
    )(switch_tab)

    app.run_server(
        debug=True,
        host=server_ip,
    )