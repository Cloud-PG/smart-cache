import tempfile
from os import path
from typing import Tuple

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
# Create random data with numpy
import pandas as pd
import plotly.express as px
# import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import Input, Output, State
from flask import send_file
from plotly.graph_objs import Layout

from .data import (COLUMNS, SIM_RESULT_FILENAME, Results, make_table,
                   measure_avg_free_space, measure_bandwidth, measure_cost,
                   measure_cost_ratio, measure_cpu_eff, measure_hit_over_miss,
                   measure_hit_rate, measure_num_miss_after_delete,
                   measure_read_on_hit_ratio, measure_redirect_volume,
                   measure_std_dev_free_space, measure_throughput,
                   measure_throughput_ratio, parse_simulation_report)

_CSV_TEMP_FILE = tempfile.NamedTemporaryFile(mode="w", delete=False)
_TEX_TEMP_FILE = tempfile.NamedTemporaryFile(mode="w", delete=False)
_HTML_TEMP_FILE = tempfile.NamedTemporaryFile(mode="w", delete=False)

_EXTERNAL_STYLESHEETS = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

LAYOUT = Layout(
    paper_bgcolor='rgb(255,255,255)',
    plot_bgcolor='rgb(255,255,255)',
    yaxis={'gridcolor': 'black'},
    xaxis={'gridcolor': 'black'},
)

_MEASURES = {
    'Throughput ratio': measure_throughput_ratio,
    'Cost ratio': measure_cost_ratio,
    'Throughput (TB)': measure_throughput,
    'Cost (TB)': measure_cost,
    'Read on hit ratio': measure_read_on_hit_ratio,
    'CPU Eff.': measure_cpu_eff,
    'Avg. Free Space': measure_avg_free_space,
    'Std. Dev. Free Space': measure_std_dev_free_space,
    'Bandwidth': measure_bandwidth,
    'Redirect Vol.': measure_redirect_volume,
    'Hit over Miss': measure_hit_over_miss,
    "Num. miss after del.": measure_num_miss_after_delete,
    'Hit rate': measure_hit_rate,
}


def get_files2plot(results: 'Results', files: list, filters_all: list,
                   filters_any: list, column: str = "",
                   agents: bool = False, with_log: bool = False) -> list:
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
                        log_df = results.get_log(
                            file_, filters_all, filters_any)
                        if log_df is not None:
                            files2plot.append((file_, df, log_df))
                    else:
                        files2plot.append((file_, df))
            elif agents:
                if "Addition epsilon" in df.columns:
                    if with_log:
                        log_df = results.get_log(
                            file_, filters_all, filters_any)
                        if log_df is not None:
                            files2plot.append((file_, df, log_df))
                    else:
                        files2plot.append((file_, df))
            else:
                if with_log:
                    log_df = results.get_log(
                        file_, filters_all, filters_any)
                    if log_df is not None:
                        files2plot.append((file_, df, log_df))
                else:
                    files2plot.append((file_, df))
    return files2plot


def get_prefix(files2plot: list) -> str:
    """Check the prefix of list of files to plot

    :param files2plot: list of files and dataframes to plot
    :type files2plot: list
    :return: the commond prefix of the list of files
    :rtype: str
    """
    return path.commonprefix([file_ for file_, *_ in files2plot])


def dashboard(results: 'Results', server_ip: str = "localhost"):

    _CACHE = {
        'columns': {},
        'measures': {},
        'agents': {},
        'tables': {},
        'compare': {}
    }

    app = dash.Dash("Result Dashboard", external_stylesheets=[
        _EXTERNAL_STYLESHEETS, dbc.themes.BOOTSTRAP
    ], suppress_callback_exceptions=True)

    @app.server.route('/table/csv')
    def download_csv():
        _CSV_TEMP_FILE.seek(0)
        return send_file(
            _CSV_TEMP_FILE.name,
            mimetype='text/csv',
            attachment_filename='table.csv',
            as_attachment=True,
        )

    @app.server.route('/table/tex')
    def download_tex():
        _TEX_TEMP_FILE.seek(0)
        return send_file(
            _TEX_TEMP_FILE.name,
            mimetype='text/plain',
            attachment_filename='table.tex',
            as_attachment=True,
        )

    @app.server.route('/table/html')
    def download_html():
        print("SEND html")
        _HTML_TEMP_FILE.seek(0)
        return send_file(
            _HTML_TEMP_FILE.name,
            mimetype='text/plain',
            attachment_filename='table.html',
            as_attachment=True,
        )

    _TAB_FILES = dbc.Card(
        dbc.CardBody(
            [
                dbc.Button(
                    "Unselect all", color="warning", block=True, id="unselect-files",
                ),
                dbc.Button(
                    "Select all", color="success", block=True, id="select-files",
                ),
                html.Hr(),
                dcc.Checklist(
                    options=[
                        {'label': f" {filename}", 'value': filename}
                        for filename in results.files
                    ],
                    value=results.files,
                    labelStyle={'display': 'block'},
                    id="selected-files",
                ),
            ]
        ),
    )

    _TAB_FILTERS = dbc.Card(
        dbc.CardBody(
            [
                dcc.Input(
                    id="num-of-results",
                    type="number",
                    placeholder="Max number of results",
                    value=0,
                ),
                html.Hr(),
                html.H2("All"),
                dcc.Checklist(
                    options=[
                        {'label': f" {component}", 'value': component}
                        for component in results.components
                    ],
                    value=[],
                    labelStyle={'display': 'block'},
                    id="selected-filters-all",
                ),
                html.Br(),
                html.H2("Any"),
                dcc.Checklist(
                    options=[
                        {'label': f" {component}", 'value': component}
                        for component in results.components
                    ],
                    value=[],
                    labelStyle={'display': 'block'},
                    id="selected-filters-any",
                ),
            ]
        ),
    )

    _TAB_COLUMNS = dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="graphs-columns",
            ),
            color="primary",
        ),
    )

    _TAB_MEASURES = dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="graphs-measures",
            ),
            color="primary",
        ),
    )

    _TAB_AGENTS = dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="graphs-agents",
            ),
            color="primary",
        ),
    )

    _TAB_TABLE = dbc.Card(
        dbc.Spinner([
            dbc.CardBody(
                dbc.ListGroup(
                    [
                        dbc.ListGroupItem(dcc.Link(
                            "Download as CSV", refresh=True,
                            href="/table/csv", target="_blank",)
                        ),
                        dbc.ListGroupItem(dcc.Link(
                            "Download as Latex table", refresh=True,
                            href="/table/tex", target="_blank",)
                        ),
                        dbc.ListGroupItem(dcc.Link(
                            "Download as html", refresh=True,
                            href="/table/html", target="_blank",)
                        ),
                    ],
                    horizontal=True,
                ),

            ),
            dbc.CardBody(
                id="table",
            )],
            color="primary",
        ),
    )

    _TAB_COMPARE = dbc.Card([
        dbc.Spinner(
            dbc.CardBody(
                id="compare",
            ),
            color="primary",
            type="grow",
        ),
        dbc.CardBody([
            dbc.Input(id="sel-num-sim",
                      placeholder="Number of simulation", type="number"),
            dbc.Input(id="sel-tick",
                      placeholder="tick of simulation", type="number"),
            dbc.Spinner([
                dbc.CardBody(
                    id="sel-sim-compare-plot-actions",
                ),
                dbc.CardBody(
                    id="sel-sim-compare-plot-actions-hist-numReq",
                ),
                dbc.CardBody(
                    id="sel-sim-compare-plot-actions-hist-size",
                ),
                dbc.CardBody(
                    id="sel-sim-compare-plot-actions-hist-deltaT",
                ),
                dbc.CardBody(
                    id="sel-sim-compare-plot-after",
                )
            ],
                color="warning",
                type="grow",
            ),
        ],
            id="compare-row",
        ),
        dbc.CardBody(
            id="compare-tables",
        ),
    ])

    _TABS = dbc.Tabs(
        [
            dbc.Tab(_TAB_FILES, label="Files", tab_id="tab-files"),
            dbc.Tab(_TAB_FILTERS, label="Filters", tab_id="tab-filters"),
            dbc.Tab(_TAB_COLUMNS, label="Columns", tab_id="tab-columns"),
            dbc.Tab(_TAB_MEASURES, label="Measures", tab_id="tab-measures"),
            dbc.Tab(_TAB_AGENTS, label="Agents", tab_id="tab-agents"),
            dbc.Tab(_TAB_TABLE, label="Table", tab_id="tab-table"),
            dbc.Tab(_TAB_COMPARE, label="Compare eviction",
                    tab_id="tab-compare"),
        ],
        id="tabs",
    )

    app.layout = html.Div(children=[
        html.H1(children='Result Dashboard'),
        _TABS,
    ], style={'padding': "1em"})

    def selection2hash(files: list, filters_all: list, filters_any: list, num_of_results: int) -> str:
        return str(hash(" ".join(files + filters_all + filters_any + [str(num_of_results)])))

    @app.callback(
        [
            Output("sel-sim-compare-plot-actions", "children"),
            Output("sel-sim-compare-plot-actions-hist-numReq", "children"),
            Output("sel-sim-compare-plot-actions-hist-size", "children"),
            Output("sel-sim-compare-plot-actions-hist-deltaT", "children"),
            Output("sel-sim-compare-plot-after", "children"),
        ],
        [
            Input("sel-num-sim", "value"),
            Input("sel-tick", "value")
        ],
        [
            State("selected-files", "value"),
            State("selected-filters-all", "value"),
            State("selected-filters-any", "value"),
            State("num-of-results", "value")
        ]
    )
    def output_text(num_sim, tick, files, filters_all, filters_any, num_of_results):
        cur_hash = selection2hash(
            files, filters_all, filters_any, num_of_results
        )
        if cur_hash in _CACHE['compare']:
            data, *_ = _CACHE['compare'][cur_hash]
            keys = list(data.keys())
            try:
                cur_sim = keys[num_sim]
                for evaluator in data[cur_sim]:
                    if evaluator.tick == tick:
                        scatterActionsFig = px.scatter_3d(
                            evaluator.actions,
                            x='num req',
                            y='size',
                            z='filename',
                            color='delta t',
                            size='size',
                            opacity=0.9,
                        )
                        scatterActionsFig.update_layout(LAYOUT)
                        histActionNumReq = px.histogram(
                            evaluator.actions, x='num req')
                        histActionNumReq.update_layout(LAYOUT)
                        histActionSize = px.histogram(
                            evaluator.actions, x='size')
                        histActionSize.update_layout(LAYOUT)
                        histActionDeltaT = px.histogram(
                            evaluator.actions, x='delta t')
                        histActionDeltaT.update_layout(LAYOUT)
                        after_data = evaluator.after4scatter
                        scatterAfterFig = px.scatter_3d(
                            after_data,
                            x='num req',
                            y='size',
                            z='filename',
                            color='delta t',
                            size='size',
                            opacity=0.9,
                        )
                        scatterAfterFig.update_layout(LAYOUT)
                        return (
                            [dcc.Graph(figure=scatterActionsFig)],
                            [dcc.Graph(figure=histActionNumReq)],
                            [dcc.Graph(figure=histActionSize)],
                            [dcc.Graph(figure=histActionDeltaT)],
                            [dcc.Graph(figure=scatterAfterFig)],
                        )
                else:
                    return [dbc.Alert(f"No tick found in simulation {num_sim}", color="danger")], [""], [""], [""], [""]
            except (IndexError, TypeError):
                return [dbc.Alert(f"No simulation found at index {num_sim}", color="danger")], [""], [""], [""], [""]
        else:
            return [dbc.Alert("No results", color="warning")], [""], [""], [""], [""]

    @app.callback(
        [Output(f"collapse-{i}", "is_open") for i in range(len(results))],
        [Input(f"group-{i}-toggle", "n_clicks") for i in range(len(results))],
        [State(f"collapse-{i}", "is_open") for i in range(len(results))],
    )
    def toggle_collapse_table(*args):
        ctx = dash.callback_context

        if not ctx.triggered:
            return [False] * len(results)
        else:
            button_id = ctx.triggered[0]["prop_id"].split(".")[0]

        button_idx = int(button_id.split("-")[1])  # "group-idx-toggle"

        res = [False] * len(results)
        for idx in range(len(res)):
            # update all is open to current status
            res[idx] = args[idx + len(results)]

        if args[button_idx]:  # Check input n
            res[button_idx] = not args[button_idx + len(results)]  # is open

        return res

    @app.callback(
        dash.dependencies.Output('selected-files', 'value'),
        [
            dash.dependencies.Input('unselect-files', 'n_clicks'),
            dash.dependencies.Input('select-files', 'n_clicks'),
        ],
    )
    def unselect_all_files(unselect_n_clicks, select_n_clicks):
        # Ref: https://dash.plotly.com/advanced-callbacks
        changed_id = [
            p['prop_id'].split('.')[0]
            for p in dash.callback_context.triggered
        ][0]
        if changed_id == 'unselect-files':
            return []
        elif changed_id == 'select-files':
            return results.files
        return results.files

    @app.callback(
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
        ],
        [
            State("selected-files", "value"),
            State("selected-filters-all", "value"),
            State("selected-filters-any", "value"),
            State("num-of-results", "value")
        ]
    )
    def switch_tab(at, files, filters_all, filters_any, num_of_results):
        cur_hash = selection2hash(
            files, filters_all, filters_any, num_of_results)
        if at == "tab-files":
            return ("", "", "", "", "", "")

        elif at == "tab-filters":
            return ("", "", "", "", "", "")

        elif at == "tab-columns":
            if cur_hash in _CACHE['columns']:
                return (_CACHE['columns'][cur_hash], "", "", "", "", "")
            else:
                figures = []
                for column in COLUMNS[1:]:
                    files2plot = get_files2plot(
                        results,
                        files,
                        filters_all,
                        filters_any,
                        column,
                    )
                    prefix = get_prefix(files2plot)
                    if num_of_results != 0:
                        table = make_table(files2plot, prefix)
                        new_file2plot = get_top_n(
                            table, num_of_results, prefix)
                        files2plot = [
                            (file_, df)
                            for file_, df in files2plot
                            if file_ in new_file2plot
                        ]
                        prefix = get_prefix(files2plot)
                    figures.append(dcc.Graph(
                        figure=make_line_figures(
                            files2plot,
                            prefix,
                            title=column,
                            column=column
                        )
                    ))
                    figures.append(html.Hr())

                _CACHE['columns'][cur_hash] = figures
                return (figures, "", "", "", "", "")

        elif at == "tab-measures":
            if cur_hash in _CACHE['measures']:
                return ("", _CACHE['measures'][cur_hash], "", "", "", "")
            else:
                figures = []
                files2plot = get_files2plot(
                    results,
                    files,
                    filters_all,
                    filters_any,
                )
                prefix = get_prefix(files2plot)
                if num_of_results != 0:
                    table = make_table(files2plot, prefix)
                    new_file2plot = get_top_n(
                        table, num_of_results, prefix)
                    files2plot = [
                        (file_, df)
                        for file_, df in files2plot
                        if file_ in new_file2plot
                    ]
                    prefix = get_prefix(files2plot)
                for measure, function in sorted(
                        _MEASURES.items(), key=lambda elm: elm[0]
                ):
                    figures.append(dcc.Graph(
                        figure=make_line_figures(
                            files2plot,
                            prefix,
                            title=measure,
                            function=function,
                        )
                    ))
                    figures.append(html.Hr())

                _CACHE['measures'][cur_hash] = figures
                return ("", figures, "", "", "", "")

        elif at == "tab-agents":
            if cur_hash in _CACHE['agents']:
                return ("", "", _CACHE['agents'][cur_hash], "", "", "")
            else:
                figures = []
                files2plot = get_files2plot(
                    results,
                    files,
                    filters_all,
                    filters_any,
                    agents=True
                )
                prefix = get_prefix(files2plot)
                if num_of_results != 0:
                    table = make_table(files2plot, prefix)
                    new_file2plot = get_top_n(table, num_of_results, prefix)
                    files2plot = [
                        (file_, df)
                        for file_, df in files2plot
                        if file_ in new_file2plot
                    ]
                    prefix = get_prefix(files2plot)
                figures.extend(
                    make_agent_figures(
                        files2plot,
                        prefix,
                    )
                )
                _CACHE['agents'][cur_hash] = figures
                return ("", "", figures, "", "", "")

        elif at == "tab-table":
            if cur_hash in _CACHE['tables']:
                return ("", "", "", _CACHE['tables'][cur_hash], "", "")
            else:
                files2plot = get_files2plot(
                    results,
                    files,
                    filters_all,
                    filters_any,
                )
                prefix = get_prefix(files2plot)
                table = make_table(files2plot, prefix)
                if num_of_results != 0:
                    new_file2plot = get_top_n(table, num_of_results, prefix)
                    files2plot = [
                        (file_, df)
                        for file_, df in files2plot
                        if file_ in new_file2plot
                    ]
                    prefix = get_prefix(files2plot)
                    table = make_table(files2plot, prefix)

                _CSV_TEMP_FILE.seek(0)
                table.to_csv(_CSV_TEMP_FILE)

                _TEX_TEMP_FILE.seek(0)
                table.to_latex(_TEX_TEMP_FILE)

                _HTML_TEMP_FILE.seek(0)
                table.to_html(_HTML_TEMP_FILE)

                table = dbc.Table.from_dataframe(
                    table, striped=True, bordered=True, hover=True
                )

                _CACHE['tables'][cur_hash] = table
                return ("", "", "", table, "", "")
        elif at == "tab-compare":
            if cur_hash in _CACHE['compare']:
                _, figs, tables = _CACHE['compare'][cur_hash]
                return ("", "", "", "", figs, tables)
            else:
                files2plot = get_files2plot(
                    results,
                    files,
                    filters_all,
                    filters_any,
                    with_log=True,
                )
                prefix = get_prefix(files2plot)
                data = parse_simulation_report(files2plot, prefix)
                figs, tables = parse_simulation_report_stuff(
                    data, len(results)
                )
                _CACHE['compare'][cur_hash] = (data, figs, tables)

                return ("", "", "", "", figs, tables)
        else:
            return ("", "", "", "", "", "")

    app.run_server(
        debug=True,
        host=server_ip,
    )


def _add_columns(fig: 'go.Figure', df: 'pd.DataFrame', name: str, column: str):
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
            mode='lines',
            name=f"{name}[{column}]",
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
        "Epsilon": ['Addition epsilon', 'Eviction epsilon'],
        "QValue": ['Addition qvalue function', 'Eviction qvalue function', ],
        "Eviction calls": ['Eviction calls', 'Eviction forced calls'],
        "Eviction categories": ['Eviction mean num categories', 'Eviction std dev num categories'],
        "Addition actions": ['Action store', 'Action not store'],
        "Eviction actions": ['Action delete all', 'Action delete half', 'Action delete quarter', 'Action delete one', 'Action not delete'],
    }
    for plot, columns in _AGENT_COLUMNS.items():
        fig_epsilon = go.Figure(layout=LAYOUT)
        for file_, df in files2plot:
            name = file_.replace(
                prefix, "").replace(
                    SIM_RESULT_FILENAME, "")
            for column in columns:
                _add_columns(fig_epsilon, df, name, column)
        fig_epsilon.update_layout(
            title=plot,
            xaxis_title='day',
            yaxis_title=plot,
            autosize=True,
            # width=1920,
            height=800,
        )
        figures.append(dcc.Graph(figure=fig_epsilon))
        figures.append(html.Hr())

    return figures


def parse_simulation_report_stuff(delEvaluators: list, tot_results: int) -> Tuple[list, list]:
    figs = []
    tables = []

    fig = go.Figure(layout=LAYOUT)
    for name, evaluators in delEvaluators.items():

        x = [evaluator.tick for evaluator in evaluators]
        fig.add_trace(
            go.Scatter(
                x=x,
                y=[evaluator.num_deleted_files for evaluator in evaluators],
                mode='lines+markers',
                name=f"{name} - # del. files",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=[int(evaluator.total_size_deleted_files / 1024.)
                   for evaluator in evaluators],
                mode='lines+markers',
                name=f"{name} - tot. Size (GB)",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=[evaluator.total_num_req_after_delete for evaluator in evaluators],
                mode='lines+markers',
                name=f"{name} - # req. after del.",
            )
        )
    fig.update_layout(
        title="Report",
        xaxis_title='tick',
        yaxis_title='',
        autosize=True,
        # width=1920,
        height=480,
    )

    figs.append(dcc.Graph(figure=fig))
    figs.append(html.Hr())

    table_header = [
        html.Thead(
            html.Tr([
                html.Th("Tick"),
                html.Th("Event"),
                html.Th("# Del. Files"),
                html.Th("Tot. Size (GB)"),
                html.Th("Tot. # req after del."),
                html.Th("Cache Size"),
                html.Th("Cache Occupancy"),
            ])
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
                html.Tr([
                    html.Td(evaluator.tick),
                    html.Td(evaluator.event),
                    html.Td(evaluator.num_deleted_files),
                    html.Td(int(evaluator.total_size_deleted_files / 1024.)),
                    html.Td(evaluator.total_num_req_after_delete),
                    html.Td(evaluator.on_delete_cache_size),
                    html.Td(evaluator.on_delete_cache_occupancy),
                ])
            )
        table_body = [html.Tbody(cur_rows)]
        tables.append(
            dbc.Collapse(
                dbc.CardBody([
                    dbc.Table(
                        # using the same table as in the above example
                        table_header + table_body,
                        bordered=True,
                        hover=True,
                        responsive=True,
                        striped=True,
                    )
                ]),
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
                style={'display': "none"},
            )
        )
        tables.append(
            dbc.Collapse(
                dbc.CardBody(f"This is the content of group {idx}..."),
                id=f"collapse-{idx}",
                style={'display': "none"},
            )
        )

    return figs, tables


def make_line_figures(files2plot: list, prefix: str, title: str,
                      function: callable = None, column: str = "",
                      additional_traces: list = [],
                      ) -> 'go.Figure':
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
    fig = go.Figure(layout=LAYOUT)

    if len(additional_traces) > 0:
        for trace in additional_traces:
            fig.add_trace(trace)

    for file_, df in files2plot:
        name = file_.replace(
            prefix, "").replace(
                SIM_RESULT_FILENAME, "")
        if function is not None:
            y_ax = function(df)
        elif column != "":
            y_ax = df[column]
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=y_ax,
                mode='lines',
                name=name,
            )
        )
    fig.update_layout(
        title=title,
        xaxis_title='day',
        yaxis_title=title,
        autosize=True,
        # width=1920,
        height=800,
    )
    return fig


def get_top_n(df: 'pd.DataFrame', n: int, prefix: str) -> list:
    """Returns the top n files from a table ordered results

    :param df: the table dataframe
    :type df: pd.DataFrame
    :param n: the number of results to extract
    :type n: int
    :return: list of file names
    :rtype: list
    """
    return [
        f"{prefix}{filename}{SIM_RESULT_FILENAME}"
        for filename in df[:n].file.to_list()
    ]
