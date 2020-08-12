import pathlib
from os import path

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
# Create random data with numpy
import pandas as pd
# import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import Input, Output, State
from plotly.graph_objs import Layout

_SIM_RESULT_FILENAME = "/simulation_results.csv"

_EXTERNAL_STYLESHEETS = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

_COLUMNS = [
    'date',
    'num req',
    'num hit',
    'num added',
    'num deleted',
    'num redirected',
    'cache size',
    'size',
    'occupancy',
    'bandwidth',
    'bandwidth usage',
    'hit rate',
    'weighted hit rate',
    'written data',
    'read data',
    'read on hit data',
    'read on miss data',
    'deleted data',
    'avg free space',
    'std dev free space',
    'CPU efficiency',
    'CPU hit efficiency',
    'CPU miss efficiency',
    'CPU efficiency upper bound',
    'CPU efficiency lower bound',
    'Addition epsilon',
    'Eviction epsilon',
    'Addition qvalue function',
    'Eviction qvalue function',
    'Eviction calls',
    'Eviction forced calls',
    'Eviction step',
    'Action store',
    'Action not store',
    'Action delete all',
    'Action delete half',
    'Action delete quarter',
    'Action delete one',
    'Action not delete',
]

_LAYOUT = Layout(
    paper_bgcolor='rgb(255,255,255)',
    plot_bgcolor='rgb(255,255,255)',
    yaxis={'gridcolor': 'black'},
    xaxis={'gridcolor': 'black'},
)


class Element(object):

    def __init__(self, components: list, filename: str, df: 'pd.DataFrame'):
        self._df = df
        self._filename = filename
        self._components = set([elm for elm in components])

    @property
    def filename(self):
        return self._filename

    @property
    def df(self):
        return self._df

    @property
    def components(self):
        return self._components

    def __hash__(self):
        return hash(str(self._components))


class Results(object):

    def __init__(self):
        self._elemts = {}

    def insert(self, path: 'pathlib.Path', components: list, filename: str, df: 'pd.DataFrame') -> 'Results':
        elm = Element(components, filename,  df)
        self._elemts[path.as_posix()] = elm
        return self

    @property
    def components(self) -> set:
        components = set()
        for elm in self._elemts.values():
            components |= elm.components
        return sorted(components)

    @property
    def files(self) -> 'list[str]':
        return list(sorted(self._elemts.keys()))

    def get_df(self, file_: str, filters_all: list, filters_any: list):
        cur_elm = self._elemts[file_]
        all_ = len(cur_elm.components.intersection(set(filters_all))) == len(
            filters_all) if len(filters_all) > 0 else True
        any_ = len(cur_elm.components.intersection(set(filters_any))
                   ) != 0 if len(filters_any) > 0 else True
        if all_ and any_:
            return cur_elm.df
        return None


def aggregate_results(folder: str):
    abs_target_folder = pathlib.Path(folder).resolve()
    results = Results()
    all_columns = set(_COLUMNS)
    for result_path in list(
        abs_target_folder.glob("**/simulation_results.csv")
    ):
        df = pd.read_csv(result_path)
        cur_columns = set(df.columns)
        if cur_columns.issubset(all_columns):
            df['date'] = pd.to_datetime(
                df['date'].apply(lambda elm: elm.split()[0]),
                format="%Y-%m-%d"
            )
            relative_path = result_path.relative_to(
                abs_target_folder
            )
            *components, filename = relative_path.parts
            results.insert(relative_path, components, filename, df)
    return results


def _measure_throughput(df: 'pd.DataFrame') -> 'pd.Series':
    return df['read on hit data'] - df['written data']


def _measure_cost(df: 'pd.DataFrame') -> 'pd.Series':
    return df['written data'] + df['deleted data']


def _measure_cpu_eff(df: 'pd.DataFrame') -> 'pd.Series':
    return df['CPU efficiency']


def _measure_avg_free_space(df: 'pd.DataFrame') -> 'pd.Series':
    return df['avg free space']


def _measure_std_dev_free_space(df: 'pd.DataFrame') -> 'pd.Series':
    return df['std dev free space']


def _measure_bandwidth(df: 'pd.DataFrame') -> 'pd.Series':
    return (df['read on miss data'] / df['bandwidth']) * 100.


def _measure_hit_rate(df: 'pd.DataFrame') -> 'pd.Series':
    return df['hit rate']


_MEASURES = {
    'Throughput': _measure_throughput,
    'Cost': _measure_cost,
    'CPU Eff.': _measure_cpu_eff,
    'Avg. Free Space': _measure_avg_free_space,
    'Std. Dev. Free Space': _measure_std_dev_free_space,
    'Bandwidth': _measure_bandwidth,
    'Hit rate': _measure_hit_rate,
}


def _get_measures(cache_filename: str, df: 'pd.DataFrame') -> list:
    measures = [cache_filename]

    cache_size = df['cache size'][0]
    bandwidth = df['bandwidth'][0]

    # Throughput
    measures.append(
        _measure_throughput(df).mean()
    )

    # Cost
    measures.append(
        (_measure_cost(df).mean() / cache_size) * 100.
    )

    # Bandwidth
    measures.append(
        _measure_bandwidth(df).mean()
    )

    # Avg. Free Space
    measures.append(
        (_measure_avg_free_space(df).mean() / cache_size) * 100.
    )

    # Std. Dev. Free Space
    measures.append(
        (_measure_std_dev_free_space(df).mean() / cache_size) * 100.
    )

    # Hit rate
    measures.append(
        _measure_hit_rate(df).mean()
    )

    # CPU Efficiency
    measures.append(
        _measure_cpu_eff(df).mean()
    )

    return measures


def dashboard(results: 'Results'):

    _CACHE = {
        'columns': {},
        'measures': {},
        'tables': {},
    }

    app = dash.Dash("Result Dashboard", external_stylesheets=[
        _EXTERNAL_STYLESHEETS, dbc.themes.BOOTSTRAP
    ], suppress_callback_exceptions=True)

    _TAB_FILES = dbc.Card(
        dbc.CardBody(
            [
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
        ),
    )

    _TAB_MEASURES = dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="graphs-measures",
            ),
        ),
    )

    _TAB_TABLE = dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="table",
            ),
        ),
    )

    _TABS = dbc.Tabs(
        [
            dbc.Tab(_TAB_FILES, label="Files", tab_id="tab-files"),
            dbc.Tab(_TAB_FILTERS, label="Filters", tab_id="tab-filters"),
            dbc.Tab(_TAB_COLUMNS, label="Columns", tab_id="tab-columns"),
            dbc.Tab(_TAB_MEASURES, label="Measures", tab_id="tab-measures"),
            dbc.Tab(_TAB_TABLE, label="Table", tab_id="tab-table"),
        ],
        id="tabs",
    )

    app.layout = html.Div(children=[
        html.H1(children='Result Dashboard'),
        _TABS,
    ], style={'padding': "1em"})

    def selection2hash(files: list, filters_all: list, filters_any: list) -> str:
        return str(hash(" ".join(files + filters_all + filters_any)))

    @app.callback(
        [
            Output("graphs-columns", "children"),
            Output("graphs-measures", "children"),
            Output("table", "children"),

        ],
        [
            Input("tabs", "active_tab"),
        ],
        [
            State("selected-files", "value"),
            State("selected-filters-all", "value"),
            State("selected-filters-any", "value"),
        ]
    )
    def switch_tab(at, files, filters_all, filters_any):
        cur_hash = selection2hash(files, filters_all, filters_any)
        if at == "tab-files":
            return ("", "", "")

        elif at == "tab-filters":
            return ("", "", "")

        elif at == "tab-columns":
            if cur_hash in _CACHE['columns']:
                return (_CACHE['columns'][cur_hash], "", "")
            else:
                figures = []
                for column in _COLUMNS[1:]:

                    files2plot = []
                    for file_ in files:
                        df = results.get_df(file_, filters_all, filters_any)
                        if df is not None and column in df.columns:
                            files2plot.append((file_, df))

                    prefix = path.commonprefix(
                        [file_ for file_, _ in files2plot])

                    fig = go.Figure(layout=_LAYOUT)

                    for file_, df in files2plot:
                        name = file_.replace(
                            prefix, "").replace(
                                _SIM_RESULT_FILENAME, "")
                        fig.add_trace(
                            go.Scatter(
                                x=df["date"],
                                y=df[column],
                                mode='lines',
                                name=name,
                            )
                        )

                    fig.update_layout(
                        title=column,
                        xaxis_title='day',
                        yaxis_title=column,
                        autosize=True,
                        # width=1920,
                        height=800,
                    )

                    figures.append(dcc.Graph(figure=fig))
                    figures.append(html.Hr())

                _CACHE['columns'][cur_hash] = figures
                return (figures, "", "")

        elif at == "tab-measures":
            if cur_hash in _CACHE['measures']:
                return ("", _CACHE['measures'][cur_hash], "")
            else:
                figures = []
                for measure, function in sorted(
                        _MEASURES.items(), key=lambda elm: elm[0]
                ):

                    files2plot = []
                    for file_ in files:
                        df = results.get_df(file_, filters_all, filters_any)
                        if df is not None:
                            files2plot.append((file_, df))

                    prefix = path.commonprefix(
                        [file_ for file_, _ in files2plot])

                    fig = go.Figure(layout=_LAYOUT)

                    for file_, df in files2plot:
                        name = file_.replace(
                            prefix, "").replace(
                                _SIM_RESULT_FILENAME, "")
                        fig.add_trace(
                            go.Scatter(
                                x=df["date"],
                                y=function(df),
                                mode='lines',
                                name=name,
                            )
                        )

                    fig.update_layout(
                        title=measure,
                        xaxis_title='day',
                        yaxis_title=measure,
                        autosize=True,
                        # width=1920,
                        height=800,
                    )

                    figures.append(dcc.Graph(figure=fig))
                    figures.append(html.Hr())

                _CACHE['measures'][cur_hash] = figures
                return ("", figures, "")

        elif at == "tab-table":
            if cur_hash in _CACHE['tables']:
                return ("", "", _CACHE['tables'][cur_hash])
            else:
                table = []
                files2plot = []
                for file_ in files:
                    df = results.get_df(file_, filters_all, filters_any)
                    if df is not None:
                        files2plot.append((file_, df))

                prefix = path.commonprefix([file_ for file_, _ in files2plot])

                for file_, df in files2plot:
                    values = _get_measures(file_, df)
                    values[0] = values[0].replace(
                        prefix, "").replace(
                        _SIM_RESULT_FILENAME, "")
                    table.append(values)

                df = pd.DataFrame(
                    table,
                    columns=[
                        "file", "Throughput", "Cost", "Bandwidth",
                        "Avg. Free Space", "Std. Dev. Free Space",
                        "Hit rate", "CPU Eff."
                    ]
                )
                df = df.sort_values(
                    by=["Throughput", "Cost", "Hit rate"],
                    ascending=[False, True, False],
                )
                df = df.round(2)

                table = dbc.Table.from_dataframe(
                    df, striped=True, bordered=True, hover=True
                )

                _CACHE['tables'][cur_hash] = table
                return ("", "", table)
        else:
            return ("", "", "")

    app.run_server(debug=True)
