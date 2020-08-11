import pathlib

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

_EXTERNAL_STYLESHEETS = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

_COLUMNS = ['date', 'num req', 'num hit', 'num added', 'num deleted',
            'num redirected', 'size', 'hit rate', 'hit over miss',
            'weighted hit rate', 'written data', 'read data', 'read on hit data',
            'read on miss data', 'deleted data', 'avg free space',
            'std dev free space', 'CPU efficiency', 'CPU hit efficiency',
            'CPU miss efficiency', 'CPU efficiency upper bound',
            'CPU efficiency lower bound', 'Addition epsilon', 'Eviction epsilon',
            'Addition qvalue function', 'Eviction qvalue function',
            'Eviction calls', 'Eviction forced calls', 'Eviction step',
            'Action store', 'Action not store', 'Action delete all',
            'Action delete half', 'Action delete quarter', 'Action delete one',
            'Action not delete']

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

    def get_df(self, file_: str, filters: list):
        cur_elm = self._elemts[file_]
        if len(filters) != 0:
            if len(cur_elm.components.intersection(set(filters))) == len(filters):
                return cur_elm.df
            else:
                return None
        else:
            return cur_elm.df


def aggregate_results(folder: str):
    abs_target_folder = pathlib.Path(folder).resolve()
    results = Results()
    all_columns = set(_COLUMNS)
    for result_path in list(abs_target_folder.glob("**/*_results.csv")):
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


def _get_cache_size(cache_filename):
    if cache_filename.find("T_") != -1:
        cache_size = float(cache_filename.split("T_")
                           [0].rsplit("_", 1)[-1])
        return float(cache_size * 1024**2)
    elif cache_filename.find("G_") != -1:
        cache_size = float(cache_filename.split("G_")
                           [0].rsplit("_", 1)[-1])
        return float(cache_size * 1024)
    elif cache_filename.find("M_") != -1:
        cache_size = float(cache_filename.split("M_")
                           [0].rsplit("_", 1)[-1])
        return float(cache_size * 1024)
    else:
        raise Exception(
            f"Error: '{cache_filename}' cache name with unspecified size...")


def _measure_throughput(df: 'pd.DataFrame') -> 'pd.Series':
    return df['read on hit data'] - df['written data']


def _measure_cost(df: 'pd.DataFrame') -> 'pd.Series':
    return df['written data'] + df['deleted data']


_MEASURES = {
    'Throughput': _measure_throughput,
    'Cost': _measure_cost,
}


def _get_measures(cache_filename: str, df: 'pd.DataFrame') -> list:
    measures = [cache_filename]

    cache_size = _get_cache_size(pathlib.Path(cache_filename).stem)

    # Throughput
    measures.append(
        (_measure_throughput(df).mean() / cache_size) * 100.
    )

    # Cost
    measures.append(
        (_measure_cost(df).mean() / cache_size) * 100.
    )

    return measures


def dashboard(results: 'Results'):

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
                dcc.Checklist(
                    options=[
                        {'label': f" {component}", 'value': component}
                        for component in results.components
                    ],
                    value=[],
                    labelStyle={'display': 'block'},
                    id="selected-filters",
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

    @ app.callback(
        [
            Output("graphs-columns", "children"),
            Output("graphs-measures", "children"),
            Output("table", "children"),
        ],
        [Input("tabs", "active_tab")],
        [
            State("selected-files", "value"),
            State("selected-filters", "value"),
        ]
    )
    def switch_tab(at, files, filters):
        if at == "tab-files":
            return "", "", ""
        elif at == "tab-filters":
            return "", "", ""
        elif at == "tab-columns":
            figures = []
            for column in _COLUMNS[1:]:

                fig = go.Figure(layout=_LAYOUT)

                for file_ in files:
                    df = results.get_df(file_, filters)
                    if df is not None and column in df.columns:
                        fig.add_trace(
                            go.Scatter(
                                x=df["date"],
                                y=df[column],
                                mode='lines',
                                name=file_,
                            )
                        )

                fig.update_layout(
                    title=column,
                    xaxis_title='day',
                    yaxis_title=column,
                    autosize=False,
                    width=1920,
                    height=800,
                )

                figures.append(dcc.Graph(figure=fig))
                figures.append(html.Hr())

            return figures, "", ""
        elif at == "tab-measures":
            figures = []
            for name, function in _MEASURES.items():

                fig = go.Figure(layout=_LAYOUT)

                for file_ in files:
                    df = results.get_df(file_, filters)
                    if df is not None:
                        fig.add_trace(
                            go.Scatter(
                                x=df["date"],
                                y=function(df),
                                mode='lines',
                                name=file_,
                            )
                        )

                fig.update_layout(
                    title=name,
                    xaxis_title='day',
                    yaxis_title=name,
                    autosize=False,
                    width=1920,
                    height=800,
                )

                figures.append(dcc.Graph(figure=fig))
                figures.append(html.Hr())

            return "", figures, ""
        elif at == "tab-table":
            table = []
            for file_ in files:
                df = results.get_df(file_, filters)
                if df is not None:
                    table.append(_get_measures(file_, df))
            df = pd.DataFrame(
                table,
                columns=[
                    "file", "Throughput", "Cost",
                ]
            )
            df = df.sort_values(
                by=["Throughput", "Cost"],
                ascending=[False, True]
            )

            table = dbc.Table.from_dataframe(
                df, striped=True, bordered=True, hover=True
            )
            return "", "", table
        return "", "", ""

    app.layout = html.Div(children=[
        html.H1(children='Result Dashboard'),
        _TABS
    ], style={'padding': "1em"})

    app.run_server(debug=True)
