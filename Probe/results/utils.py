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
            if len(cur_elm.components.intersection(set(filters))) != 0:
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

    _TAB_MEASURES = dbc.Card(
        dbc.CardBody(
            [
                html.H1("MEASURES"),
            ]
        ),
    )

    _TAB_TABLE = dbc.Card(
        dbc.CardBody(
            id="table",
        ),
    )

    _TAB_COLUMNS = dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="graphs",
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

    @app.callback(
       [
           Output("graphs", "children"),
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
            return "", ""
        elif at == "tab-filters":
            return "", ""
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
                )

                figures.append(dcc.Graph(figure=fig))

            return figures, ""
        elif at == "tab-measures":
            return "", ""
        elif at == "tab-table":
            return "", ""

    app.layout = html.Div(children=[
        html.H1(children='Result Dashboard'),
        _TABS
    ], style={'padding': "1em"})

    app.run_server(debug=True)
