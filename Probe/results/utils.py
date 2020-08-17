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
from tqdm import tqdm

_SIM_RESULT_FILENAME = "/simulation_results.csv"

_EXTERNAL_STYLESHEETS = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

_COLUMNS = [
    'date',
    'num req',
    'num hit',
    'num added',
    'num deleted',
    'num redirected',
    'size redirected',
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

_ALGORITHMS = ['lru', 'lfu', 'sizeSmall', 'sizeBig']


class Element(object):

    def __init__(self, components: list, filename: str, df: 'pd.DataFrame'):
        self._df = df
        self._filename = filename
        self._components = set([
            elm for elm in self.__parse_components(components)
        ])

    @staticmethod
    def __parse_components(components: list) -> list:
        for component in components:
            if component.find("weightFunLRU") != -1:
                yield component.rsplit("_", 3)[0]
            elif component.find("_") != -1 and \
                    component.split("_")[0] in _ALGORITHMS:
                yield component.split("_")[0]
            elif component.find("aiRL") != -1:
                cur_type = component.split("aiRL_")[1].split("_", 1)[0]
                if cur_type == "SCDL":
                    yield "SCDL"
                elif cur_type == "SCDL2":
                    yield "SCDL2"
                yield "aiRL"
            else:
                yield component

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
    for result_path in tqdm(list(
        abs_target_folder.glob("**/simulation_results.csv")
    ), desc="Opening results"):
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
    cache_size = df['cache size'][0]
    return (df['read on hit data'] - df['written data'])/cache_size


def _measure_cost(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['written data'] + df['deleted data'])/cache_size


def _measure_cpu_eff(df: 'pd.DataFrame') -> 'pd.Series':
    return df['CPU efficiency']


def _measure_avg_free_space(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['avg free space'] / cache_size) * 100.


def _measure_std_dev_free_space(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['std dev free space'] / cache_size) * 100.


def _measure_bandwidth(df: 'pd.DataFrame') -> 'pd.Series':
    return (df['read on miss data'] / df['bandwidth']) * 100.


def _measure_redirect_volume(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['size redirected'] / cache_size) * 100.


def _measure_hit_rate(df: 'pd.DataFrame') -> 'pd.Series':
    return df['hit rate']


def _agent_epsilon(df: 'pd.DataFrame') -> 'pd.Series':
    return df['']


_MEASURES = {
    'Throughput': _measure_throughput,
    'Cost': _measure_cost,
    'CPU Eff.': _measure_cpu_eff,
    'Avg. Free Space': _measure_avg_free_space,
    'Std. Dev. Free Space': _measure_std_dev_free_space,
    'Bandwidth': _measure_bandwidth,
    'Redirect Vol.': _measure_redirect_volume,
    'Hit rate': _measure_hit_rate,
}


def _get_measures(cache_filename: str, df: 'pd.DataFrame') -> list:
    measures = [cache_filename]

    # Throughput
    measures.append(
        _measure_throughput(df).mean()
    )

    # Cost
    measures.append(
        _measure_cost(df).mean()
    )

    # Bandwidth
    measures.append(
        _measure_bandwidth(df).mean()
    )

    # Redirect Vol.
    measures.append(
        _measure_redirect_volume(df).mean()
    )

    # Avg. Free Space
    measures.append(
        _measure_avg_free_space(df).mean()
    )

    # Std. Dev. Free Space
    measures.append(
        _measure_std_dev_free_space(df).mean()
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


def get_files2plot(results: 'Results', files: list, filters_all: list,
                   filters_any: list, column: str = "",
                   agents: bool = False,) -> list:
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
                    files2plot.append((file_, df))
            elif agents:
                if "Addition epsilon" in df.columns:
                    files2plot.append((file_, df))
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
    return path.commonprefix([file_ for file_, _ in files2plot])


def dashboard(results: 'Results'):

    _CACHE = {
        'columns': {},
        'measures': {},
        'agents': {},
        'tables': {},
    }

    app = dash.Dash("Result Dashboard", external_stylesheets=[
        _EXTERNAL_STYLESHEETS, dbc.themes.BOOTSTRAP
    ], suppress_callback_exceptions=True)

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
        ),
    )

    _TAB_MEASURES = dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="graphs-measures",
            ),
        ),
    )

    _TAB_AGENTS = dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="graphs-agents",
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
            dbc.Tab(_TAB_AGENTS, label="Agents", tab_id="tab-agents"),
            dbc.Tab(_TAB_TABLE, label="Table", tab_id="tab-table"),
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
            return ("", "", "", "")

        elif at == "tab-filters":
            return ("", "", "", "")

        elif at == "tab-columns":
            if cur_hash in _CACHE['columns']:
                return (_CACHE['columns'][cur_hash], "", "", "")
            else:
                figures = []
                for column in _COLUMNS[1:]:
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
                return (figures, "", "", "")

        elif at == "tab-measures":
            if cur_hash in _CACHE['measures']:
                return ("", _CACHE['measures'][cur_hash], "", "")
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
                            function=function
                        )
                    ))
                    figures.append(html.Hr())

                _CACHE['measures'][cur_hash] = figures
                return ("", figures, "", "")

        elif at == "tab-agents":
            if cur_hash in _CACHE['agents']:
                return ("", "", _CACHE['agents'][cur_hash], "")
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
                return ("", "", figures, "")

        elif at == "tab-table":
            if cur_hash in _CACHE['tables']:
                return ("", "", "", _CACHE['tables'][cur_hash])
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

                table = dbc.Table.from_dataframe(
                    table, striped=True, bordered=True, hover=True
                )
                _CACHE['tables'][cur_hash] = table
                return ("", "", "", table)
        else:
            return ("", "", "", "")

    app.run_server(debug=True)


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
        "Addition actions": ['Action store', 'Action not store'],
        "Eviction actions": ['Action delete all', 'Action delete half', 'Action delete quarter', 'Action delete one', 'Action not delete'],
    }
    for plot, columns in _AGENT_COLUMNS.items():
        fig_epsilon = go.Figure(layout=_LAYOUT)
        for file_, df in files2plot:
            name = file_.replace(
                prefix, "").replace(
                    _SIM_RESULT_FILENAME, "")
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


def make_line_figures(files2plot: list, prefix: str, title: str,
                      function: callable = None, column: str = ""
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
    fig = go.Figure(layout=_LAYOUT)
    for file_, df in files2plot:
        name = file_.replace(
            prefix, "").replace(
                _SIM_RESULT_FILENAME, "")
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
        f"{prefix}{filename}{_SIM_RESULT_FILENAME}"
        for filename in df[:n].file.to_list()
    ]


def make_table(files2plot: list, prefix: str) -> 'pd.DataFrame':
    """Make html table from files to plot

    :param files2plot: list of files to plot with their dataframes
    :type files2plot: list
    :param prefix: the files' prefix
    :type prefix: str
    :return: html table component
    :rtype: dbc.Table
    """
    table = []
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
            "Redirect Vol.", "Avg. Free Space", 
            "Std. Dev. Free Space", "Hit rate", "CPU Eff."
        ]
    )
    df = df.sort_values(
        by=["Throughput", "Cost", "Hit rate"],
        ascending=[False, True, False],
    )
    df = df.round(2)
    return df
