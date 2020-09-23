import pathlib

import pandas as pd
import plotly.express as px
from tqdm import tqdm

from ..utils import STATUS_ARROW

_ALGORITHMS = ['lru', 'lfu', 'sizeSmall', 'sizeBig']

SIM_RESULT_FILENAME = "simulation_results.csv"
SIM_CHOICE_LOG_FILE = "simulationLogFile.csv.gz"

COLUMNS = [
    'date',
    'num req',
    'num hit',
    'num added',
    'num deleted',
    'num redirected',
    'size redirected',
    'cache size',
    'num miss after delete',
    'size',
    'capacity',
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
    'Eviction mean num categories',
    'Eviction std dev num categories',
    'Action store',
    'Action not store',
    'Action delete all',
    'Action delete half',
    'Action delete quarter',
    'Action delete one',
    'Action not delete',
]


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
                elif cur_type.find("SCDL2") != -1 and cur_type.index("SCDL2") == 0:
                    yield cur_type
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
        self._elemets = {}
        self._choices = {}

    def __len__(self) -> int:
        return len(self._elemets)

    def insert(self, path: 'pathlib.Path', components: list, filename: str,
               df: 'pd.DataFrame', choices: 'pd.DataFrame' = None) -> 'Results':
        elm = Element(components, filename,  df)
        self._elemets[path.as_posix()] = elm
        self._choices[path.as_posix()] = choices
        return self

    @ property
    def components(self) -> set:
        components = set()
        for elm in self._elemets.values():
            components |= elm.components
        return sorted(components)

    @ property
    def files(self) -> 'list[str]':
        return list(sorted(self._elemets.keys()))

    def get_all(self):
        for file_, df in self._elemets.items():
            yield file_, df, self.get_choices(file_, [], [])

    def get_df(self, file_: str, filters_all: list, filters_any: list) -> 'pd.DataFrame':
        cur_elm = self._elemets[file_]
        all_ = len(cur_elm.components.intersection(set(filters_all))) == len(
            filters_all) if len(filters_all) > 0 else True
        any_ = len(cur_elm.components.intersection(set(filters_any))
                   ) != 0 if len(filters_any) > 0 else True
        if all_ and any_:
            return cur_elm.df
        return None

    def get_choices(self, file_: str, filters_all: list, filters_any: list) -> 'pd.DataFrame':
        cur_elm = self._elemets[file_]
        all_ = len(cur_elm.components.intersection(set(filters_all))) == len(
            filters_all) if len(filters_all) > 0 else True
        any_ = len(cur_elm.components.intersection(set(filters_any))
                   ) != 0 if len(filters_any) > 0 else True
        if all_ and any_:
            tmp = self._choices[file_]
            if isinstance(tmp, pathlib.Path):
                print(f"{STATUS_ARROW}LAZY LOADING OF {tmp}")
                tmp = pd.read_csv(tmp)
                self._choices[file_] = tmp
                print(f"{STATUS_ARROW}LAZY LOAD DONE...")
            return tmp


def aggregate_results(folder: str) -> 'Results':
    abs_target_folder = pathlib.Path(folder).resolve()
    results = Results()
    all_columns = set(COLUMNS)
    for result_path in tqdm(list(
        abs_target_folder.glob(f"**/{SIM_RESULT_FILENAME}")
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
            * components, filename = relative_path.parts
            # Check choices
            choice_file = result_path.parent.joinpath(SIM_CHOICE_LOG_FILE)
            if choice_file.exists():
                choices = choice_file
            else:
                choices = None
            results.insert(relative_path, components, filename, df, choices)
    return results


def measure_throughput_ratio(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['read on hit data'] - df['written data'])/cache_size


def measure_cost_ratio(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['written data'] + df['deleted data'])/cache_size


def measure_throughput(df: 'pd.DataFrame') -> 'pd.Series':
    # to Terabytes
    return (df['read on hit data'] - df['written data'])/(1024.**2.)


def measure_cost(df: 'pd.DataFrame') -> 'pd.Series':
    # to Terabytes
    return (df['written data'] + df['deleted data'])/(1024.**2.)


def measure_read_on_hit_ratio(df: 'pd.DataFrame') -> 'pd.Series':
    return (df['read on hit data']/df['read data']) * 100.


def measure_cpu_eff(df: 'pd.DataFrame') -> 'pd.Series':
    return df['CPU efficiency']


def measure_avg_free_space(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['avg free space'] / cache_size) * 100.


def measure_std_dev_free_space(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['std dev free space'] / cache_size) * 100.


def measure_bandwidth(df: 'pd.DataFrame') -> 'pd.Series':
    return (df['read on miss data'] / df['bandwidth']) * 100.


def measure_redirect_volume(df: 'pd.DataFrame') -> 'pd.Series':
    cache_size = df['cache size'][0]
    return (df['size redirected'] / cache_size) * 100.


def measure_num_miss_after_delete(df: 'pd.DataFrame') -> 'pd.Series':
    return df['num miss after delete']


def measure_hit_rate(df: 'pd.DataFrame') -> 'pd.Series':
    return df['hit rate']


def measure_hit_over_miss(df: 'pd.DataFrame') -> 'pd.Series':
    return df['read on hit data'] / df['read on miss data']


class LogDeleteEvaluator(object):

    def __init__(self, event: tuple):
        self._event = event
        self.actions = []
        self.after = []
        self.after4scatter = None

        self.figs = None

        self.tick = self._event[1]
        self.event = self._event[2]
        self.num_deleted_files = -1
        self.total_size_deleted_files = -1.
        self.total_num_req_after_delete = -1

        self.on_delete_cache_size = self._event[3]
        self.on_delete_cache_occupancy = self._event[4]

    def add(self, action: tuple):
        self.actions.append(action)

    def trace(self, after_action: tuple):
        self.after.append(after_action)

    def prepare(self, columns):
        self.actions = pd.DataFrame(self.actions, columns=columns)
        self.actions.set_index('Index', inplace=True)
        self.actions.reset_index(inplace=True, drop=True)
        self.after = pd.DataFrame(self.after, columns=columns)
        self.after.set_index('Index', inplace=True)
        self.after.reset_index(inplace=True, drop=True)
        self.after4scatter = self.after.copy()
        self.after4scatter = self.after4scatter.loc[self.after4scatter['action or event'] == "ADD"]

        self._fix_delta_t_max()

        self.num_deleted_files = self._get_num_deleted_files()
        self.total_size_deleted_files = self._get_total_size_deleted_files()
        self.total_num_req_after_delete = self._get_num_deleted_miss()

    @property
    def scatterActions(self):
        return px.scatter_3d(
            self.actions,
            x='num req',
            y='size',
            z='filename',
            color='delta t',
            size='size',
            opacity=0.9,
        )

    @property
    def scatterAfter(self):
        return px.scatter_3d(
            self.after[self.after.size != -1.],
            x='num req',
            y='size',
            z='filename',
            color='delta t',
            size='size',
            opacity=0.9,
        )

    @property
    def histActionNumReq(self):
        return px.histogram(self.actions, x='num req')

    @property
    def histActionSize(self):
        return px.histogram(self.actions, x='Size')

    @property
    def histActionDeltaT(self):
        return px.histogram(self.actions, x='delta t')

    def _get_num_deleted_files(self):
        return len(set(self.actions.filename))

    def _get_total_size_deleted_files(self):
        return self.actions.size.sum()

    def _get_num_deleted_miss(self):
        files = set(self.after.filename) & set(self.actions.filename)
        tot = 0
        if len(files) > 0:
            counts = self.after.filename[self.after['action or event'] == "MISS"].value_counts(
            )
            tot = sum(counts[file_] for file_ in files if file_ in counts)
        return tot

    def _fix_delta_t_max(self):
        cur_max = self.actions['delta t'].max()
        selectRows = self.actions['delta t'] == cur_max
        self.actions.loc[selectRows, 'delta t'] = -1.
        new_max = self.actions['delta t'].max()
        selectRows = self.actions['delta t'] == -1.
        self.actions.loc[selectRows, 'delta t'] = new_max * 2.

        cur_max = self.after['delta t'].max()
        selectRows = (self.after['delta t'] == cur_max) & (
            self.after.size != -1.)
        self.after.loc[selectRows, 'delta t'] = -1.
        new_max = self.after['delta t'].max()
        selectRows = (self.after['delta t'] == -1.) & (self.after.size != -1.)
        self.after.loc[selectRows, 'delta t'] = new_max * 2.


def parse_simulation_report(files2plot: list, prefix: str, generator: bool = False) -> dict:
    del_evaluator = {}

    for file_, _, choices in tqdm(files2plot, desc="Parse log", position=1):
        name = file_.replace(
            prefix,
            ""
        ).replace(
            f"/{SIM_RESULT_FILENAME}",
            ""
        )

        curEvents = []
        curLog = None
        state = "AFTERDELETE"

        # print(file_, choices)

        # choices = choices[:1000000]
        for row in tqdm(choices.itertuples(), desc=f"Parse {name}",
                        total=len(choices.index), position=2):
            event = row[2]
            if state == "AFTERDELETE":
                if event in ["ONFREE", "ONDAYEND", "ONK", "FORCEDCALL", "FREE"]:
                    if curLog is not None:
                        curLog.prepare(['Index'] + list(choices.columns))
                        if not generator:
                            curEvents.append(curLog)
                        else:
                            yield name, curLog
                    curLog = LogDeleteEvaluator(row)
                    state = "DELETING"
                elif curLog is not None:
                    curLog.trace(row)
            elif state == "DELETING":
                if event in ["KEEP", "DELETE"]:
                    curLog.add(row)
                else:
                    state = "AFTERDELETE"
                    curLog.trace(row)
        else:
            curLog.prepare(['Index'] + list(choices.columns))
            if not generator:
                curEvents.append(curLog)
            else:
                yield name, curLog

        if not generator:
            del_evaluator[name] = curEvents

    # print(del_evaluator)

    return del_evaluator


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
        values = get_measures(file_, df)
        values[0] = values[0].replace(
            prefix, "").replace(
            f"/{SIM_RESULT_FILENAME}", "")
        table.append(values)
    df = pd.DataFrame(
        table,
        columns=[
            "file", "Throughput ratio", "Cost ratio",
            "Throughput (TB)", "Cost (TB)",
            "Read on hit ratio", "Bandwidth",
            "Redirect Vol.", "Avg. Free Space",
            "Std. Dev. Free Space", "Hit over Miss",
            "Num. miss after del.", "Hit rate", "CPU Eff."
        ]
    )
    df = df.sort_values(
        by=["Throughput ratio", "Cost ratio", "Num. miss after del."],
        ascending=[False, True, False],
    )
    df = df.round(3)
    return df


def get_measures(cache_filename: str, df: 'pd.DataFrame') -> list:
    measures = [cache_filename]
    print(cache_filename)

    # Throughput ratio
    measures.append(
        measure_throughput_ratio(df).mean()
    )

    # Cost ratio
    measures.append(
        measure_cost_ratio(df).mean()
    )

    # Throughput (TB)
    measures.append(
        measure_throughput(df).mean()
    )

    # Cost (TB)
    measures.append(
        measure_cost(df).mean()
    )

    # Read on hit ratio
    measures.append(
        measure_read_on_hit_ratio(df).mean()
    )

    # Bandwidth
    measures.append(
        measure_bandwidth(df).mean()
    )

    # Redirect Vol.
    measures.append(
        measure_redirect_volume(df).mean()
    )

    # Avg. Free Space
    measures.append(
        measure_avg_free_space(df).mean()
    )

    # Std. Dev. Free Space
    measures.append(
        measure_std_dev_free_space(df).mean()
    )

    # Hit over Miss
    measures.append(
        measure_hit_over_miss(df).mean()
    )

    # Num. miss after delete
    measures.append(
        measure_num_miss_after_delete(df).mean()
    )

    # Hit rate
    measures.append(
        measure_hit_rate(df).mean()
    )

    # CPU Efficiency
    measures.append(
        measure_cpu_eff(df).mean()
    )

    return measures
