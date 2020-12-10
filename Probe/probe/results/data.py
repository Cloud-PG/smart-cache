import json
import pathlib
import re
from typing import Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

from ..utils import STATUS_ARROW
from .utils import parse_sim_log

_ALGORITHMS = ["lru", "lfu", "sizeSmall", "sizeBig"]

SIM_RESULT_FILENAME = "simulation_results.csv"
SIM_CHOICE_LOG_FILE = "simulationLogFile.csv.gz"

COLUMNS = [
    "date",
    "num req",
    "num hit",
    "num added",
    "num deleted",
    "num redirected",
    "size redirected",
    "cache size",
    "num miss after delete",
    "size",
    "capacity",
    "bandwidth",
    "bandwidth usage",
    "hit rate",
    "weighted hit rate",
    "written data",
    "read data",
    "read on hit data",
    "read on miss data",
    "deleted data",
    "avg free space",
    "std dev free space",
    "CPU efficiency",
    "CPU hit efficiency",
    "CPU miss efficiency",
    "CPU efficiency upper bound",
    "CPU efficiency lower bound",
    "Addition epsilon",
    "Eviction epsilon",
    "Addition qvalue function",
    "Eviction qvalue function",
    "Eviction calls",
    "Eviction forced calls",
    "Eviction mean num categories",
    "Eviction std dev num categories",
    "Action store",
    "Action not store",
    "Action delete all",
    "Action delete half",
    "Action delete quarter",
    "Action delete one",
    "Action not delete",
]


class Element(object):
    def __init__(self, components: list, filename: str, df: "pd.DataFrame"):
        self._df = df
        self._filename = filename
        self._components = set([elm for elm in self.__parse_components(components)])

    @staticmethod
    def __parse_components(components: list) -> list:
        for component in components:
            if component.find("weightFunLRU") != -1:
                yield component.rsplit("_", 3)[0]
            elif component.find("_") != -1 and component.split("_")[0] in _ALGORITHMS:
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

    def insert(
        self,
        path: "pathlib.Path",
        components: list,
        filename: str,
        df: "pd.DataFrame",
        choices: "pd.DataFrame" = None,
    ) -> "Results":
        elm = Element(components, filename, df)
        self._elemets[path.as_posix()] = elm
        self._choices[path.as_posix()] = choices
        return self

    @property
    def components(self) -> set:
        components = set()
        for elm in self._elemets.values():
            components |= elm.components
        return sorted(components)

    @property
    def files(self) -> "list[str]":
        return list(sorted(self._elemets.keys()))

    def get_all(self):
        for file_, df in self._elemets.items():
            yield file_, df, self.get_log(file_, [], [])

    def get_all_df(self):
        for file_, df in self._elemets.items():
            yield file_, df

    def get_df(
        self, file_: str, filters_all: list, filters_any: list
    ) -> "pd.DataFrame":
        cur_elm = self._elemets[file_]
        all_ = (
            len(cur_elm.components.intersection(set(filters_all))) == len(filters_all)
            if len(filters_all) > 0
            else True
        )
        any_ = (
            len(cur_elm.components.intersection(set(filters_any))) != 0
            if len(filters_any) > 0
            else True
        )
        if all_ and any_:
            return cur_elm.df
        return None

    def get_log(
        self, file_: str, filters_all: list, filters_any: list
    ) -> "pd.DataFrame":
        cur_elm = self._elemets[file_]
        all_ = (
            len(cur_elm.components.intersection(set(filters_all))) == len(filters_all)
            if len(filters_all) > 0
            else True
        )
        any_ = (
            len(cur_elm.components.intersection(set(filters_any))) != 0
            if len(filters_any) > 0
            else True
        )
        if all_ and any_:
            tmp = self._choices[file_]
            if isinstance(tmp, pathlib.Path):
                print(f"{STATUS_ARROW}LAZY LOADING OF {tmp}")
                tmp = pd.read_csv(tmp)
                self._choices[file_] = tmp
                print(f"{STATUS_ARROW}LAZY LOAD DONE...")
            return tmp


def aggregate_results(folders: list) -> "Results":
    results = Results()

    for folder in folders:
        abs_target_folder = pathlib.Path(folder).resolve().parent
        all_columns = set(COLUMNS)
        for result_path in tqdm(
            list(pathlib.Path(folder).glob(f"**/{SIM_RESULT_FILENAME}")),
            desc=f"Opening results",
        ):
            df = pd.read_csv(result_path)
            cur_columns = set(df.columns)
            if not cur_columns.issubset(all_columns):
                print("Warning: not all columns are present")

            df["date"] = pd.to_datetime(
                df["date"].apply(lambda elm: elm.split()[0]), format="%Y-%m-%d"
            )
            relative_path = result_path.resolve().relative_to(abs_target_folder)
            *components, filename = relative_path.parts
            # Check choices
            choice_file = result_path.parent.joinpath(SIM_CHOICE_LOG_FILE)
            if choice_file.exists():
                choices = choice_file
            else:
                choices = None
            results.insert(relative_path, components, filename, df, choices)
    return results


def missing_column(func):
    def wrapper(df: "pd.DataFrame"):
        try:
            return func(df)
        except KeyError:
            return pd.Series(np.zeros(len(df.index)))

    return wrapper


@missing_column
def measure_throughput_ratio(df: "pd.DataFrame") -> "pd.Series":
    return (df["read on hit data"] - df["written data"]) / df["cache size"]


@missing_column
def measure_cost_ratio(df: "pd.DataFrame") -> "pd.Series":
    return (df["written data"] + df["deleted data"]) / df["cache size"]


@missing_column
def measure_throughput(df: "pd.DataFrame") -> "pd.Series":
    # to Terabytes
    return (df["read on hit data"] - df["written data"]) / (1024.0 ** 2.0)


@missing_column
def measure_cost(df: "pd.DataFrame") -> "pd.Series":
    # to Terabytes
    return (df["written data"] + df["deleted data"]) / (1024.0 ** 2.0)


@missing_column
def measure_read_on_hit_ratio(df: "pd.DataFrame") -> "pd.Series":
    return (df["read on hit data"] / df["read data"]) * 100.0


@missing_column
def measure_cpu_eff(df: "pd.DataFrame") -> "pd.Series":
    return df["CPU efficiency"]


@missing_column
def measure_avg_free_space(df: "pd.DataFrame") -> "pd.Series":
    cache_size = df["cache size"][0]
    return (df["avg free space"] / cache_size) * 100.0


@missing_column
def measure_std_dev_free_space(df: "pd.DataFrame") -> "pd.Series":
    cache_size = df["cache size"][0]
    return (df["std dev free space"] / cache_size) * 100.0


@missing_column
def measure_bandwidth(df: "pd.DataFrame") -> "pd.Series":
    return (df["read on miss data"] / df["bandwidth"]) * 100.0


@missing_column
def measure_redirect_volume(df: "pd.DataFrame") -> "pd.Series":
    cache_size = df["cache size"][0]
    return (df["size redirected"] / cache_size) * 100.0


@missing_column
def measure_num_miss_after_delete(df: "pd.DataFrame") -> "pd.Series":
    return df["num miss after delete"]


@missing_column
def measure_hit_rate(df: "pd.DataFrame") -> "pd.Series":
    return df["hit rate"]


@missing_column
def measure_hit_over_miss(df: "pd.DataFrame") -> "pd.Series":
    return df["read on hit data"] / df["read on miss data"]


def parse_simulation_report(
    files2plot: list, prefix: str, generator: bool = False, target: str = "AFTERDELETE"
) -> dict:
    del_evaluators = {}

    for file_, _, sim_log in tqdm(files2plot, desc="Parse log", position=1):
        name = file_.replace(prefix, "").replace(f"/{SIM_RESULT_FILENAME}", "")

        curEvents = []

        for del_evaluator in parse_sim_log(sim_log, target):
            if generator:
                yield name, del_evaluator
            else:
                curEvents.append(del_evaluator)

        if not generator:
            del_evaluators[name] = curEvents

    return del_evaluators


def get_name_no_feature(name: str):
    if name.find("no_") != -1 and name.find("_feature") != -1:
        feature = name.split("no_")[1].split("_feature")[0].replace("_", "")
        return f"[No {feature} feature] "


def make_table(
    files2plot: list, prefix: str, top_n: int = 0, extended: bool = False
) -> Tuple["pd.DataFrame", list]:
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
        values = get_measures(file_, df, extended=extended)
        values[0] = values[0].replace(prefix, "").replace(f"/{SIM_RESULT_FILENAME}", "")
        values[0] = values[0].replace("run_full_normal/", "")

        search_size = re.search("[\/]?[0-9]*T\/", values[0])
        if search_size != None:
            values[0] = values[0].replace(
                search_size.group(),
                "",
                # f"{search_size.group().replace('/', '')} - "
            )

        search_no_feature = re.search("[\_]no\_[a-zA-Z]*\_feature", values[0])
        if search_no_feature != None:
            values[0] = values[0].replace(
                search_no_feature.group(),
                get_name_no_feature(values[0]),
                # f"{search_size.group().replace('/', '')} - "
            )

        values.insert(0, file_)
        table.append(values)

    if extended:
        columns = [
            "source",
            "file",
            "Throughput",
            "Cost",
            "Throughput (TB)",
            "Cost (TB)",
            "Read on hit ratio",
            "Bandwidth",
            "Redirect Vol.",
            "Avg. Free Space",
            "Std. Dev. Free Space",
            "Hit over Miss",
            "Num. miss after del.",
            "Hit rate",
            "CPU Eff.",
        ]
    else:
        columns = [
            "source",
            "file",
            "Throughput",
            "Cost",
            "Read on hit ratio",
            "Bandwidth",
            "Num. miss after del.",
            "CPU Eff.",
        ]
    df = pd.DataFrame(table, columns=columns)
    df = df.sort_values(
        by=[
            "Throughput",
            "Cost",
            "Read on hit ratio",
            "Num. miss after del.",
        ],
        ascending=[False, True, False, False],
    )
    df = df.round(6)
    if top_n != 0:
        df = df.iloc[:10]

    source_files = df.source.to_list()
    del df["source"]

    return df, source_files


def get_measures(
    cache_filename: str, df: "pd.DataFrame", extended: bool = False
) -> list:
    measures = [cache_filename]
    # print(cache_filename)

    # Throughput ratio
    measures.append(measure_throughput_ratio(df).mean())

    # Cost ratio
    measures.append(measure_cost_ratio(df).mean())

    if extended:
        # Throughput (TB)
        measures.append(measure_throughput(df).mean())

        # Cost (TB)
        measures.append(measure_cost(df).mean())

    # Read on hit ratio
    measures.append(measure_read_on_hit_ratio(df).mean())

    # Bandwidth
    measures.append(measure_bandwidth(df).mean())

    if extended:
        # Redirect Vol.
        measures.append(measure_redirect_volume(df).mean())

        # Avg. Free Space
        measures.append(measure_avg_free_space(df).mean())

        # Std. Dev. Free Space
        measures.append(measure_std_dev_free_space(df).mean())

        # Hit over Miss
        measures.append(measure_hit_over_miss(df).mean())

    # Num. miss after delete
    measures.append(measure_num_miss_after_delete(df).mean())

    if extended:
        # Hit rate
        measures.append(measure_hit_rate(df).mean())

    # CPU Efficiency
    measures.append(measure_cpu_eff(df).mean())

    return measures


def get_all_metric_values(results: list) -> "pd.DataFrame":
    throughput = []
    cost = []
    read_on_hit_ratio = []
    bandwidth = []
    num_miss_after_del = []
    cpu_eff = []

    for file_, elm in results:
        throughput += measure_throughput_ratio(elm.df).to_list()
        cost += measure_cost_ratio(elm.df).to_list()
        read_on_hit_ratio += measure_read_on_hit_ratio(elm.df).to_list()
        bandwidth += measure_bandwidth(elm.df).to_list()
        num_miss_after_del += measure_num_miss_after_delete(elm.df).to_list()
        cpu_eff += measure_cpu_eff(elm.df).to_list()

    df = pd.DataFrame(
        {
            "Throughput": throughput,
            "Cost": cost,
            "Read on hit ratio": read_on_hit_ratio,
            "Bandwidth": bandwidth,
            "Num. miss after del.": num_miss_after_del,
            "CPU efficiency": cpu_eff,
        }
    )

    return df
