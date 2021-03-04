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
    "num miss after delete",
    "num free calls",
    "num over high watermark",
    "size redirected",
    "cache size",
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
    def __init__(
        self,
        components: list,
        filename: str,
        full_path: str,
        df: "pd.DataFrame",
        idx: int = -1,
        lazy: bool = True,
    ):
        self._df = None
        self._lazy = lazy
        self._idx = idx
        self._filename = filename
        self._full_path = full_path
        self._components = set([elm for elm in self.__parse_components(components)])

        if not self._lazy:
            self._df = df

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
    def full_path(self):
        return self._full_path

    @property
    def path(self):
        return pathlib.Path(self._full_path)

    @property
    def df(self):
        if self._df is None and self._lazy:
            print(f"[LAZY LOAD {self._idx}]->[{self.full_path}]", end="\r")
            df = pd.read_csv(self.full_path)

            if check_columns(df):
                print("Warning: not all columns are present")

            df = fix_date_column(df)

            self._df = df

        return self._df

    @property
    def components(self):
        return self._components

    def __hash__(self):
        return hash(str(self._components))


class Results(object):
    def __init__(self, lazy: bool = True):
        self._elemets = {}
        self._choices = {}
        self._lazy = lazy
        self._counter = 0

    def __len__(self) -> int:
        return len(self._elemets)

    def insert(
        self,
        path: "pathlib.Path",
        components: list,
        filename: str,
        full_path: str,
        df: "pd.DataFrame",
        choices: "pd.DataFrame" = None,
    ) -> "Results":
        elm = Element(
            components, filename, full_path, df, idx=self._counter, lazy=self._lazy
        )
        self._elemets[path.as_posix()] = elm
        self._choices[path.as_posix()] = choices
        self._counter += 1
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


def check_columns(df: "pd.DataFrame") -> bool:
    cur_columns = set(df.columns)
    return not cur_columns.issubset(set(COLUMNS))


def fix_date_column(df: "pd.DataFrame") -> "pd.DataFrame":
    df["date"] = pd.to_datetime(
        df["date"].apply(lambda elm: elm.split()[0]), format="%Y-%m-%d"
    )
    return df


def aggregate_results(folders: list, lazy: bool = False) -> "Results":
    results = Results(lazy=lazy)

    for folder in folders:
        abs_target_folder = pathlib.Path(folder).resolve().parent

        for result_path in tqdm(
            list(pathlib.Path(folder).glob(f"**/{SIM_RESULT_FILENAME}")),
            desc=f"Opening results",
        ):
            if not lazy:
                df = pd.read_csv(result_path)

                if check_columns(df):
                    print("Warning: not all columns are present")

                df = fix_date_column(df)
            else:
                df = None

            relative_path = result_path.resolve().relative_to(abs_target_folder)

            *components, filename = relative_path.parts
            # Check choices
            choice_file = result_path.parent.joinpath(SIM_CHOICE_LOG_FILE)
            if choice_file.exists():
                choices = choice_file
            else:
                choices = None

            results.insert(
                relative_path,
                components,
                filename,
                result_path.resolve().as_posix(),
                df,
                choices,
            )

    return results


def missing_column(func):
    def wrapper(df: "pd.DataFrame"):
        try:
            return func(df)
        except KeyError:
            return pd.Series(np.zeros(len(df.index)))

    return wrapper


@missing_column
def measure_score_ratio(df: "pd.DataFrame") -> "pd.Series":
    score = measure_throughput_ratio(df) - measure_cost_ratio(df)
    return score


@missing_column
def measure_throughput_ratio(df: "pd.DataFrame") -> "pd.Series":
    return (df["read on hit data"]) / df["cache size"]


@missing_column
def measure_throughput_ratio_old(df: "pd.DataFrame") -> "pd.Series":
    return (df["read on hit data"] - df["written data"]) / df["cache size"]


@missing_column
def measure_cost_ratio(df: "pd.DataFrame") -> "pd.Series":
    return (df["written data"] + df["deleted data"]) / df["cache size"]


@missing_column
def measure_score(df: "pd.DataFrame") -> "pd.Series":
    score = measure_throughput(df) - measure_cost(df)
    return score


@missing_column
def measure_throughput(df: "pd.DataFrame") -> "pd.Series":
    # to Terabytes
    return df["read on hit data"] / (1024.0 ** 2.0)


@missing_column
def measure_throughput_old(df: "pd.DataFrame") -> "pd.Series":
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
    return df["bandwidth usage"]


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
    files2plot: list,
    prefix: str,
    top_n: int = 0,
    extended: bool = False,
    sorting_by: list = [],
    new_metrics: bool = True,
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
        values = get_measures(file_, df, extended=extended, new_metrics=new_metrics)
        values[0] = values[0].replace(prefix, "").replace(f"/{SIM_RESULT_FILENAME}", "")

        row_name = []

        for part in values[0].split("/"):
            if part.find("run_full_normal") != -1:
                continue
            elif part.find("T") == len(part) - 1:
                continue
            elif part.find("no_") != -1 and part.find("_feature") != -1:
                feature = part.replace("no_", "").replace("_feature", "").strip()
                row_name.append(f"[NO {feature} feature]")
            elif part.find("epsilon_test") != -1:
                continue
            elif part.find("random_cache") != -1:
                continue
            elif part in ["slow", "fast"]:
                row_name.append(f"[epsilon {part}]")
            else:
                row_name.append(part)

        values[0] = " ".join(row_name)

        values.insert(0, file_)
        table.append(values)

    if extended:
        columns = [
            "source",
            "file",
            "Throughput",
            "Cost",
            "Score (TB)",
            "Throughput (TB)",
            "Cost (TB)",
            "Read on hit ratio",
            "Read on hit (TB)",
            "Bandwidth",
            "Bandwidth (TB)",
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
            "Read on hit (TB)",
            "Bandwidth",
            "Avg. Free Space",
            "Std. Dev. Free Space",
            "CPU Eff.",
        ]

    if new_metrics:
        columns.insert(2, "Score")

    df = pd.DataFrame(table, columns=columns)

    sorting_by = [elm for elm in sorting_by if elm in columns]

    if not sorting_by:
        sorting_by.insert(0, "Score")

    df = df.sort_values(
        by=sorting_by,
        ascending=[False if elm.find("Cost") == -1 else True for elm in sorting_by],
    )

    df = df.round(2)
    if top_n > 0:
        df = df.iloc[:top_n]
    elif top_n < 0:
        raise Exception("top_n is negative")

    source_files = df.source.to_list()
    del df["source"]

    return df, source_files


def get_measures(
    cache_filename: str,
    df: "pd.DataFrame",
    extended: bool = False,
    new_metrics: bool = True,
) -> list:
    measures = [cache_filename]
    # print(cache_filename)

    # Score ratio
    if new_metrics:
        measures.append(measure_score_ratio(df).mean())
        measures.append(measure_throughput_ratio(df).mean())
    else:
        measures.append(measure_throughput_ratio_old(df).mean())

    # Cost ratio
    measures.append(measure_cost_ratio(df).mean())

    if extended:
        # Score (TB)
        measures.append(measure_score(df).mean())
        # Throughput (TB)
        if new_metrics:
            measures.append(measure_throughput(df).mean())
        else:
            measures.append(measure_throughput_old(df).mean())

        # Cost (TB)
        measures.append(measure_cost(df).mean())

    # Read on hit ratio
    measures.append(measure_read_on_hit_ratio(df).mean())

    # Read on hit
    measures.append(df["read on hit data"].mean() / (1024.0 ** 2.0))

    # Bandwidth percentage
    measures.append(measure_bandwidth(df).mean())

    # Bandwidth (TB)
    if extended:
        measures.append(df["read on miss data"].mean() / (1024.0 ** 2.0))

    if extended:
        # Redirect Vol.
        measures.append(measure_redirect_volume(df).mean())

    # Avg. Free Space
    measures.append(measure_avg_free_space(df).mean())

    # Std. Dev. Free Space
    measures.append(measure_std_dev_free_space(df).mean())

    # Hit over Miss
    if extended:
        measures.append(measure_hit_over_miss(df).mean())

    # Num. miss after delete
    if extended:
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
