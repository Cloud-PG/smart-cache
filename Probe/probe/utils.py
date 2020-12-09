import pandas as pd
from colorama import Fore, Style
from pathlib import Path

STATUS_ARROW = f"{Style.BRIGHT + Fore.MAGENTA}==> {Style.RESET_ALL}"


def STATUS_WARNING(string):
    return f"{Style.BRIGHT + Fore.YELLOW}{string}{Style.RESET_ALL}"


def STATUS_ERROR(string):
    return f"{Style.BRIGHT + Fore.RED}{string}{Style.RESET_ALL}"


def STATUS_OK(string):
    return f"{Style.BRIGHT + Fore.GREEN}{string}{Style.RESET_ALL}"


def sort_by_date(df: "pd.DataFrame", column_name: str = "reqDay") -> "pd.DataFrame":
    """Sort the dataframe by date.

    :return: the sorted dataframe
    :rtype: pandas.DataFrame
    """
    df.sort_values(by="day", inplace=True)
    return df


def search_runs(folder: Path) -> list:
    """Search for previous training results.

    :param folder: result folder
    :type folder: Path
    :return: list of the files of previous runs
    :rtype: list
    """
    runs = []
    for file_ in folder.glob("*_run-*.csv"):
        runs.append(file_)

    return list(sorted(runs))
