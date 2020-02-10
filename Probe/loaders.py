import gzip
import os
from os import path

import pandas as pd
from tqdm import tqdm

from .utils import STATUS_ARROW

__all__ = ['csv_data']


def _load_csv_file(input_path: str, region_filter: str = None,
                   file_type_filter: str = None) -> 'pd.DataFrame':
    """Load a csv data file.

    :raises Exception: File type not supported
    :raises Exception: Compressed file type not supported
    :return: The data content
    :rtype: pandas.DataFrame
    """
    head, tail = path.splitext(input_path)
    if tail in ['.gz', 'gzip']:
        head, tail = path.splitext(head)
        if tail == ".csv":
            with gzip.GzipFile(input_path, "rb") as data_file:
                df = pd.read_csv(data_file, index_col=False)
        else:
            raise Exception(f"File type '{tail}' is not supported...")
    elif tail == '.csv':
        df = pd.read_csv(input_path, index_col=False)
    else:
        raise Exception(f"File type '{tail}' is not supported...")

    if region_filter and region_filter != "all":
        df = df[df.SiteName.str.contains(f"_{region_filter}_", case=False)]

    if file_type_filter and file_type_filter != "all":
        df = df[df.Filename.str.contains(
            f"/{file_type_filter}/", case=False, regex=True)]

    return df


def _get_month(filename: str) -> int:
    """Get the month number from a data filename

    :param filename: The data filename
    :type filename: str
    :return: the number of the month found inthe filename
    :rtype: int
    """
    return int(filename.split(".")[0].replace("results_", "").split("-")[1])


def gen_csv_data(input_path: str, region_filter: str = None,
                 file_type_filter: str = None,
                 month_filter: int = -1) -> 'pd.DataFrame':
    """Generate the dataframe of source data (folder or a file)

    :yield: first the total amount of files and then 
            a tuple with filepath and DataFrame
    :rtype: generator
    """
    if path.isdir(input_path):
        files = [file_ for file_ in os.listdir(
            input_path) if file_.find("csv") != -1]
        yield len(files)
        for filename in files:
            if month_filter != -1:
                if _get_month(filename) != month_filter:
                    continue
            filepath = path.join(input_path, filename)
            df = _load_csv_file(
                filepath,
                region_filter,
                file_type_filter
            )
            yield filepath, df
    else:
        yield 1
        yield input_path, _load_csv_file(
            input_path, region_filter, file_type_filter
        )


def csv_data(input_path: str, region_filter: str = None,
             file_type_filter: str = None,
             month_filter: int = -1) -> 'pd.DataFrame':
    """Open csv data folder and files

    :return: The whole dataset
    :rtype: pandas.DataFrame
    """
    if path.isdir(input_path):
        data_frames = []
        files = [file_ for file_ in os.listdir(
            input_path) if file_.find("csv") != -1]
        for filename in tqdm(files, desc=f"{STATUS_ARROW}Load folder {input_path}"):
            if month_filter != -1:
                if _get_month(filename) != month_filter:
                    continue
            data_frames.append(
                _load_csv_file(
                    path.join(input_path, filename),
                    region_filter,
                    file_type_filter
                )
            )
        else:
            if data_frames:
                return pd.concat(data_frames)
            else:
                pd.DataFrame()
    else:
        print(f"{STATUS_ARROW}Load file {input_path}")
        return _load_csv_file(input_path, region_filter, file_type_filter)
