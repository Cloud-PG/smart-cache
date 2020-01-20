import pandas as pd
from os import path
import os
import gzip
from tqdm import tqdm
from colorama import Fore, Back, Style

__all__ = ['csv_data']

_STATUS = f"{Style.BRIGHT + Fore.MAGENTA}==> {Style.RESET_ALL}"


def _load_csv_file(input_path: str, region_filter: str = None, 
                   file_type_filter: str = None) -> 'pd.DataFrame':
    head, tail = path.splitext(input_path)
    if tail in ['.gz', 'gzip']:
        head, tail = path.splitext(head)
        if tail == ".csv":
            with gzip.GzipFile(input_path, "rb") as data_file:
                df = pd.read_csv(data_file)
        else:
            raise Exception(f"File type '{tail}' is not supported...")
    elif tail == '.csv':
        df = pd.read_csv(input_path, index=False)
    else:
        raise Exception(f"File type '{tail}' is not supported...")

    if region_filter and region_filter != "all":
        df = df[df.site_name.str.contains(f"_{region_filter}_", case=False)]
    
    if file_type_filter and file_type_filter != "all":
        df = df[df.filename.str.contains(f"\/{file_type_filter}\/", case=False, regex=True)]

    return df


def csv_data(input_path: str, region_filter: str = None, 
             file_type_filter: str = None) -> 'pd.DataFrame':
    """Open all data from csv files.

    input_path cold be a folder or a file.
    CSV data could be also zipped with gZip.
    """
    if path.isdir(input_path):
        data_frames = []
        for filename in tqdm(os.listdir(input_path), desc=f"{_STATUS}Load folder {input_path}"):
            data_frames.append(
                _load_csv_file(path.join(input_path, filename)),
                region_filter,
                file_type_filter
            )
        else:
            return pd.concat(data_frames)
    else:
        print(f"{_STATUS}Load file {input_path}")
        return _load_csv_file(input_path, region_filter, file_type_filter)
