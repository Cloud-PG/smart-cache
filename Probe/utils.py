import pandas as pd
from colorama import Fore, Style

STATUS_ARROW = f"{Style.BRIGHT + Fore.MAGENTA}==> {Style.RESET_ALL}"
STATUS_WARNING = lambda string: f"{Style.BRIGHT + Fore.YELLOW}{string}{Style.RESET_ALL}"
STATUS_ERROR = lambda string: f"{Style.BRIGHT + Fore.RED}{string}{Style.RESET_ALL}"
STATUS_OK = lambda string: f"{Style.BRIGHT + Fore.GREEN}{string}{Style.RESET_ALL}"


def sort_by_date(df: 'pd.DataFrame', column_name: str = "reqDay") -> 'pd.DataFrame':
    """Sort the dataframe by date.
    
    :return: the sorted dataframe
    :rtype: pandas.DataFrame
    """
    df.sort_values(by="reqDay", inplace=True)
    return df
