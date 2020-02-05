import pandas as pd
from colorama import Fore, Style

_STATUS_COLOR = f"{Style.BRIGHT + Fore.MAGENTA}==> {Style.RESET_ALL}"


def sort_by_date(df: 'pd.DataFrame', column_name: str = "reqDay") -> 'pd.DataFrame':
    df.sort_values(by="reqDay", inplace=True)
    return df
