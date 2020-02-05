import pandas as pd


def get_object_columns(df: 'pd.DataFrame') -> list:
    return [
        df.columns[idx]
        for idx, type_ in enumerate(df.dtypes)
        if type_ == pd.StringDtype
    ]

def get_unique_values(df: 'pd.DataFrame') -> list:
    return df.unique().tolist()