import pandas as pd


def check_region(df: 'pd.DataFrame') -> 'pd.DataFrame':
    """Check if region column exists.

    If not exists, it creates the column from the SiteName information

    :return: the data frame with the new column
    :rtype: pandas.DataFrame
    """
    if 'region' not in df.columns:
        # Example lambda map:
        #  T2_US_Vanderbilt -> us
        df['region'] = df['SiteName'].map(
            lambda elm: elm.split("_")[1].lower())
    return df


def get_object_columns(df: 'pd.DataFrame') -> list:
    """Returns the name of the columns that are objects (string)

    :param df: the input dataframe
    :type df: pandas.DataFrame
    :return: the list of the column names that are string
    :rtype: list
    """
    return [
        df.columns[idx]
        for idx, type_ in enumerate(df.dtypes)
        if type_ == pd.StringDtype
    ]


def get_unique_values(df: 'pd.Series') -> list:
    """Returns unique values from a pandas series.

    :param df: The value series
    :type df: pandas.Series
    :return: the unique values
    :rtype: list
    """
    return df.unique().tolist()
