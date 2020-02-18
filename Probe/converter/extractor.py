import pandas as pd


def check_region_info(df: 'pd.DataFrame') -> 'pd.DataFrame':
    """Check if region column exists.

    If not exists, it creates the column from the SiteName information

    :param df: the input dataframe
    :type df: pandas.DataFrame
    :return: the data frame with the new column
    :rtype: pandas.DataFrame
    """
    if 'Region' not in df.columns:
        # Example lambda map:
        #  T2_US_Vanderbilt -> us
        df['Region'] = df['SiteName'].map(
            lambda elm: elm.split("_")[1].lower()
        )
    return df


def check_filename_info(df: 'pd.DataFrame') -> 'pd.DataFrame':
    """Check if filename stats exist.

    If not exist, it creates the columns from the filename information

    :param df: the input dataframe
    :type df: pandas.DataFrame
    :return: the data frame with the new column
    :rtype: pandas.DataFrame
    """
    if 'Campain' not in df.columns:
        # Example lambda map:
        #  /store/data/Run2016B/DoubleEG/MINIAOD/03Feb2017_ver2-v2/50000/0EEFA768-E2EA-E611-86FE-0025905A610A.root -> Run2016B
        df['Campain'] = df['Filename'].map(
            lambda elm: elm.split("/")[3]
        )
    if 'Process' not in df.columns:
        # Example lambda map:
        #  /store/data/Run2016B/DoubleEG/MINIAOD/03Feb2017_ver2-v2/50000/0EEFA768-E2EA-E611-86FE-0025905A610A.root -> DoubleEG
        df['Process'] = df['Filename'].map(
            lambda elm: elm.split("/")[4]
        )
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
