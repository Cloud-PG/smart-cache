import sqlite3
from os import path

import pandas as pd
from tqdm import tqdm

from..utils import _STATUS_COLOR


def make_sqlite_categories(source_filename: str,
                           categories: dict,
                           out_db_file: str = "categories.db",
                           region_filter: str = "all"
                           ):
    """Create a database to manage the categories.

    :param source_filename: the source filename
    :type source_filename: str
    :param categories: the categories and their values
    :type categories: dict
    :param out_db_file: output database filename, defaults to "categories.db"
    :type out_df_file: str, optional
    :param region_filter: the ragion of the values
    :type region_filter: str
    """
    filename, extension = path.splitext(out_db_file)
    database_filename = f"{filename}_{region_filter}{extension}"
    conn = sqlite3.connect(database_filename)

    cursor = conn.cursor()

    for category, values in tqdm(
        categories.items(),
        desc=f"{_STATUS_COLOR}[File:{source_filename}] Populate db",
    ):
        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {category} (
            ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            {category.lower()} TEXT NOT NULL UNIQUE
        );
        ''')
        for value in tqdm(
            values,
            desc=f"{_STATUS_COLOR}[File:{source_filename}] Insert values of {category} category",
        ):
            cursor.execute(
                f'''INSERT OR IGNORE INTO {category} ({category.lower()}) VALUES ("{value}")'''
            )
        else:
            conn.commit()
    else:
        conn.commit()

    cursor.close()


def convert_categories_from_sqlite(source_filename: str,
                                   df: 'pd.DataFrame',
                                   categories: list,
                                   db_file: str = "categories.db",
                                   region_filter: str = "all"
                                   ) -> 'pd.DataFrame':
    """Get the category ID from the category sqlite database.

    :param source_filename: the source filename
    :type source_filename: str
    :param df: the input dataframe
    :type df: pandas.DataFrame
    :param db_file: the database filename, defaults to "categories.db"
    :type db_file: str, optional
    :param region_filter: the region of the values
    :type region_filter: str
    :return: the dataframe with the id instead of the values
    :rtype: pandas.DataFrame
    """
    filename, extension = path.splitext(db_file)
    database_filename = f"{filename}_{region_filter}{extension}"
    conn = sqlite3.connect(database_filename)

    cursor = conn.cursor()
    total_rows = df.shape[0]
    for category in tqdm(
        categories,
        desc=f"{_STATUS_COLOR}[File:{source_filename}] Convert categories",
    ):
        raplace_cache = {}
        for row in tqdm(
            df.itertuples(),
            desc=f"{_STATUS_COLOR}[File:{source_filename}] Convert rows of {category} category",
            total=total_rows,
        ):
            cur_cat_value = getattr(row, category)
            if cur_cat_value not in raplace_cache and not isinstance(cur_cat_value, int):
                query = f'SELECT ID FROM {category} WHERE {category.lower()}=="{cur_cat_value}"'
                cursor.execute(query)
                cat_id = cursor.fetchone()[0]
                raplace_cache[cur_cat_value] = cat_id
                df[category].replace(
                    cur_cat_value, raplace_cache[cur_cat_value], inplace=True)
        else:
            df[category].astype(int)
    else:
        cursor.close()

    return df


def save_numeric_df(filepath: str, df: 'pd.DataFrame', region_filter: str = "all"):
    """Save the new numeric dataset.

    :param filepath: The original dataset filename
    :type filepath: str
    :param df: the new dataframe source to save
    :type df: pandas.DataFrame
    :param region_filter: the region of the source, defaults to "all"
    :type region_filter: str, optional
    """
    head, tail = path.split(filepath)
    output_filename = tail.replace(
        "results_", f"results_numeric_{region_filter}_")
    print(f"{_STATUS_COLOR}Save csv {output_filename}")
    df.to_csv(
        path.join(
            head,
            output_filename
        ),
        index=False,
    )
