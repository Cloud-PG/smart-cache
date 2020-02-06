import sqlite3
from os import path

import pandas as pd
from tqdm import tqdm

from..utils import _STATUS_COLOR


def make_sqlite_categories(categories: dict, out_db_file: str = "categories.db"):
    """Create a database to manage the categories.

    :param categories: the categories and their values
    :type categories: dict
    :param out_db_file: output database filename, defaults to "categories.db"
    :type out_df_file: str, optional
    """
    conn = sqlite3.connect(out_db_file)

    cursor = conn.cursor()

    for category, values in tqdm(
        categories.items(),
        desc=f"{_STATUS_COLOR}Populate db",
        position=1,
    ):
        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {category} (
            ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            {category.lower()} TEXT NOT NULL UNIQUE
        );
        ''')
        for value in tqdm(
            values,
            desc=f"{_STATUS_COLOR}Insert values of {category} category",
            position=2,
        ):
            cursor.execute(
                f'''INSERT OR IGNORE INTO {category} ({category.lower()}) VALUES ("{value}")'''
            )
        else:
            conn.commit()
    else:
        conn.commit()

    cursor.close()


def convert_categories_from_sqlite(df: 'pd.DataFrame', categories: list, db_file: str = "categories.db") -> 'pd.DataFrame':
    conn = sqlite3.connect(db_file)

    cursor = conn.cursor()
    total_rows = df.shape[0]
    for category in tqdm(
        categories,
        desc=f"{_STATUS_COLOR}Convert categories",
        position=1,
    ):
        raplace_cache = {}
        for row in tqdm(
            df.itertuples(),
            desc=f"{_STATUS_COLOR}Convert rows of {category} category",
            total=total_rows,
            position=2,
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


def save_numeric_df(filepath: str, df: 'pd.DataFrame'):
    head, tail = path.split(filepath)
    df.to_csv(
        path.join(
            head,
            f"numeric_{tail}"
        ),
        index=False,
    )
