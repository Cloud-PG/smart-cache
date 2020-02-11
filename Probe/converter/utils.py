import sqlite3
from os import path

import pandas as pd
from tqdm import tqdm

from..utils import STATUS_ARROW, STATUS_WARNING, STATUS_OK


class CategoryContainer:

    def __init__(self, filename: str = ''):
        self._data = dict()
        self.__sequences = dict()

    def __getstate__(self):
        return {
            'data': self._data,
            'sequences': self.__sequences
        }

    def __setstate__(self, data):
        self._data = data['data']
        self.__sequences = data['sequences']

    def update(self, categories, source_filepath: str):
        for category, values in tqdm(
            categories.items(),
            desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filepath)}] Populate container",
            position=1,
        ):
            if category not in self._data:
                self._data[category] = {}
                self.__sequences[category] = 0

            cur_category = self._data[category]
            for value in tqdm(
                values,
                desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filepath)}] Insert values of {category} category",
                position=2,
            ):
                if value not in cur_category:
                    cur_category[value] = self.__sequences[category]
                    self.__sequences[category] += 1

    def get(self, category, value):
        return self._data[category][value]


def convert_categories(source_filepath: str,
                       df: 'pd.DataFrame',
                       categories: dict,
                       container: 'CategoryContainer',
                       ) -> 'pd.DataFrame':
    """Get the category ID from the category sqlite database.

    :param source_filepath: the source filename
    :type source_filepath: str
    :param df: the input dataframe
    :type df: pandas.DataFrame
    :param db_file: the database filename, defaults to "categories.db"
    :type db_file: str, optional
    :param region_filter: the region of the values
    :type region_filter: str
    :return: the dataframe with the id instead of the values
    :rtype: pandas.DataFrame
    """

    total_rows = df.shape[0]
    for category in tqdm(
        categories,
        desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filepath)}] Convert categories",
    ):
        raplace_cache = {}
        for row in tqdm(
            df.itertuples(),
            desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filepath)}] Convert rows of {category} category",
            total=total_rows,
        ):
            cur_cat_value = getattr(row, category)
            if cur_cat_value not in raplace_cache and not isinstance(cur_cat_value, int):

                cat_id = container.get(category, cur_cat_value)
                raplace_cache[cur_cat_value] = cat_id
                df[category].replace(
                    cur_cat_value, raplace_cache[cur_cat_value], inplace=True)
        else:
            df[category].astype(int)

    return df


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
        desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filename)}] Populate db",
    ):
        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {category} (
            ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            {category.lower()} TEXT NOT NULL UNIQUE
        );
        ''')
        for value in tqdm(
            values,
            desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filename)}] Insert values of {category} category",
        ):
            print(
                f'''INSERT OR IGNORE INTO {category} ({category.lower()}) VALUES ("{value}")''')
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
        desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filename)}] Convert categories",
    ):
        raplace_cache = {}
        for row in tqdm(
            df.itertuples(),
            desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filename)}] Convert rows of {category} category",
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


def save_numeric_df(filepath: str, df: 'pd.DataFrame', output_filename: str = "result_numeric.csv.gz"):
    """Save the new numeric dataset.

    :param filepath: The original dataset filename
    :type filepath: str
    :param df: the new dataframe source to save
    :type df: pandas.DataFrame
    :param output_filename: the name of the saved file
    :type output_filename: str, optional
    """
    print(f"{STATUS_ARROW}Save csv {STATUS_OK(output_filename)}\x1b[0K")
    df.to_csv(output_filename, index=False)
