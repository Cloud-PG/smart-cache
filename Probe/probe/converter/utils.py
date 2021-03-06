import json
import sqlite3
from multiprocessing import Pool
from os import cpu_count, path, walk

import numpy as np
import pandas as pd
from pygments import formatters, highlight, lexers
from tqdm import tqdm

from..utils import STATUS_ARROW, STATUS_WARNING, STATUS_OK


def shuffle_df(df: 'pd.DataFrame', seed: int = 42) -> 'pd.DataFrame':
    """Shuffle a dataframe with the given seed

    :param df: the input dataframe
    :type df: pandas.DataFrame
    :param seed: the seed number
    :type seed: int
    :return: the shuffled dataframe
    :rtype: pandas.DataFrame
    """
    return df.sample(frac=1, random_state=seed).reset_index(drop=True)


def sort_from_avro(df: 'pd.DataFrame', cur_filename: str, order_folder: str) -> 'pd.DataFrame':
    """Shuffle a dataframe with the given seed

    :param df: the input dataframe
    :type df: pandas.DataFrame
    :param cur_filename: the initial file name
    :type cur_filename: str
    :param order_folder: the order_folder path
    :type order_folder: str
    :return: the shuffled dataframe
    :rtype: pandas.DataFrame
    """

    real_filename = cur_filename.split(".", 1)[0].replace("results_", "")
    ord_df = None

    for root, _, files in walk(order_folder):
        for file_ in files:
            if file_.find(real_filename) != -1:
                ord_df = pd.read_csv(path.join(root, file_))
                ord_df.rename(columns={'FileName': "Filename"}, inplace=True)

    if ord_df is None:
        return None

    print(
        f"{STATUS_ARROW}[File:{STATUS_WARNING(cur_filename)}][Order dataframe with avro indexes]")
    df_mask = df.Filename.duplicated(keep=False)
    ord_df_mask = ord_df.Filename.duplicated(keep=False)
    # Add counter number for unique indexes
    df.loc[df_mask, 'Filename'] += "_#" + \
        df.groupby('Filename').cumcount().add(1).astype(str)
    ord_df.loc[ord_df_mask, 'Filename'] += "_#" + \
        ord_df.groupby('Filename').cumcount().add(1).astype(str)
    # Change indexes
    df = df.set_index("Filename")
    ord_df = ord_df.set_index("Filename")
    # Reindex
    new_index = df.reindex_like(ord_df, method=None).dropna()
    df.set_index(new_index.index, inplace=True)
    df.reset_index(inplace=True)
    ord_df.reset_index(inplace=True)
    # Remove duplicate counters
    df.Filename = df.Filename.apply(lambda elm: elm.rsplit("_#", 1)[
                                    0] if elm.find("_#") else elm)
    ord_df.Filename = ord_df.Filename.apply(lambda elm: elm.rsplit("_#", 1)[
        0] if elm.find("_#") else elm)

    if not all(ord_df.Filename.eq(df.Filename)):
        print("File name not equal...")
        exit(-1)

    return df


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

    def __add_category(self, category: str):
        self._data[category] = {}
        self.__sequences[category] = 0

    def update(self, categories, source_filepath: str):
        for category, values in tqdm(
            categories.items(),
            desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filepath)}] Populate container",
            position=1,
        ):
            if category not in self._data:
                self.__add_category(category)

            cur_category = self._data[category]
            for value in tqdm(
                values,
                desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filepath)}] Insert values of {category} category",
                position=2,
            ):
                if value not in cur_category:
                    cur_category[value] = self.__sequences[category]
                    self.__sequences[category] += 1

    def get(self, category, value=None):
        if value:
            return self._data[category][value]
        else:
            return self._data[category]

    def query(self, query: str, sort_by_value: bool = False) -> str:
        if query == "categories":
            json_output = json.dumps(
                {'keys': list(self._data.keys())}, indent=2, sort_keys=True)
            colorful_json = make_colored_json(json_output)
            return colorful_json
        elif query.find(".") != -1:
            subQuery, category = query.split(".", 1)
            if subQuery == "all":
                if sort_by_value:
                    obj = {key: value for key, value in sorted(
                        self._data[category].items(), key=lambda elm: elm[1])}
                else:
                    obj = {key: value for key,
                           value in self._data[category].items()}
                json_output = json.dumps(
                    {category: obj},
                    indent=2,
                    sort_keys=True if not sort_by_value else False
                )
                colorful_json = make_colored_json(json_output)
                return colorful_json
            elif subQuery == "valueOf":
                category, key = category.split(".")
                json_output = json.dumps(
                    {category: {key: self._data[category][key]}},
                    indent=2,
                    sort_keys=True if not sort_by_value else False
                )
                colorful_json = make_colored_json(json_output)
                return colorful_json
            else:
                raise Exception(
                    f"Error: sub query {subQuery} of {query} is not correct...")
        else:
            raise Exception(f"Error: query {query} is not correct...")


def make_colored_json(json_string: str) -> str:
    return highlight(
        json_string, lexers.JsonLexer(),
        formatters.TerminalFormatter()
    )


def category_replace(items: tuple):
    values, category = items
    return [category[value] for value in values]


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
    num_cores = cpu_count()
    for category in tqdm(
        categories,
        desc=f"{STATUS_ARROW}[File:{STATUS_WARNING(source_filepath)}] Convert categories",
    ):
        pool = Pool(num_cores)
        cur_category = container.get(category)
        cur_column = df[category].to_numpy()
        column_split = np.array_split(cur_column, num_cores*4)
        items = [(elm, cur_category) for elm in column_split]
        df[category] = [elm for chunk in pool.map(
            category_replace, items) for elm in chunk]
        pool.close()
        pool.join()
        df[category] = df[category].astype(int)

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
