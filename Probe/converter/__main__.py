import argparse

from colorama import init
from tqdm import tqdm

from .. import loaders, utils
from ..utils import _STATUS_COLOR
from .extractor import get_object_columns, get_unique_values
from .utils import (convert_categories_from_sqlite, make_sqlite_categories,
                    save_numeric_df)


def main():
    parser = argparse.ArgumentParser(
        "converter", description="Convert the data")

    parser.add_argument('path', default=None,
                        help='Folder or file to open')
    parser.add_argument('--category-db-file', type=str,
                        default="categories.db",
                        help='The database file name [DEFAULT: "categories.db"]')
    parser.add_argument('--region', type=str,
                        default="all",
                        help='Region of the data to analyse [DEFAULT: "all"]')

    args, _ = parser.parse_known_args()

    init()

    if args.path is not None:
        files = loaders.gen_csv_data(args.path, region_filter=args.region)
        tot_files = next(files)
        for filepath, df in tqdm(
            files,
            desc=f"{_STATUS_COLOR}Convert files",
            total=tot_files,
        ):
            columns = get_object_columns(df)
            categories = dict(
                (name, get_unique_values(df[name])) for name in columns
            )
            make_sqlite_categories(
                filepath,
                categories,
                args.category_db_file,
                region_filter=args.region
            )
            new_df = convert_categories_from_sqlite(
                filepath,
                df, categories,
                args.category_db_file,
                region_filter=args.region
            )
            save_numeric_df(filepath, new_df, region_filter=args.region)


if __name__ == "__main__":
    main()
