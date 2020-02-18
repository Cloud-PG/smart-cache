import argparse
import pickle
from os import path

from colorama import init
from tqdm import tqdm

from .. import loaders
from ..utils import STATUS_ARROW, STATUS_WARNING
from .extractor import (check_filename_info, check_region_info,
                        get_object_columns, get_unique_values)
from .utils import CategoryContainer, convert_categories, save_numeric_df


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

        container_filename = f"container_{args.region}_.pickle"
        if path.isfile(container_filename):
            with open(container_filename, "rb") as container_file:
                container = pickle.load(container_file)
        else:
            container = CategoryContainer()

        for filepath, df in tqdm(
            files,
            desc=f"{STATUS_ARROW}Convert files",
            total=tot_files,
        ):

            head, tail = path.split(filepath)
            output_filename = path.join(
                head,
                tail.replace(
                    "results_", f"results_numeric_{args.region}_"
                )
            )

            if not path.isfile(output_filename):
                print(f"{STATUS_ARROW}Process file: {STATUS_WARNING(filepath)}")

                print(f"{STATUS_ARROW}Check region info...")
                df = check_region_info(df)

                print(f"{STATUS_ARROW}Check filename info...")
                df = check_filename_info(df)

                columns = get_object_columns(df)
                categories = dict(
                    (name, get_unique_values(df[name])) for name in columns
                )
                container.update(categories, filepath)

                new_df = convert_categories(
                    filepath, df, categories, container)
                save_numeric_df(filepath, new_df,
                                output_filename=output_filename)

                print(f"{STATUS_ARROW}Save database...")
                with open(f"container_{args.region}_.pickle", "wb") as out_file:
                    pickle.dump(container, out_file)


if __name__ == "__main__":
    main()
