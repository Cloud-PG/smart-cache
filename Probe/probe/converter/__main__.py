import argparse
import pickle
from os import path

from colorama import init
from tqdm import tqdm

from .. import loaders
from ..utils import STATUS_ARROW, STATUS_WARNING, str2bool
from .extractor import (check_filename_info, check_region_info,
                        get_object_columns, get_unique_values)
from .utils import (CategoryContainer, convert_categories, save_numeric_df,
                    shuffle_df, sort_from_avro)


def main():
    parser = argparse.ArgumentParser(
        "converter", description="Convert the data")

    parser.register('type', 'bool', str2bool)  # add type keyword to registries

    parser.add_argument('path', default=None,
                        help='Folder or file to open')
    parser.add_argument('--category-db-file', type=str,
                        default="categories.db",
                        help='The database file name [DEFAULT: "categories.db"]')
    parser.add_argument('--region', type=str,
                        default="all",
                        help='Region of the data to analyse [DEFAULT: "all"]')
    parser.add_argument('--seed', type=int,
                        default=42,
                        help='Shuffle seed number [DEFAULT: 42]')
    parser.add_argument('--shuffle', type='bool',
                        default=False,
                        help='Shuffle the dataframe [DEFAULT: True]')
    parser.add_argument('--order-folder', type=str,
                        default="",
                        help='Folder with file order from AVRO source [DEFAULT: ""]')

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
            cur_filename = tail
            if args.shuffle:
                output_filename = path.join(
                    head,
                    tail.replace(
                        "results_", f"results_numeric_{args.region}_shuffle_{args.seed}"
                    )
                )
            elif args.order_folder:
                output_filename = path.join(
                    head,
                    tail.replace(
                        "results_", f"results_numeric_{args.region}_avro_order_"
                    )
                )
            else:
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

                if args.shuffle:
                    print(f"{STATUS_ARROW}Shuffle DataFrame...")
                    df = shuffle_df(df, args.seed)

                if args.order_folder:
                    df = sort_from_avro(df, cur_filename, args.order_folder)
                    if df is None:
                        print(
                            f"{STATUS_ARROW}Jump file due to no avro order: {STATUS_WARNING(filepath)}")
                        continue

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
