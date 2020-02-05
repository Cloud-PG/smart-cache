import argparse

from colorama import init

from .extractor import get_object_columns, get_unique_values 
from .. import loaders, utils
from ..utils import _STATUS_COLOR


def main():
    parser = argparse.ArgumentParser(
        "converter", description="Convert the data")

    parser.add_argument('path', default=None,
                        help='Folder or file to open')
    parser.add_argument('--output-category-db', type=str,
                        default="categories.db",
                        help='The output file name [DEFAULT: "categories.db"]')

    args, _ = parser.parse_known_args()

    init()

    if args.path is not None:
        df = loaders.csv_data(args.path)
        # print(f"{_STATUS_COLOR}Extract stats...")
        # plotter.plot_daily_stats(
        #     df,
        #     output_filename=args.output_filename,
        #     output_type=args.output_type,
        #     reset_stat_days=args.reset_stat_days
        # )
        print(df.columns)
        print(df.dtypes)
        print(df.sample())
        columns = get_object_columns(df)
        values = [
            get_unique_values(df[name]) for name in columns
        ]
        print(values)


if __name__ == "__main__":
    main()
