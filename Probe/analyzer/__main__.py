import argparse

from colorama import init

from .. import loaders, plotter, utils
from ..utils import _STATUS_COLOR


def main():
    parser = argparse.ArgumentParser(
        "analyzer", description="Analyse the data")

    parser.add_argument('path', default=None,
                        help='Folder or file to open')
    parser.add_argument('--output-filename', type=str,
                        default="dailystats",
                        help='The output file name [DEFAULT: "dailystats"]')
    parser.add_argument('--region', type=str,
                        default="all",
                        help='Region of the data to analyse [DEFAULT: "all"]')
    parser.add_argument('--reset-stat-days', type=int,
                        default=7,
                        help='Number of days after the stats are reset [DEFAULT: 7]')
    parser.add_argument('--month', type=int,
                        default=-1,
                        help='Month to extract [DEFAULT: -1]')
    parser.add_argument('--output-type', choices=['show', 'html', 'png'],
                        type=str, default='show',
                        help='How to plot the results [DEFAULT: show]')
    parser.add_argument('--file-type', type=str,
                        default="all",
                        help='File type of the data to analyse [DEFAULT: "all"]')

    args, _ = parser.parse_known_args()

    init()

    if args.path is not None:
        df = loaders.csv_data(
            args.path,
            region_filter=args.region,
            file_type_filter=args.file_type,
            month_filter=args.month
        )
        print(f"{_STATUS_COLOR}Sort data by date...")
        utils.sort_by_date(df)
        print(f"{_STATUS_COLOR}Extract stats...")
        plotter.plot_daily_stats(
            df,
            output_filename=args.output_filename,
            output_type=args.output_type,
            reset_stat_days=args.reset_stat_days
        )
        print(df.columns)
        print(df.JobStart.astype('datetime64[ms]'))


if __name__ == "__main__":
    main()
