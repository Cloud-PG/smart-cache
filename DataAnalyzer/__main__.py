import argparse

from colorama import init

from . import loaders, utils
from .utils import _STATUS


def main():
    parser = argparse.ArgumentParser(
        "analyzer", description="Analyse the data")

    parser.add_argument('path', default=None,
                        help='Folder or file to open')
    parser.add_argument('--output-filename', type=str,
                        default="dailystats.html",
                        help='The output file name [DEFAULT: "dailystats.html"]')
    parser.add_argument('--region', type=str,
                        default="all",
                        help='Region of the data to analyse [DEFAULT: "all"]')
    parser.add_argument('--reset-stat-days', type=int,
                        default=7,
                        help='Number of days after the stats are reset [DEFAULT: 7]')
    parser.add_argument('--file-type', type=str,
                        default="all",
                        help='File type of the data to analyse [DEFAULT: "all"]')

    args, _ = parser.parse_known_args()

    init()

    if args.path != None:
        df = loaders.csv_data(args.path, args.region, args.file_type)
        print(f"{_STATUS}Sort data by date...")
        utils.sort_by_date(df)
        print(f"{_STATUS}Plot stats...")
        utils.plot_daily_stats(df,
                               output_filename=args.output_filename,
                               reset_stat_days=args.reset_stat_days)
        print(df.columns)
        print(df.JobStart.astype('datetime64[ms]'))


if __name__ == "__main__":
    main()
