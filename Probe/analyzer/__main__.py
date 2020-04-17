import argparse

from colorama import init

from .. import loaders, plotter, utils
from ..utils import STATUS_ARROW


def main():
    parser = argparse.ArgumentParser(
        "analyzer", description="Analyse the data")

    parser.add_argument('path', default=None,
                        help='Folder or file to open')
    parser.add_argument('analysis', default="dailystats",
                        choices=["dailystats", "weekstats", "globalstats"],
                        help='Folder or file to open')
    parser.add_argument('--output-filename', type=str,
                        default="stats",
                        help='The output file name [DEFAULT: "stats"]')
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
        print(f"{STATUS_ARROW}Sort data by date...")
        utils.sort_by_date(df)
        if args.analysis == "dailystats":
            print(f"{STATUS_ARROW}Extract daily stats...")
            plotter.plot_daily_stats(
                df,
                output_filename=args.output_filename,
                output_type=args.output_type,
                reset_stat_days=args.reset_stat_days
            )
            print(df.columns)
            print(df.JobStart.astype('datetime64[ms]'))
        elif args.analysis == "weekstats":
            print(f"{STATUS_ARROW}Extract weekstats stats...")
            plotter.plot_week_stats(
                df,
                output_filename=args.output_filename,
                output_type=args.output_type,
                reset_stat_days=args.reset_stat_days
            )
        elif args.analysis == "yearstats":
            print(f"{STATUS_ARROW}Extract year stats...")
            plotter.plot_global_stats(
                df,
                output_filename=args.output_filename,
                output_type=args.output_type,
                region=args.region,
            )
        else:
            raise Exception(f"I cannot apply {args.analysis} analysis...")


if __name__ == "__main__":
    main()
