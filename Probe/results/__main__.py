import argparse
from os import path

from colorama import init

from ..utils import STATUS_ARROW, str2bool
from .dashboard import dashboard
from .data import aggregate_results, parse_simulation_report
from .plotters import plot_miss_freq, plot_num_miss_after_del


def main():
    parser = argparse.ArgumentParser(
        "results", description="Find and plot all results")

    parser.register('type', 'bool', str2bool)  # add type keyword to registries

    parser.add_argument('action', default='dashboard',
                        choices=['dashboard', 'plot'],
                        help='Action to make')

    parser.add_argument('folder', default=None,
                        help='Folder to inspect for results')

    parser.add_argument('--p-type', default="AFTERDELETE",
                        choices=['AFTERDELETE', 'MISSFREQ'],
                        help='Plot type')

    parser.add_argument('--dash-ip', default="localhost", type=str,
                        help='IP addr where start the dashboard server')

    args, _ = parser.parse_known_args()

    init()

    print(f"{STATUS_ARROW}Aggregate results...")
    results = aggregate_results(args.folder)

    if args.action == 'dashboard':
        print(f"{STATUS_ARROW}Start dashboard...")
        dashboard(results, args.dash_ip)
    elif args.action == 'plot':
        if args.p_type == "AFTERDELETE":
            plot_num_miss_after_del(
                parse_simulation_report(
                    results.get_all(), path.commonprefix(results.files),
                    generator=True,
                    target=args.p_type,
                )
            )
        elif args.p_type == "MISSFREQ":
            plot_miss_freq(
                parse_simulation_report(
                    results.get_all(), path.commonprefix(results.files),
                    generator=True,
                    target=args.p_type,
                )
            )


if __name__ == "__main__":
    main()
