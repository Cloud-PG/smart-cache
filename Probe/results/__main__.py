import argparse
from os import path

from colorama import init

from ..utils import STATUS_ARROW, str2bool
from .data import aggregate_results, parse_simulation_report
from .plotters import plot_num_miss_after_del
from .utils import dashboard, get_prefix


def main():
    parser = argparse.ArgumentParser(
        "results", description="Find and plot all results")

    parser.register('type', 'bool', str2bool)  # add type keyword to registries

    parser.add_argument('action', default='dashboard',
                        choices=['dashboard', 'plot'],
                        help='Action to make')

    parser.add_argument('folder', default=None,
                        help='Folder to inspect for results')

    parser.add_argument('--p-type', default="num_miss_after_del",
                        choices=['num_miss_after_del'],
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
        if args.p_type == "num_miss_after_del":
            plot_num_miss_after_del(
                parse_simulation_report(
                    results.get_all(), path.commonprefix(results.files),
                    generator=True,
                )
            )


if __name__ == "__main__":
    main()
