import argparse

from colorama import init

from ..utils import STATUS_ARROW, str2bool
from .data import aggregate_results
from .utils import dashboard


def main():
    parser = argparse.ArgumentParser(
        "results", description="Find and plot all results")

    parser.register('type', 'bool', str2bool)  # add type keyword to registries

    parser.add_argument('folder', default=None,
                        help='Folder to inspect for results')

    args, _ = parser.parse_known_args()

    init()

    print(f"{STATUS_ARROW}Aggregate results...")
    results = aggregate_results(args.folder)
    print(f"{STATUS_ARROW}Start dashboard...")
    dashboard(results)


if __name__ == "__main__":
    main()
