import argparse
import pickle
from os import path

from ..utils import STATUS_ARROW, STATUS_WARNING
from .utils import str2bool


def main():
    parser = argparse.ArgumentParser(
        "converter", description="Convert the data")

    parser.register('type', 'bool', str2bool)  # add type keyword to registries

    parser.add_argument('pickleFile', default=None,
                        help='File pickle to open')
    parser.add_argument('query', default=None,
                        help='Query to resolve')
    parser.add_argument('--sort-by-value', default=False, type='bool',
                        help='Sort results by values')

    args = parser.parse_args()

    if path.isfile(args.pickleFile):
        print(f"{STATUS_ARROW}Loading file: {STATUS_WARNING(args.pickleFile)}")
        with open(args.pickleFile, "rb") as container_file:
            container = pickle.load(container_file)
        print(f"{STATUS_ARROW}Executing query: {STATUS_WARNING(args.query)}")
        print("="*42)
        print(container.query(args.query, args.sort_by_value))
        print("="*42)
        print(f"{STATUS_ARROW}Done!")
    else:
        raise Exception("File not valid")


if __name__ == "__main__":
    main()
