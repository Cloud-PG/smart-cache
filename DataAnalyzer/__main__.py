import argparse
from . import loaders
from colorama import init, Fore, Back, Style

def main():
    parser = argparse.ArgumentParser(
        "analyzer", description="Analyse the data")

    parser.add_argument('path', default=None,
                        help='Folder or file to open')
    parser.add_argument('--region', type=str,
                        default="all",
                        help='Region of the data to analyse [DEFAULT: "all"]')
    parser.add_argument('--file-type', type=str,
                        default="all",
                        help='File type of the data to analyse [DEFAULT: "all"]')

    args, _ = parser.parse_known_args()

    init()
    
    if args.path != None:
        df = loaders.csv_data(args.path, args.region, args.file_type)
        print(df.columns)


if __name__ == "__main__":
    main()
