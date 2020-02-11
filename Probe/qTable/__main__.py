import argparse

import pandas as pd
from..utils import STATUS_ARROW, STATUS_WARNING, STATUS_OK, STATUS_ERROR


def main():
    parser = argparse.ArgumentParser(
        "qTable inspector", description="Inspect the Q-Learning results")

    parser.add_argument('path',
                        help='Path of qTable csv')

    args, _ = parser.parse_known_args()

    df = pd.read_csv(args.path)
    df = df.sort_values(by=['numReq', 'size', 'deltaNumLastRequest'])

    print(f"numReq\tsize\tdeltaNumLastRequest")
    print("-"*42)
    for row in df.sort_values(by=['numReq', 'size', 'deltaNumLastRequest']).itertuples():
        if row.ActionNotStore == row.ActionStore == 0.0:
            print(f"{row.numReq}\t{row.size}\t{row.deltaNumLastRequest}\t{STATUS_ARROW} {STATUS_WARNING('NOT EXPLORED')}")
        elif row.ActionNotStore >= row.ActionStore:
            print(f"{row.numReq}\t{row.size}\t{row.deltaNumLastRequest}\t{STATUS_ARROW} {STATUS_ERROR('NOT STORE')}")
        else:
            print(f"{row.numReq}\t{row.size}\t{row.deltaNumLastRequest}\t{STATUS_ARROW} {STATUS_OK('STORE')}")


if __name__ == "__main__":
    main()
