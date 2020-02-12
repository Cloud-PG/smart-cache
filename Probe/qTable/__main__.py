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
    df = df.sort_values(
        by=['numReq', 'size', 'cacheUsage', 'dataType', 'deltaNumLastRequest'])

    print(f"numReq\tsize\tcacheUsage\tdataType\tdeltaNumLastRequest")
    print("-"*42)

    action_explored = 0
    state_not_explored = 0

    for row in df.itertuples():
        if row.ActionNotStore != 0.:
            action_explored += 1
        if row.ActionStore != 0.:
            action_explored += 1

        if row.ActionNotStore == row.ActionStore == 0.0:
            print(f"{row.dataType}\t{row.numReq}\t{row.size}\t{row.deltaNumLastRequest}\t{STATUS_ARROW} {STATUS_WARNING('NOT EXPLORED')}")
            state_not_explored += 1
        elif row.ActionNotStore >= row.ActionStore:
            print(f"{row.dataType}\t{row.numReq}\t{row.size}\t{row.deltaNumLastRequest}\t{STATUS_ARROW} {STATUS_ERROR('NOT STORE')}")
        else:
            print(
                f"{row.dataType}\t{row.numReq}\t{row.size}\t{row.deltaNumLastRequest}\t{STATUS_ARROW} {STATUS_OK('STORE')}")

    print("-"*42)
    print(
        f"Explored {((df.shape[0] - state_not_explored) / df.shape[0])*100.:0.2f}% states and {(action_explored / (df.shape[0] * 2))*100.:0.2f}% of actions"
    )
    print("-"*42)


if __name__ == "__main__":
    main()
