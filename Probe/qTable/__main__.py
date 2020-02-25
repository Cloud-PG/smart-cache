import argparse

import pandas as pd
from colorama import Fore, Style

from..utils import STATUS_ARROW


def main():
    parser = argparse.ArgumentParser(
        "qTable inspector", description="Inspect the Q-Learning results")

    parser.add_argument('path',
                        help='Path of qTable csv')

    args, _ = parser.parse_known_args()

    df = pd.read_csv(args.path)

    print(df)

    sort_by = [column for column in df.columns if column.find("Action") == -1]
    actions = list(set(df.columns) - set(sort_by))
    action_counters = [0 for _ in range(len(actions))]

    df = df.sort_values(by=sort_by)

    print("-"*80)
    print(" | ".join(sort_by+['Action']))
    print("-"*80)

    state_not_explored = 0

    for idx, row in enumerate(df.itertuples()):
        action_values = [getattr(row, value) for value in actions]
        best_action = actions[action_values.index(max(action_values))]
        for idx, value in enumerate(action_values):
            if value != 0.:
                action_counters[idx] += 1

        if all([value == 0.0 for value in action_values]):
            state_values = " | ".join(
                [str(getattr(row, value)) for value in sort_by])
            print(f"{Style.DIM}{Fore.YELLOW}{state_values} {STATUS_ARROW} {Style.DIM}{Fore.YELLOW}{'NOT EXPLORED'}{Style.RESET_ALL}")
            state_not_explored += 1
        else:
            state_values = " | ".join(
                [str(getattr(row, value)) for value in sort_by])
            print(f"{Style.BRIGHT}{state_values} {STATUS_ARROW} {Style.BRIGHT}{best_action}{Style.RESET_ALL}")

    print("-"*42)
    print(
        f"Explored {((df.shape[0] - state_not_explored) / df.shape[0])*100.:0.2f}% states and {(sum(action_counters) / (df.shape[0] * 2))*100.:0.2f}% of actions"
    )
    print("-"*42)


if __name__ == "__main__":
    main()
