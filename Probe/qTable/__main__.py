import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from colorama import Style

from..utils import STATUS_ARROW


def main():
    parser = argparse.ArgumentParser(
        "qTable inspector", description="Inspect the Q-Learning results"
    )

    parser.add_argument('path', help='Path of qTable csv')

    args, _ = parser.parse_known_args()

    df = pd.read_csv(args.path)
    filename = Path(args.path)

    # print(df)

    sort_by = [column for column in df.columns if column.find("Action") == -1]
    actions = [column for column in df.columns if column.find("Action") != -1]
    state_features = [
        column for column in df.columns if column.find("Action") == -1
    ]
    action_counters = [0 for _ in range(len(actions))]

    df = df.sort_values(by=sort_by)
    df.reset_index(drop=True, inplace=True)

    print("-"*80)
    print(" | ".join(sort_by+['Action']))
    print("-"*80)

    df['best'] = df[actions].idxmax(axis=1)
    df['explored'] = df[actions].apply(
        lambda row: not all([val == 0. for val in row]), axis=1)

    explored_res = df.explored.value_counts()
    explored_res.rename("State exploration", inplace=True)
    explored_res.index = explored_res.index.map(
        {True: 'Explored', False: 'Not explored'}
    )
    action_stats = df.best.value_counts()
    action_stats.rename("Action distribution", inplace=True)

    print(f"{STATUS_ARROW}Plot explored states pie")
    fig_action_general, (axes_explored_states, axes_actions) = plt.subplots(
        nrows=1, ncols=2, figsize=(16, 8))
    explored_res.plot(
        ax=axes_explored_states,
        kind="pie",
        autopct='%.2f%%'
    ).legend()
    axes_explored_states.legend(loc='upper right')
    print(f"{STATUS_ARROW}Plot action states pie")
    action_stats.plot(
        ax=axes_actions,
        kind="pie",
        autopct='%.2f%%'
    ).legend()
    axes_actions.legend(loc='upper right')
    fig_action_general.tight_layout()
    fig_action_general.savefig(
        f"{filename.name}.actionGeneral.png",
        dpi=300,
        bbox_inches="tight",
        pad_inches=0.24
    )

    fig_actions, action_axes = plt.subplots(
        nrows=len(actions), ncols=3, figsize=(
            8*len(actions), 8*len(state_features)
        )
    )
    for idx, action in enumerate(actions):
        cur_axes = action_axes[idx]
        for ax in cur_axes:
            ax.set_title(action)
        cur_data = df[df.best == action][state_features]
        for col_idx, column in enumerate(state_features):
            print(f"{STATUS_ARROW}Plot column {column} of action {action} pie")
            cur_data[column].value_counts().plot(
                ax=cur_axes[col_idx],
                kind="pie",
                autopct='%.2f%%',
            )
            cur_axes[col_idx].legend(loc='upper right')
    fig_actions.tight_layout()
    fig_actions.savefig(
        f"{filename.name}.actions.png",
        dpi=300,
        bbox_inches="tight",
        pad_inches=0.24
    )

    print(f"{STATUS_ARROW}Plot heatmap")
    fig_table_map, (axes_bars, axes_heatmap) = plt.subplots(
        nrows=1, ncols=2, figsize=(16, 32),
        sharey=True, constrained_layout=True,
    )
    sns.heatmap(
        ax=axes_heatmap,
        data=df[actions],
        cbar_kws={"orientation": "horizontal"},
        annot=True,
    )
    axes_heatmap.invert_yaxis()

    print(f"{STATUS_ARROW}Plot bars")
    df[actions].plot(
        ax=axes_bars,
        kind="barh", stacked=False, width=0.5,
        align='edge',
        ylim=(
            df[actions].min().min(),
            df[actions].max().max()
        ),
    ).legend()
    axes_bars.legend(loc='lower left')
    axes_bars.axvline(x=0, c="k")
    state_labels = [
        " | ".join(
            [
                f"{state_features[idx-1]}:{row[idx]}"
                for idx in range(1, len(row))
            ]
        ) for row in df[state_features].itertuples()
    ]
    # axes_bars.set_yticks(np.arange(len(df.index))+0.5)
    axes_bars.set_yticklabels(
        state_labels,
        fontdict={
            'fontsize': 16,
            'verticalalignment': 'bottom'
        },
    )
    axes_bars.grid()

    # fig_table_map.tight_layout()
    fig_table_map.savefig(
        f"{filename.name}.tableMap.png",
        dpi=300,
        bbox_inches="tight",
        pad_inches=0.24
    )


if __name__ == "__main__":
    main()
