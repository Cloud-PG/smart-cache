import argparse
from pathlib import Path

import graphviz
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn import tree

from..utils import STATUS_ARROW, search_runs


def main():
    parser = argparse.ArgumentParser(
        "qTable inspector", description="Inspect the Q-Learning results"
    )

    parser.add_argument('path', help='Path of qTable csv')

    args, _ = parser.parse_known_args()

    df = pd.read_csv(args.path)
    # print(df)

    filename = Path(args.path)

    print(f"{STATUS_ARROW}Plot value function")
    tableType = filename.name.split(".csv")[0].rsplit(
        "_", 1)[1].replace("Qtable", "").strip()
    prev_runs = search_runs(filename.resolve().parent)
    value_function_runs = []
    value_function_index = None
    for file_ in prev_runs:
        cur_df = pd.read_csv(file_)
        if value_function_index is None:
            # Full date example: 2020-01-01 00:00:00 +0100 CET
            value_function_index = pd.to_datetime(
                cur_df.date.apply(lambda elm: elm.split()[0]),
                format="%Y-%m-%d"
            )
        if tableType == "addition":
            value_function_runs.append(cur_df['Addition value function'])
        elif tableType == "eviction":
            value_function_runs.append(cur_df['Eviction value function'])
        else:
            raise Exception(f"ERROR: wrong type {tableType}...")
    value_fun_df = pd.DataFrame({
        f'run {idx}': values
        for idx, values in enumerate(value_function_runs)
    })
    value_fun_df.set_index(value_function_index, drop=True, inplace=True)
    cur_ax = value_fun_df.plot.line(
        figsize=(16, 8),
        # width=1.0,
    )
    cur_ax.grid('on', which='both')
    cur_ax.legend()
    cur_ax.axhline(y=0, color='k', linestyle='-')
    fig_value_function = cur_ax.get_figure()
    fig_value_function.savefig(
        filename.parent.joinpath(f"{filename.name}.valueFunction.png"),
        dpi=300,
        bbox_inches="tight",
        pad_inches=0.24
    )

    sort_by = [column for column in df.columns if column.find("Action") == -1]
    actions = [column for column in df.columns if column.find("Action") != -1]
    state_features = [
        column for column in df.columns if column.find("Action") == -1
    ]

    df = df.sort_values(by=sort_by)
    df.reset_index(drop=True, inplace=True)

    df['best'] = df[actions].idxmax(axis=1)
    df['explored'] = df[actions].apply(
        lambda row: not all([val == 0. for val in row]), axis=1)

    df_dtree_X = df[state_features].copy()
    for column in state_features:
        df_dtree_X[column].replace(to_replace={
            'max': max(pd.to_numeric(df_dtree_X[column], errors='coerce')) * 2
        }, inplace=True)
    df_dtree_labels = df['best'].copy()
    df_dtree_labels.replace(to_replace={
        (key, actions.index(key)) for key in actions
    })
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(df_dtree_X, df_dtree_labels)
    dot_data = tree.export_graphviz(
        clf, out_file=None,
        feature_names=state_features,
        class_names=actions,
        filled=True,
        rounded=True,
        special_characters=True,
        leaves_parallel=True,
        impurity=True,
        proportion=True,
    )
    graph = graphviz.Source(dot_data)
    print(f"{STATUS_ARROW}Plot decision tree")
    graph.render(filename.parent.joinpath(f"{filename.name}.decisionTree"))

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
        filename.parent.joinpath(f"{filename.name}.actions.png"),
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
            # 'fontsize': 16,
            'verticalalignment': 'bottom'
        },
    )
    axes_bars.grid()

    # fig_table_map.tight_layout()
    fig_table_map.savefig(
        filename.parent.joinpath(f"{filename.name}.tableMap.png"),
        dpi=300,
        bbox_inches="tight",
        pad_inches=0.24
    )


if __name__ == "__main__":
    main()
