import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .dashboard import LAYOUT
from .data import get_all_metric_values
from .utils import LogDeleteEvaluator


from biokit.viz import corrplot


import matplotlib.pyplot as plt


def metric_corr(results: list):
    df = get_all_metric_values(results)

    # ref: https://nbviewer.jupyter.org/github/biokit/biokit/blob/master/notebooks/viz/corrplot.ipynb
    c = corrplot.Corrplot(df)
    # c.plot(method="text", colorbar=False, fontsize=12, rotation=45)
    c.plot(method="square", colorbar=True, shrink=0.9, rotation=45)
    plt.show()

    # Using plotly
    # fig = px.imshow(df.corr(method="spearman"))
    # fig.show()


def plot_num_miss_after_del(results: list, output_filename: str = ""):
    fig = go.Figure(layout=LAYOUT)
    fig_cum = go.Figure(layout=LAYOUT)

    res = {}

    for name, delEvaluator in results:
        if not isinstance(delEvaluator, LogDeleteEvaluator):
            raise Exception("ERROR: result element is not a log del evaluator")

        if name not in res:
            res[name] = {"x": [], "y": [], "cumulative": []}

        res[name]["x"].append(delEvaluator.tick)
        res[name]["y"].append(delEvaluator.total_num_req_after_delete)

    for name, obj in res.items():
        obj["x"] = np.array(obj["x"])
        obj["y"] = np.array(obj["y"])
        obj["cumulative"] = np.cumsum(obj["y"])

        x = obj["x"]
        y = obj["y"]
        cumulative = obj["cumulative"]

        # print(x)
        # print(y)
        # print(cumulative)

        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="lines",
                name=name,
            ),
        )
        fig_cum.add_trace(
            go.Scatter(
                x=x,
                y=cumulative,
                mode="lines",
                name=name,
            ),
        )

    fig.update_layout(
        title="# Miss after delete",
        xaxis_title="tick",
        yaxis_title="#",
    )
    fig_cum.update_layout(
        title="Cumulative # Miss after delete",
        xaxis_title="tick",
        yaxis_title="#",
    )

    filename = "-" + output_filename if output_filename else ""
    fig.write_html(
        f"num_miss{filename}.html",
        include_plotlyjs=True,
    )
    fig_cum.write_html(
        f"num_miss_cumulative{filename}.html",
        include_plotlyjs=True,
    )


def _get_bins(data: list, bins: list, tot: int = 0):
    counts = {}
    prevBin = 0
    for bin_ in bins:
        counts[bin_] = len([elm for elm in data if prevBin < elm <= bin_])
        prevBin = bin_
    max_val = bins[-1]
    counts["max"] = len([elm for elm in data if elm > max_val])
    if tot == 0:
        tot = len(data)
    for key, value in counts.items():
        if value > 0.0:
            counts[key] = (value / tot) * 100.0
    return list(counts.values()), list(counts.keys())


def plot_miss_freq(results: list, output_filename: str = ""):
    all_plots = []

    for name, (freq_deleted, freq_skip) in results:
        tot = len(freq_deleted) + len(freq_skip)
        counts_del, bins_del = _get_bins(freq_deleted, bins=[1, 2, 6], tot=tot)
        counts_skip, bins_skip = _get_bins(freq_skip, bins=[1, 2, 6], tot=tot)

        all_plots.append(
            {
                "deleted": (bins_del, counts_del),
                "skipped": (bins_skip, counts_skip),
                "title": name,
            }
        )

    all_plots = list(sorted(all_plots, key=lambda elm: elm["title"]))

    fig = make_subplots(
        rows=len(all_plots),
        cols=1,
        subplot_titles=[elm["title"] for elm in all_plots],
        shared_yaxes=True,
    )

    fig.update_layout(
        title="Frequency distribution of miss files",
        paper_bgcolor="rgb(255,255,255)",
        plot_bgcolor="rgb(255,255,255)",
        yaxis={"gridcolor": "black"},
        xaxis={"gridcolor": "black"},
        height=320 * len(all_plots),
        width=1280,
        showlegend=False,
    )

    for idx, values in enumerate(all_plots, 1):
        bins_del, counts_del = values["deleted"]
        bins_skip, counts_skip = values["skipped"]
        fig.add_trace(
            go.Bar(name="deleted", x=bins_del, y=counts_del),
            row=idx,
            col=1,
        )
        fig.add_trace(
            go.Bar(name="skipped", x=bins_skip, y=counts_skip),
            row=idx,
            col=1,
        )
        fig.update_xaxes(title="freq. class", type="category", row=idx, col=1)
        fig.update_yaxes(
            title="%",
            type="log",
            row=idx,
            col=1,
            showgrid=True,
        )

    filename = "-" + output_filename if output_filename else ""
    fig.write_html(
        f"miss_freq{filename}.html",
        include_plotlyjs=True,
    )
