import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from .data import LogDeleteEvaluator
from .utils import LAYOUT


def plot_num_miss_after_del(results: list):
    fig = go.Figure(layout=LAYOUT)
    fig_cum = go.Figure(layout=LAYOUT)

    for name, delEvaluatorList in results:
        if not all([isinstance(elm, LogDeleteEvaluator) for elm in delEvaluatorList]):
            raise Exception(
                "ERROR: result element are not all log del evaluator")

        x = [item.tick for item in delEvaluatorList]
        y = np.array(
            [item.total_num_req_after_delete for item in delEvaluatorList])
        cumulative = np.cumsum(y)

        print(x)
        print(y)
        print(cumulative)

        fig.add_trace(
            go.Scatter(
                x=x, y=y,
                mode='lines',
                name=name,
            ),
        )
        fig_cum.add_trace(
            go.Scatter(
                x=x, y=cumulative,
                mode='lines',
                name=name,
            ),
        )

    fig.update_layout(
        title="# Miss after delete",
        xaxis_title='tick',
        yaxis_title='#',
    )
    fig_cum.update_layout(
        title="Cumulative # Miss after delete",
        xaxis_title='tick',
        yaxis_title='#',
    )

    fig.write_html(
        "./test_num_miss.html",
        include_plotlyjs=True,
    )
    fig_cum.write_html(
        "./test_cumulative.html",
        include_plotlyjs=True,
    )
