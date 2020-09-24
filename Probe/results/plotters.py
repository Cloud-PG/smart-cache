import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from .dashboard import LAYOUT
from .utils import LogDeleteEvaluator


def plot_num_miss_after_del(results: list):
    fig = go.Figure(layout=LAYOUT)
    fig_cum = go.Figure(layout=LAYOUT)

    res = {}

    for name, delEvaluator in results:
        if not isinstance(delEvaluator, LogDeleteEvaluator):
            raise Exception(
                "ERROR: result element is not a log del evaluator")

        if name not in res:
            res[name] = {'x': [], 'y': [], 'cumulative': []}

        res[name]['x'].append(delEvaluator.tick)
        res[name]['y'].append(delEvaluator.total_num_req_after_delete)

    for name, obj in res.items():
        obj['x'] = np.array(obj['x'])
        obj['y'] = np.array(obj['y'])
        obj['cumulative'] = np.cumsum(obj['y'])

        x = obj['x']
        y = obj['y']
        cumulative = obj['cumulative']

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
