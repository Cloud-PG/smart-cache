import gzip
import json
import os
import pickle
from itertools import cycle
from typing import Dict, List

from bokeh.models import Span
from bokeh.layouts import column
from bokeh.plotting import figure, output_file, save
from flask import Flask, escape, request
from tqdm import tqdm

BASE_PATH = "plot_server_app"

TABLES = {
    'hit_rate': {},
    'size': {},
    'written_data': {}
}

WINDOW_INFO = {}

TABLE_COLORS = {}

COLORS = cycle(["red", "mediumblue", "green", "purple", "black", "yellow"])


def plot_info_window(window: int, filename: str, **kwargs):
    data = {}
    lru = {}
    for cache_name, info in WINDOW_INFO.items():
        if cache_name.lower().find('lru') != -1:
            lru = info[window]['cache']
        else:
            data[cache_name] = {
                'weights': info[window]['weights'],
                'cache': info[window]['cache']
            }

    output_file(
        os.path.join(
            BASE_PATH,
            filename
        ),
        kwargs.get('title', "Chart"),
        mode="inline"
    )

    figures = []

    for cache_name, cur_data in data.items():
        filenames = [key for key, _ in sorted(
            cur_data['weights'].items(),
            key=lambda elm: elm[1],
            reverse=True
        )
        ]
        plot_figure = figure(
            title=f"{cache_name} window {window}",
            tools="box_zoom,pan,reset,save",
            plot_width=kwargs.get('plot_width', 1280),
            plot_height=kwargs.get('plot_height', 800),
            x_range=filenames,
            y_range=(1, int(max(cur_data['weights'].values())) + 10),
            x_axis_type=None,
            y_axis_type=kwargs.get('y_axis_type', 'auto'),
        )

        # Empty plot with log scale:
        # - https://github.com/bokeh/bokeh/issues/6671

        plot_figure.vbar(
            filenames,
            top=[
                cur_data['weights'][filename]
                for filename in filenames
            ],
            color="gray",
            width=1.0,
            bottom=0.01 if kwargs.get('y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
        )

        plot_figure.vbar(
            filenames,
            top=[
                cur_data['weights'][filename] * 0.75
                if filename in cur_data['cache'] else 0
                for filename in filenames
            ],
            color="blue",
            width=1.0,
            bottom=0.01 if kwargs.get('y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
        )

        plot_figure.vbar(
            filenames,
            top=[
                cur_data['weights'][filename] * 0.5
                if filename in lru else 0
                for filename in filenames
            ],
            color="red",
            width=1.0,
            bottom=0.01 if kwargs.get('y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
        )

        figures.append(plot_figure)

    save(column(*figures))


def plot_line(table_name: str, filename: str, **kwargs):
    # output to static HTML file
    output_file(
        os.path.join(
            BASE_PATH,
            filename
        ),
        kwargs.get('title', "Line plot"),
        mode="inline"
    )

    # create a new plot
    plot_figure = figure(
        tools="box_zoom,pan,reset,save",
        y_axis_type=kwargs.get('y_axis_type', 'auto'),
        title=kwargs.get('title', ''),
        x_axis_label=kwargs.get('x_axis_label', ''),
        y_axis_label=kwargs.get('y_axis_label', ''),
        y_range=kwargs.get('y_range', None),
        plot_width=kwargs.get('plot_width', 1280),
        plot_height=kwargs.get('plot_height', 800)
    )

    v_lines = []

    for name, values in TABLES[table_name].items():
        if not v_lines:
            v_lines = [len(elm) for elm in values]
            for idx in range(1, len(v_lines)):
                v_lines[idx] += v_lines[idx-1]
            if len(v_lines) > 1:
                v_lines = [
                    Span(
                        location=vl_index, dimension='height',
                        line_color='black', line_width=1.2
                    )
                    for vl_index in v_lines
                ]
            else:
                v_lines = []

        points = [value for bucket in values for value in bucket]
        if name not in TABLE_COLORS:
            TABLE_COLORS[name] = next(COLORS)
        plot_figure.line(
            range(len(points)),
            points,
            legend=name,
            color=TABLE_COLORS[name],
            line_width=2.
        )

    if v_lines:
        plot_figure.renderers.extend(v_lines)

    save(plot_figure)


app = Flask(
    __name__,
    static_folder=os.path.abspath(BASE_PATH)
)


@app.route('/cache/plot/<string:table_name>', methods=['GET'])
def table_plot(table_name: str):
    kwargs = {
        'x_axis_label': "Requests"
    }
    if table_name == "hit_rate":
        kwargs['y_range'] = (0, 100)
        kwargs['y_axis_label'] = "Hit rate %"
    elif table_name == "size":
        kwargs['y_axis_label'] = "Size (MB)"
        kwargs['y_axis_type'] = "log"
    elif table_name == "written_data":
        kwargs['y_axis_label'] = "Written data (MB)"
        kwargs['y_axis_type'] = "log"

    plot_line(
        table_name,
        f"plot_{table_name}.html",
        title=f"Cache {table_name}",
        **kwargs
    )

    return app.send_static_file(f'plot_{table_name}.html')


@app.route('/cache/<string:table_name>/<string:cache_name>/<int:window>/<int:req_idx>/<float:value>', methods=['POST', 'PUT'])
def table_insert(table_name: str, cache_name: str,
                 window: int, req_idx: int, value: float):
    return insert_line_in_table(
        f'{table_name}', cache_name, window, req_idx, value, force_save=True
    )


@app.route('/cache/update/<string:cache_name>/<int:window>', methods=['POST', 'PUT'])
def cache_update(cache_name: str, window: int):
    data = request.data
    obj = json.loads(gzip.decompress(data))
    for table_name, list_ in obj.items():
        for req_idx, value in tqdm(
            list_,
            desc=f"Insert values in window {window} of {table_name} for {cache_name}",
            ascii=True
        ):
            insert_line_in_table(
                f'{table_name}', cache_name, window, req_idx, value
            )
        save_table(f"{table_name}", TABLES[table_name])
    return f"Updated window {window} of {cache_name}"


@app.route('/cache/plot/info/<int:window>', methods=['GET'])
def cache_info_plot(window: int):
    plot_info_window(
        window,
        f'plot_info_w{window}.html',
        title=f"Info window {window}",
        y_axis_type="log"
    )
    return app.send_static_file(f'plot_info_w{window}.html')


@app.route('/cache/info/<string:cache_name>/<int:window>', methods=['POST', 'PUT'])
def cache_info(cache_name: str, window: int):
    data = request.data
    obj = json.loads(gzip.decompress(data))

    if cache_name not in WINDOW_INFO:
        WINDOW_INFO[cache_name] = []

    if window < len(WINDOW_INFO[cache_name]):
        WINDOW_INFO[cache_name][window] = obj
    else:
        WINDOW_INFO[cache_name].append(obj)

    save_table("cache_info", WINDOW_INFO)

    return f"Updated cache {cache_name} info of window {window}"


def insert_line_in_table(table_name: str, cache_name: str,
                         window: int, req_idx: int, value: float, force_save: bool = False):
    cur_table = TABLES[table_name]

    if cache_name not in cur_table:
        cur_table[cache_name] = []
    cur_line = cur_table[cache_name]

    try:
        cur_line[window]
    except IndexError:
        cur_line.append([])

    if req_idx < len(cur_line[window]):
        cur_line[window][req_idx] = value
        result = f"Updated index {req_idx} of window {window} with the value {value}"
    else:
        cur_line[window].append(value)
        result = f"Inserted value {value} with index {req_idx} in window {window}"

    if force_save:
        save_table(table_name, cur_table)

    return result


def save_table(table_name, table):
    with open(os.path.join(
        BASE_PATH,
        f"{table_name}.pickle"
    ), 'wb') as table_file:
        pickle.dump(
            table,
            table_file,
            pickle.HIGHEST_PROTOCOL
        )


def load_table(table_name):
    if os.path.exists(
        os.path.join(
            BASE_PATH,
            f"{table_name}.pickle"
        )
    ):
        with open(os.path.join(
            BASE_PATH,
            f"{table_name}.pickle"
        ), "rb") as input_file:
            return pickle.load(input_file)


if __name__ == '__main__':
    os.makedirs(BASE_PATH, exist_ok=True)

    for table in TABLES:
        loaded_table = load_table(table)
        if loaded_table:
            TABLES[table] = loaded_table

    if os.path.exists(
        os.path.join(
            BASE_PATH,
            f"cache_info.pickle"
        )
    ):
        with open(os.path.join(
            BASE_PATH,
            f"cache_info.pickle"
        ), "rb") as input_file:
            WINDOW_INFO = pickle.load(input_file)

    app.run(
        host="0.0.0.0",
        port=5524
    )
