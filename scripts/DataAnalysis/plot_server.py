from bokeh.plotting import figure, output_file, save
from bokeh.models import Span
from typing import Dict, List

from flask import Flask, escape, request
import os
import pickle

BASE_PATH = "plot_server_app"

TABLES = {
    'hit_rate': {},
    'size': {},
    'written_data': {}
}

TABLE_COUNTERS = {
    'hit_rate': 0,
    'size': 0,
    'written_data': 0
}

TABLE_FLAGS = {
    'hit_rate_dirty': False,
    'size_dirty': False,
    'written_data_dirty': False
}


def plot_line(table_name: str, filename: str, **kwargs):
    # output to static HTML file
    output_file(
        os.path.join(
            BASE_PATH,
            filename
        ),
        kwargs.get('title', "Line plot")
    )

    # create a new plot
    plot_figure = figure(
        tools="box_zoom,reset,save",
        y_axis_type=kwargs.get('y_axis_type', 'auto'),
        title=kwargs.get('title', ''),
        x_axis_label=kwargs.get('x_axis_label', ''),
        y_axis_label=kwargs.get('y_axis_label', ''),
        y_range=kwargs.get('y_range', None),
        plot_width=kwargs.get('plot_width', 800),
        plot_height=kwargs.get('plot_height', 600)
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
                        line_color='black', line_width=2
                    )
                    for vl_index in v_lines
                ]
            else:
                v_lines = []

        points = [value for bucket in values for value in bucket]
        plot_figure.line(
            range(len(points)),
            points,
            legend=name
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
    if TABLE_FLAGS[f'{table_name}_dirty']:
        save_table(f"{table_name}", TABLES[table_name])
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
        TABLE_FLAGS[f'{table_name}_dirty'] = False

    return app.send_static_file(f'plot_{table_name}.html')


@app.route('/cache/<string:table_name>/<string:cache_name>/<int:window>/<int:req_idx>/<float:value>', methods=['POST', 'PUT'])
def table_insert(table_name: str, cache_name: str, window: int, req_idx: int, value: float):
    return insert_line_in_table(f'{table_name}', cache_name, window, req_idx, value)


def insert_line_in_table(table_name: str, cache_name: str, window: int, req_idx: int, value: float):
    TABLE_COUNTERS[table_name] += 1
    TABLE_FLAGS[f'{table_name}_dirty'] = True

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

    if TABLE_COUNTERS[table_name] >= 1000:
        save_table(table_name, cur_table)
        TABLE_FLAGS[f'{table_name}_dirty'] = False

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
            TABLE_FLAGS[f'{table}_dirty'] = True

    app.run(
        host="0.0.0.0",
        port=5524
    )
