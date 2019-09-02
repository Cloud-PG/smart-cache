import gzip
import json
import math
import os
import pickle
from itertools import cycle
from typing import Dict, List

import pandas as pd
from bokeh.layouts import column, row
from bokeh.palettes import Category10
from bokeh.models import ColumnDataSource, LabelSet, Span
from bokeh.plotting import figure, output_file, save
from bokeh.transform import cumsum
from flask import Flask, escape, jsonify, request
from tqdm import tqdm

BASE_PATH = "plot_server_app"

TABLES = {
    'hit_rate': {},
    'size': {},
    'written_data': {},
    'read_on_hit': {},
}

WINDOW_INFO = {}

TABLE_COLORS = {}

COLORS = cycle(Category10[10])


def get_size_from_name(name: str) -> str:
    string = name.split("_")
    for part in string:
        if part.find("T") != -1:
            return part
    return 'unknown'


def get_size_from_cache_list(name, cache_list):
    for cache in cache_list:
        if name in cache:
            return cache[name]
    return 0.


def create_map(size: int, filenames: list, sizes: list):
    element_idx = iter(range(len(filenames)))
    for _x_ in reversed(range(size + 1)):
        for _y_ in range(size + 1):
            try:
                cur_idx = next(element_idx)
            except StopIteration:
                return
            if cur_idx < len(filenames):
                yield (filenames[cur_idx], (_y_, _x_, sizes[cur_idx]))


def plot_info_window(window: int, filename: str, **kwargs):
    # Empty plot with log scale:
    # - https://github.com/bokeh/bokeh/issues/6671
    global TABLES, WINDOW_INFO, TABLE_COLORS
    data = {}
    filters = kwargs.get('filters', [])

    for cache_name, info in WINDOW_INFO.items():
        size = get_size_from_name(cache_name)
        if filters:
            if size not in filters:
                continue
        if size not in data:
            data[size] = {}
        if cache_name.lower().find('lru') != -1:
            lru = info[window]['cache']
            data[size]['lru'] = lru
        else:
            data[size][cache_name] = {
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

    for size, caches in data.items():
        for cache_name, cur_data in [
            (cache_name, cur_data)
            for cache_name, cur_data in caches.items()
            if cache_name != 'lru'
        ]:
            filenames = [key for key, _ in sorted(
                cur_data['weights'].items(),
                key=lambda elm: elm[1],
                reverse=True)
            ]
            sizes = [
                get_size_from_cache_list(
                    filename, [cur_data['cache'], caches['lru']])
                for filename in filenames
            ]
            tot = len(filenames)
            map_size = math.ceil(math.sqrt(tot))
            map_ = dict((key, value)
                        for key, value in create_map(map_size, filenames, sizes)
                        )

            dots_plot = figure(
                title=f"File weights",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 640),
                plot_height=kwargs.get('plot_height', 400),
                x_range=(0, map_size),
                y_range=(0, map_size),
                x_axis_type=None,
                y_axis_type=None,
            )

            dots_plot.circle(
                [map_[filename][0] for filename in filenames],
                [map_[filename][1] for filename in filenames],
                color="gainsboro",
                size=1.
            )

            dots_plot.circle(
                [map_[filename][0]
                    for filename in filenames if filename in cur_data['cache']],
                [map_[filename][1]
                    for filename in filenames if filename in cur_data['cache']],
                fill_color="blue",
                fill_alpha=1.0,
                size=2.5
            )

            dots_plot.circle(
                [map_[filename][0]
                    for filename in filenames if filename in caches['lru']],
                [map_[filename][1]
                    for filename in filenames if filename in caches['lru']],
                fill_color="red",
                fill_alpha=0.9,
                size=2.5
            )

            # Pie
            plot_figure_pie = figure(
                plot_width=kwargs.get('plot_width', 640),
                plot_height=kwargs.get('plot_height', 400),
                title="% all file seen",
                tools="box_zoom,pan,reset,save",
                tooltips="@type: @value",
                x_range=(-0.5, 1.0)
            )

            two_pi = 2.0*3.14
            pie_data = pd.Series({
                'all files': len(filenames),
                'weighted': len(
                    [filename for filename in filenames if filename in cur_data['cache']]),
                'cache': len(
                    [filename for filename in filenames if filename in caches['lru']]),
                'both': len(
                    [filename for filename in filenames if filename in cur_data['cache'] or filename in caches['lru']])
            }).reset_index(name='value').rename(
                columns={'index': 'type'})
            pie_data['angle'] = pie_data['value'] / \
                pie_data['value'].sum() * two_pi
            pie_data['color'] = ['gainsboro', 'blue', 'red', 'green']

            plot_figure_pie.wedge(
                x=0, y=0, radius=0.4,
                start_angle=cumsum('angle', include_zero=True),
                end_angle=cumsum('angle'),
                line_color="white",
                fill_color='color',
                legend='type',
                source=pie_data
            )

            pie_data['value'] = (
                pie_data['value'] / pie_data['value'].sum()
            ) * 100.

            pie_data["value"] = pie_data['value'].apply(lambda elm: f"{elm:0.2f}%")
            pie_data["value"] = pie_data["value"].str.pad(42, side="left")
            labels = LabelSet(
                x=0, y=0, text='value', level='glyph',
                angle=cumsum('angle', include_zero=True), 
                source=ColumnDataSource(pie_data),
                # render_mode='canvas'
            )

            plot_figure_pie.add_layout(labels)

            plot_figure_pie.axis.axis_label = None
            plot_figure_pie.axis.visible = False
            plot_figure_pie.grid.grid_line_color = None

            filenames_common = list(sorted([
                filename for filename in filenames
                if filename in cur_data['cache'] or filename in caches['lru']
            ],
                key=lambda name: get_size_from_cache_list(
                    name, [cur_data['cache'], caches['lru']]
            ),
                reverse=True
            ))

            # Sizes plot
            plot_figure_size_weighted_cache = figure(
                title=f"Sizes of files in {cache_name} (sort by size)",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 640),
                plot_height=kwargs.get('plot_height', 400),
                x_range=filenames_common,
                y_range=(1, int(max(cur_data['weights'].values())) + 10),
                x_axis_type=None,
                y_axis_type=kwargs.get('y_axis_type', 'auto'),
            )

            plot_figure_size_weighted_cache.vbar(
                filenames_common,
                top=[
                    get_size_from_cache_list(
                        filename, [cur_data['cache'], caches['lru']])
                    for filename in filenames_common
                ],
                color="gainsboro",
                width=1.0,
                bottom=0.01 if kwargs.get(
                    'y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
            )

            # Size of weighted cache files
            plot_figure_size_weighted_cache.vbar(
                filenames_common,
                top=[
                    cur_data['cache'][filename]
                    if filename in cur_data['cache'] else 0
                    for filename in filenames_common
                ],
                color="blue",
                width=1.0,
                bottom=0.01 if kwargs.get(
                    'y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
            )

            # Sizes plot
            plot_figure_size_cache = figure(
                title=f"Sizes of files not in {cache_name} (sort by size)",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 640),
                plot_height=kwargs.get('plot_height', 400),
                x_range=filenames_common,
                y_range=(1, int(max(cur_data['weights'].values())) + 10),
                x_axis_type=None,
                y_axis_type=kwargs.get('y_axis_type', 'auto'),
            )

            plot_figure_size_cache.vbar(
                filenames_common,
                top=[
                    get_size_from_cache_list(
                        filename, [cur_data['cache'], caches['lru']])
                    for filename in filenames_common
                ],
                color="gainsboro",
                width=1.0,
                bottom=0.01 if kwargs.get(
                    'y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
            )

            # Size of cache files
            plot_figure_size_cache.vbar(
                filenames_common,
                top=[
                    caches['lru'][filename]
                    if filename in caches['lru'] else 0
                    for filename in filenames_common
                ],
                color="red",
                width=1.0,
                bottom=0.01 if kwargs.get(
                    'y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
            )

            figures.append(row(column(dots_plot, plot_figure_pie), column(
                plot_figure_size_weighted_cache, plot_figure_size_cache)))

    save(column(*figures))


def plot_line(table_name: str, filename: str, **kwargs):
    global TABLES, WINDOW_INFO, TABLE_COLORS
    # output to static HTML file
    output_file(
        os.path.join(
            BASE_PATH,
            filename
        ),
        kwargs.get('title', "Line plot"),
        mode="inline"
    )
    filters = kwargs.get('filters', [])

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

    if table_name != 'ratio':
        for name, values in TABLES[table_name].items():
            if filters:
                size = get_size_from_name(name)
                if size not in filters:
                    continue
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
    elif table_name == 'ratio':
        data = {}
        for cur_table_name in ['written_data', 'read_on_hit']:
            for name, values in TABLES[cur_table_name].items():
                if name not in data:
                    data[name] = {
                        'written_data': [],
                        'read_on_hit': []
                    }
                if filters:
                    size = get_size_from_name(name)
                    if size not in filters:
                        continue
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
                data[name][cur_table_name] = [
                    value for bucket in values for value in bucket]
                if name not in TABLE_COLORS:
                    TABLE_COLORS[name] = next(COLORS)

        for name, values in data.items():
            plot_figure.line(
                range(len(values['read_on_hit'])),
                [value / values['written_data'][idx]
                    for idx, value in enumerate(values['read_on_hit'])],
                legend=name,
                color=TABLE_COLORS[name],
                line_width=2.
            )

    if v_lines:
        plot_figure.renderers.extend(v_lines)

    plot_figure.legend.location = "top_left"
    plot_figure.legend.click_policy = "hide"

    save(plot_figure)


app = Flask(
    __name__,
    static_folder=os.path.abspath(BASE_PATH)
)


@app.route('/cache/service/status', methods=['GET'])
def service_status():
    global TABLES, WINDOW_INFO, TABLE_COLORS
    return jsonify({
        'status': "online",
        'num_cache_hit_rates': len(TABLES['hit_rate']),
        'num_cache_sizes': len(TABLES['size']),
        'num_cache_written_data': len(TABLES['written_data']),
        'num_cache_window_info': len(WINDOW_INFO),
        'len_window_cache_hit_rate': [(f'[{cache_name}][window {win_idx}][len {len(window)}]')for cache_name, cache in TABLES['hit_rate'].items() for win_idx, window in enumerate(cache)],
        'len_window_cache_size': [(f'[{cache_name}][window {win_idx}][len {len(window)}]')for cache_name, cache in TABLES['size'].items() for win_idx, window in enumerate(cache)],
        'len_window_cache_written_data': [(f'[{cache_name}][window {win_idx}][len {len(window)}]')for cache_name, cache in TABLES['written_data'].items() for win_idx, window in enumerate(cache)],
        'len_window_cache_info': [(f'[{cache_name}][window {win_idx}][len {len(window)}]')for cache_name, cache in WINDOW_INFO.items() for win_idx, window in enumerate(cache)],
    })


@app.route('/cache/plot/<string:table_name>', methods=['GET'])
def table_plot(table_name: str):
    filters = request.args.get('filter')
    if filters:
        filters = filters.split(',')

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
    elif table_name == "read_on_hit":
        kwargs['y_axis_label'] = "Data read on hit (MB)"
        kwargs['y_axis_type'] = "log"

    plot_line(
        table_name,
        f"plot_{table_name}.html",
        title=f"Cache {table_name}",
        filters=filters,
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
    global TABLES
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
    filters = request.args.get('filter')
    if filters:
        filters = filters.split(',')
    plot_info_window(
        window,
        f'plot_info_w{window}.html',
        title=f"Info window {window}",
        y_axis_type="log",
        filters=filters
    )
    return app.send_static_file(f'plot_info_w{window}.html')


@app.route('/cache/plot', methods=['DELETE'])
def delete_plots(window: int):
    global TABLES, WINDOW_INFO, TABLE_COLORS
    TABLES = {
        'hit_rate': {},
        'size': {},
        'written_data': {},
        'read_on_hit': {},
    }

    WINDOW_INFO = {}

    TABLE_COLORS = {}
    return f'Deleted all data'


@app.route('/cache/info/<string:cache_name>/<int:window>', methods=['POST', 'PUT'])
def cache_info(cache_name: str, window: int):
    global WINDOW_INFO
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
    global TABLES
    cur_table = TABLES[table_name]

    if cache_name not in cur_table:
        cur_table[cache_name] = []
    cur_line = cur_table[cache_name]

    try:
        cur_line[window]
    except IndexError:
        cur_line.append([])

    try:
        cur_line[window][req_idx] = value
    except IndexError:
        cur_line[window].append(value)

    if force_save:
        save_table(table_name, cur_table)

    return f"Inserted value {value} with index {req_idx} in window {window}"


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

    print("[Loading data...]")
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

    print("[Loading data done!]")
    app.run(
        host="0.0.0.0",
        port=5524
    )
