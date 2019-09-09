import gzip
import json
import math
import os
import pickle
from itertools import cycle

import numpy as np
import pandas as pd
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, LabelSet, Span
from bokeh.palettes import Category10
from bokeh.plotting import figure, output_file, save
from bokeh.transform import cumsum
from flask import Flask, jsonify, request
from tqdm import tqdm

BASE_PATH = "plot_server_app"

TABLES = {
    'hit_rate': {},
    'weighted_hit_rate': {},
    'hit_over_miss': {},
    'size': {},
    'written_data': {},
    'read_on_hit': {},
}

WINDOW_INFO = {}

TABLE_COLORS = {}

MAX_BINS = 100


def update_colors(new_name: str):
    global TABLE_COLORS
    names = list(TABLE_COLORS.keys()) + [new_name]
    colors = cycle(Category10[10])
    for name in sorted(names):
        cur_color = next(colors)
        TABLE_COLORS[name] = cur_color


def fill_hit_miss_bins(n_bins: int, edges: list, values: list) -> tuple:
    hist_hit_bin = [0 for _ in range(n_bins)]
    hist_miss_bin = [0 for _ in range(n_bins)]

    tot_hits = sum(n_hit for _, n_hit, _ in values)
    tot_miss = sum(n_miss for _, _, n_miss in values)

    for value, n_hit, n_miss in values:
        for idx in range(n_bins - 1):
            lower = edges[idx]
            higher = edges[idx+1]
            if value >= lower and value <= higher:
                hist_hit_bin[idx] += n_hit
                hist_miss_bin[idx] += n_miss
                break

    hist_hit_bin = [value / tot_hits for value in hist_hit_bin]
    hist_miss_bin = [value / tot_miss for value in hist_miss_bin]

    return hist_hit_bin, hist_miss_bin


def check_filters(string: str, filters: list):
    return not all([string.find(filter_) != -1 for filter_ in filters])


def get_size_from_name(name: str) -> str:
    string = name.split("_")
    for part in string:
        if part.find("T") != -1:
            return part
    return 'unknown'


def plot_info_window(window: int, filename: str, **kwargs):
    # Empty plot with log scale:
    # - https://github.com/bokeh/bokeh/issues/6671
    global TABLES, WINDOW_INFO, TABLE_COLORS
    data = {}
    filters = kwargs.get('filters', [])

    for cache_name, info in WINDOW_INFO.items():
        if filters and check_filters(cache_name, filters):
            continue

        size = get_size_from_name(cache_name)
        if size not in data:
            data[size] = {}

        if cache_name.lower().find('lru') != -1:
            data[size]['lru'] = {
                'cache': info[window]['cache'],
                'stats': info[window]['stats']
            }
        else:
            data[size][cache_name] = {
                'weights': info[window]['weights'],
                'cache': info[window]['cache'],
                'stats': info[window]['stats']
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
            filenames_sort_by_weights = [key for key, _ in sorted(
                cur_data['weights'].items(),
                key=lambda weight_elm: weight_elm[1],
                reverse=True)
            ]

            filenames_sort_by_size = [key for key, _ in sorted(
                cur_data['stats'].items(),
                key=lambda stat_elm: stat_elm[1]['size'],
                reverse=True)
            ]

            ##
            # Files in weighted cache
            pf_fileW_hit_weighted_cache = figure(
                title=f"File in {cache_name} (sorted by weight function score, gray area)",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 1280),
                plot_height=kwargs.get('plot_height', 200),
                x_range=filenames_sort_by_weights,
                y_range=(1, int(max(cur_data['weights'].values())) + 10),
                x_axis_type=None,
                y_axis_type=kwargs.get('y_axis_type', 'auto'),
            )

            pf_fileW_hit_weighted_cache.vbar(
                filenames_sort_by_weights,
                top=[
                    cur_data['weights'][filename]
                    for filename in filenames_sort_by_weights
                ],
                color="gainsboro",
                width=1.0,
                bottom=0.01 if kwargs.get(
                    'y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
            )

            pf_fileW_hit_weighted_cache.vbar(
                filenames_sort_by_weights,
                top=[
                    cur_data['weights'][filename]
                    if filename in cur_data['cache']
                    else 0
                    for filename in filenames_sort_by_weights
                ],
                color="blue",
                width=1.0,
                bottom=0.01 if kwargs.get(
                    'y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
            )

            ##
            # Files in LRU cache
            pf_fileW_hit_lru_cache = figure(
                title=f"File in LRU cache (sorted by weight function score, gray area)",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 1280),
                plot_height=kwargs.get('plot_height', 200),
                x_range=filenames_sort_by_weights,
                y_range=(1, int(max(cur_data['weights'].values())) + 10),
                x_axis_type=None,
                y_axis_type=kwargs.get('y_axis_type', 'auto'),
            )

            pf_fileW_hit_lru_cache.vbar(
                filenames_sort_by_weights,
                top=[
                    cur_data['weights'][filename]
                    for filename in filenames_sort_by_weights
                ],
                color="gainsboro",
                width=1.0,
                bottom=0.01 if kwargs.get(
                    'y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
            )

            pf_fileW_hit_lru_cache.vbar(
                filenames_sort_by_weights,
                top=[
                    cur_data['weights'][filename]
                    if filename in caches['lru']['cache']
                    else 0
                    for filename in filenames_sort_by_weights
                ],
                color="red",
                width=1.0,
                bottom=0.01 if kwargs.get(
                    'y_axis_type', False) == 'log' else 0.0  # To avoid empty plot
            )

            _, size_edges = np.histogram(
                [stat['size'] for stat in cur_data['stats'].values()],
                bins=MAX_BINS
            )
            hist_hit_wc, hist_miss_wc = fill_hit_miss_bins(MAX_BINS, size_edges, [
                (
                    cur_data['stats'][filename]['size'],
                    cur_data['stats'][filename]['nHits'],
                    cur_data['stats'][filename]['nMiss']
                )
                for filename in filenames_sort_by_size
            ])

            hist_hit_lru, hist_miss_lru = fill_hit_miss_bins(MAX_BINS, size_edges, [
                (
                    caches['lru']['stats'][filename]['size'],
                    caches['lru']['stats'][filename]['nHits'],
                    caches['lru']['stats'][filename]['nMiss']
                )
                for filename in filenames_sort_by_size
            ])

            ##
            # Number hits x file in weighted cache
            pf_fileSize_hit_weighted_cache = figure(
                title=f"Number of hits x file in {cache_name} (sorted by file size value, gray area)",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 640),
                plot_height=kwargs.get('plot_height', 200),
                x_axis_label="File size",
                y_axis_label="Sum n. hit",
                y_range=(0., 1.),
            )

            pf_fileSize_hit_weighted_cache.quad(
                bottom=0.001,
                top=hist_hit_wc,
                left=size_edges[:-1],
                right=size_edges[1:],
                color="blue",
                line_color="white"
            )

            ##
            # Number miss x file in weighted cache
            pf_fileSize_miss_weighted_cache = figure(
                title=f"Number of miss x file in {cache_name} (sorted by file size value, gray area)",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 640),
                plot_height=kwargs.get('plot_height', 200),
                x_axis_label="File size",
                y_axis_label="Sum n. miss",
                y_range=(0., 1.),
            )

            pf_fileSize_miss_weighted_cache.quad(
                bottom=0.001,
                top=hist_miss_wc,
                left=size_edges[:-1],
                right=size_edges[1:],
                color="blue",
                line_color="white"
            )

            ##
            # Number hits x file in LRU cache
            pf_fileSize_hit_LRU_cache = figure(
                title=f"Number of hits x file in {cache_name} (sorted by file size value, gray area)",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 640),
                plot_height=kwargs.get('plot_height', 200),
                x_axis_label="File size",
                y_axis_label="Sum n. hit",
                y_range=(0., 1.),

            )
            pf_fileSize_hit_LRU_cache.quad(
                bottom=0.001,
                top=hist_hit_lru,
                left=size_edges[:-1],
                right=size_edges[1:],
                color="red",
                line_color="white"
            )

            ##
            # Number miss x file in LRU cache
            pf_fileSize_miss_LRU_cache = figure(
                title=f"Number of miss x file in {cache_name} (sorted by file size value, gray area)",
                tools="box_zoom,pan,reset,save",
                plot_width=kwargs.get('plot_width', 640),
                plot_height=kwargs.get('plot_height', 200),
                x_axis_label="File size",
                y_axis_label="Sum n. miss",
                y_range=(0., 1.),

            )
            pf_fileSize_miss_LRU_cache.quad(
                bottom=0.001,
                top=hist_miss_lru,
                left=size_edges[:-1],
                right=size_edges[1:],
                color="red",
                line_color="white"
            )

            figures.append(
                column(
                    pf_fileW_hit_weighted_cache,
                    pf_fileW_hit_lru_cache,
                    row(
                        column(
                            pf_fileSize_hit_weighted_cache,
                            pf_fileSize_hit_LRU_cache
                        ),
                        column(
                            pf_fileSize_miss_weighted_cache,
                            pf_fileSize_miss_LRU_cache
                        )
                    )
                )
            )

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

    if table_name != 'ratio' and table_name != 'diff':
        for cache_name, values in sorted(
            TABLES[table_name].items(), key=lambda elm: elm[0]
        ):
            if filters and check_filters(cache_name, filters):
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
            if cache_name not in TABLE_COLORS:
                update_colors(cache_name)
            plot_figure.line(
                range(len(points)),
                points,
                legend=cache_name,
                color=TABLE_COLORS[cache_name],
                line_width=2.
            )
    elif table_name == 'ratio' or table_name == 'diff':
        data = {}
        for cur_table_name in ['written_data', 'read_on_hit']:
            for cache_name, values in sorted(
                TABLES[cur_table_name].items(), key=lambda elm: elm[0]
            ):
                if cache_name not in data:
                    data[cache_name] = {
                        'written_data': [],
                        'read_on_hit': []
                    }
                if filters and check_filters(cache_name, filters):
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
                data[cache_name][cur_table_name] = [
                    value for bucket in values for value in bucket]
                if cache_name not in TABLE_COLORS:
                    update_colors(cache_name)

        for cache_name, values in data.items():
            if cache_name not in TABLE_COLORS:
                update_colors(cache_name)
            if table_name == 'ratio':
                cur_values = [
                    value / values['written_data'][idx]
                    if values['written_data'][idx] != 0.
                    else 0.
                    for idx, value in enumerate(values['read_on_hit'])
                ]
            elif table_name == 'diff':
                cur_values = [
                    value - values['written_data'][idx]
                    if values['written_data'][idx] != 0.
                    else 0.
                    for idx, value in enumerate(values['read_on_hit'])
                ]
            plot_figure.line(
                range(len(values['read_on_hit'])),
                cur_values,
                legend=cache_name,
                color=TABLE_COLORS[cache_name],
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
    elif table_name == "weighted_hit_rate":
        kwargs['y_axis_label'] = "Weighted hit rate"
    elif table_name == "size":
        kwargs['y_axis_label'] = "Size (MB)"
        kwargs['y_axis_type'] = "log"
    elif table_name == "written_data":
        kwargs['y_axis_label'] = "Written data (MB)"
        kwargs['y_axis_type'] = "log"
    elif table_name == "read_on_hit":
        kwargs['y_axis_label'] = "Data read on hit (MB)"
        kwargs['y_axis_type'] = "log"
    elif table_name == "ratio":
        kwargs['y_axis_label'] = "Ratio"
    elif table_name == "diff":
        kwargs['y_axis_label'] = "Diff"

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
    # obj example:
    # {
    #   cache: {filename -> size}
    #   weights: {filename -> weight}
    #   stats: {filename -> {
    #       size -> value,
    #       totReq -> value,
    #       nHits -> value,
    #       nMiss -> value,
    #   }}
    # }
    obj = json.loads(gzip.decompress(data))

    if cache_name not in WINDOW_INFO:
        WINDOW_INFO[cache_name] = []

    try:
        WINDOW_INFO[cache_name][window] = obj
    except IndexError:
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
