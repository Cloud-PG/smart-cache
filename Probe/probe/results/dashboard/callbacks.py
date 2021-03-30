import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc

# import dash_daq as daq
import dash_html_components as html

# Create random data with numpy
import pandas as pd
import plotly.express as px

from ..data import (
    COLUMNS,
    make_table,
    measure_score,
    measure_avg_free_space,
    measure_bandwidth,
    measure_cost,
    measure_cost_ratio,
    measure_cpu_eff,
    measure_hit_over_miss,
    measure_hit_rate,
    measure_num_miss_after_delete,
    measure_read_on_hit_ratio,
    measure_redirect_volume,
    measure_std_dev_free_space,
    measure_throughput,
    measure_throughput_ratio,
    parse_simulation_report,
)
from .utils import (
    DashCacheManager,
    get_files2plot,
    get_prefix,
    make_agent_figures,
    make_line_figures,
    parse_simulation_report_stuff,
)
from .vars import PLOT_LAYOUT

# import plotly.express as px
# import plotly.graph_objects as go


_EMPTY_TUPLE = ("", "", "", "", "", "")

_MEASURES = {
    "Score": measure_score,
    "Throughput ratio": measure_throughput_ratio,
    "Cost ratio": measure_cost_ratio,
    "Throughput (TB)": measure_throughput,
    "Cost (TB)": measure_cost,
    "Read on hit ratio": measure_read_on_hit_ratio,
    "CPU Eff.": measure_cpu_eff,
    "Avg. Free Space": measure_avg_free_space,
    "Std. Dev. Free Space": measure_std_dev_free_space,
    "Bandwidth": measure_bandwidth,
    "Redirect Vol.": measure_redirect_volume,
    "Num. miss after del.": measure_num_miss_after_delete,
    "Hit rate": measure_hit_rate,
}


def binning_size(value: int) -> str:
    return f"binning size = {value}"


def _tab_columns(
    cache_manager: "DashCacheManager",
    hash_args: tuple,
    files,
    filters_all,
    filters_any,
    num_of_results,
    columns_binning_size,
    columns,
) -> tuple:
    if cache_manager.check("columns", hash_args=hash_args):
        data = cache_manager.get("columns", hash_args=hash_args)
        return (data, "", "", "", "", "")
    else:
        figures = []
        results = cache_manager.get("results", hash_="data")
        for column in columns:
            files2plot = get_files2plot(
                results,
                files,
                filters_all,
                filters_any,
                column,
            )
            prefix = get_prefix(files2plot)

            if num_of_results != 0 and num_of_results is not None:
                _, new_file2plot = make_table(files2plot, prefix, num_of_results)
                files2plot = [
                    (file_, df) for file_, df in files2plot if file_ in new_file2plot
                ]
                prefix = get_prefix(files2plot)

            figures.append(
                dcc.Graph(
                    figure=make_line_figures(
                        files2plot,
                        prefix,
                        title=column,
                        column=column,
                        binning_size=columns_binning_size,
                    )
                )
            )
            figures.append(html.Hr())

        cache_manager.set("columns", hash_args, data=figures)
        return (figures, "", "", "", "", "")


def _tab_measures(
    cache_manager: "DashCacheManager",
    hash_args: tuple,
    files,
    filters_all,
    filters_any,
    num_of_results,
    measures_binning_size,
    measures,
) -> tuple:
    if cache_manager.check("measures", hash_args=hash_args):
        data = cache_manager.get("measures", hash_args=hash_args)
        return ("", data, "", "", "", "")
    else:
        results = cache_manager.get("results", hash_="data")
        figures = []
        files2plot = get_files2plot(
            results,
            files,
            filters_all,
            filters_any,
        )
        prefix = get_prefix(files2plot)

        if num_of_results != 0 and num_of_results is not None:
            table, new_file2plot = make_table(files2plot, prefix, num_of_results)
            files2plot = [
                (file_, df) for file_, df in files2plot if file_ in new_file2plot
            ]
            prefix = get_prefix(files2plot)

        for measure in sorted(measures):
            function = _MEASURES[measure]
            figures.append(
                dcc.Graph(
                    figure=make_line_figures(
                        files2plot,
                        prefix,
                        title=measure,
                        function=function,
                        binning_size=measures_binning_size,
                    )
                )
            )
            figures.append(html.Hr())

        cache_manager.set("measures", hash_args, data=figures)
        return ("", figures, "", "", "", "")


def _tab_agents(
    cache_manager: "DashCacheManager",
    hash_args: tuple,
    files,
    filters_all,
    filters_any,
    num_of_results,
) -> tuple:
    if cache_manager.check("agents", hash_args=hash_args):
        data = cache_manager.get("agents", hash_args=hash_args)
        return ("", "", data, "", "", "")
    else:
        figures = []
        results = cache_manager.get("results", hash_="data")
        files2plot = get_files2plot(
            results, files, filters_all, filters_any, agents=True
        )
        prefix = get_prefix(files2plot)

        if num_of_results != 0 and num_of_results is not None:
            table, new_file2plot = make_table(files2plot, prefix, num_of_results)
            files2plot = [
                (file_, df) for file_, df in files2plot if file_ in new_file2plot
            ]
            prefix = get_prefix(files2plot)

        figures.extend(
            make_agent_figures(
                files2plot,
                prefix,
            )
        )

        cache_manager.set("agents", hash_args, data=figures)
        return ("", "", figures, "", "", "")


def _tab_table(
    cache_manager: "DashCacheManager",
    hash_args: tuple,
    extended,
    sorting_by,
    normalization_file,
    new_metrics,
    files,
    filters_all,
    filters_any,
    num_of_results,
) -> tuple:

    results = cache_manager.get("results", hash_="data")
    files2plot = get_files2plot(
        results,
        files,
        filters_all,
        filters_any,
    )
    prefix = get_prefix(files2plot)

    normalization_res = None

    if normalization_file != "Nil":
        if cache_manager.check("norm_results", hash_="data"):
            norm_data = cache_manager.get("norm_results", hash_="data")
            normalization_res = norm_data.get_df(normalization_file)

    # print(normalization_file, normalization_res)

    if num_of_results != 0 and num_of_results is not None:
        table, new_file2plot = make_table(
            files2plot,
            prefix,
            num_of_results,
            extended=extended,
            sorting_by=sorting_by,
            new_metrics=new_metrics,
            normalization_res=normalization_res,
        )
        files2plot = [(file_, df) for file_, df in files2plot if file_ in new_file2plot]
        prefix = get_prefix(files2plot)
        table, _ = make_table(
            files2plot,
            prefix,
            extended=extended,
            sorting_by=sorting_by,
            new_metrics=new_metrics,
            normalization_res=normalization_res,
        )
    else:
        table, _ = make_table(
            files2plot,
            prefix,
            extended=extended,
            sorting_by=sorting_by,
            new_metrics=new_metrics,
            normalization_res=normalization_res,
        )

    cache_manager.set(
        "results",
        hash_="table_obj",
        data=table,
    )
    cache_manager.set(
        "results", hash_="table.csv", data=table.to_csv(index=False).encode("utf-8")
    )
    cache_manager.set(
        "results",
        hash_="table.tex",
        data=table.to_latex(index=False).encode("utf-8"),
    )
    cache_manager.set(
        "results",
        hash_="table.html",
        data=table.to_html(index=False).encode("utf-8"),
    )

    table = dbc.Table.from_dataframe(table, striped=True, bordered=True, hover=True)

    cache_manager.set("tables", hash_args, data=table)

    return ("", "", "", table, "", "")


def _tab_compare(
    cache_manager: "DashCacheManager",
    hash_args: tuple,
    files,
    filters_all,
    filters_any,
) -> tuple:
    if cache_manager.check("compare", hash_args=hash_args):
        _, figs, tables = cache_manager.get("compare", hash_args=hash_args)
        return ("", "", "", "", figs, tables)
    else:
        results = cache_manager.get("results", hash_="data")
        files2plot = get_files2plot(
            results,
            files,
            filters_all,
            filters_any,
            with_log=True,
        )
        prefix = get_prefix(files2plot)
        data = parse_simulation_report(files2plot, prefix)
        figs, tables = parse_simulation_report_stuff(data, len(results))

        cache_manager.set("tables", hash_args, data=(data, figs, tables))

        return ("", "", "", "", figs, tables)


def switch_tab(
    at,
    extended,
    sorting_by,
    normalization_file,
    new_metrics,
    columns_binning_size,
    measures_binning_size,
    columns,
    measures,
    files,
    filters_all,
    filters_any,
    num_of_results,
):
    hash_args = (
        files,
        filters_all,
        filters_any,
        num_of_results,
        extended,
        sorting_by,
        normalization_file,
        new_metrics,
        columns_binning_size,
        measures_binning_size,
        columns,
        measures,
    )

    if at == "tab-files":
        return _EMPTY_TUPLE
    elif at == "tab-filters":
        return _EMPTY_TUPLE
    elif at == "tab-columns":
        cache_manager = DashCacheManager()
        data = cache_manager.get("results", hash_="data")
        files = [filename for filename in data.files]
        return _tab_columns(
            cache_manager,
            hash_args,
            files,
            filters_all,
            filters_any,
            num_of_results,
            columns_binning_size,
            columns,
        )
    elif at == "tab-measures":
        cache_manager = DashCacheManager()
        data = cache_manager.get("results", hash_="data")
        files = [filename for filename in data.files]
        return _tab_measures(
            cache_manager,
            hash_args,
            files,
            filters_all,
            filters_any,
            num_of_results,
            measures_binning_size,
            measures,
        )
    elif at == "tab-agents":
        cache_manager = DashCacheManager()
        data = cache_manager.get("results", hash_="data")
        files = [filename for filename in data.files]
        return _tab_agents(
            cache_manager, hash_args, files, filters_all, filters_any, num_of_results
        )

    elif at == "tab-table":
        cache_manager = DashCacheManager()
        data = cache_manager.get("results", hash_="data")
        files = [filename for filename in data.files]
        return _tab_table(
            cache_manager,
            hash_args,
            extended,
            sorting_by,
            normalization_file,
            new_metrics,
            files,
            filters_all,
            filters_any,
            num_of_results,
        )

    elif at == "tab-compare":
        cache_manager = DashCacheManager()
        data = cache_manager.get("results", hash_="data")
        files = [filename for filename in data.files]
        return _tab_compare(
            cache_manager,
            hash_args,
            files,
            filters_all,
            filters_any,
        )
    else:
        return _EMPTY_TUPLE


def show_value(msg: str = ""):
    def inn_fun(value: "Any"):
        return f"{msg}: {value}"

    return inn_fun


def unselect_all_files(unselect_n_clicks, select_n_clicks):
    cache_manager = DashCacheManager()
    results = cache_manager.get("results", hash_="data")
    # Ref: https://dash.plotly.com/advanced-callbacks
    changed_id = [p["prop_id"].split(".")[0] for p in dash.callback_context.triggered][
        0
    ]
    if changed_id == "unselect-files":
        return []
    elif changed_id == "select-files":
        return results.files
    return results.files


def compare_results(num_sim, tick, files, filters_all, filters_any, num_of_results):
    cache_manager = DashCacheManager()
    hash_args = [
        files,
        filters_all,
        filters_any,
        num_of_results,
    ]
    if cache_manager.check("compare", hash_args=hash_args):
        data, *_ = cache_manager.get("compare", hash_args=hash_args)
        keys = list(data.keys())
        try:
            cur_sim = keys[num_sim]
            for evaluator in data[cur_sim]:
                if evaluator.tick == tick:
                    scatterActionsFig = px.scatter_3d(
                        evaluator.actions,
                        x="num req",
                        y="size",
                        z="filename",
                        color="delta t",
                        size="size",
                        opacity=0.9,
                    )
                    scatterActionsFig.update_layout(PLOT_LAYOUT)
                    histActionNumReq = px.histogram(evaluator.actions, x="num req")
                    histActionNumReq.update_layout(PLOT_LAYOUT)
                    histActionSize = px.histogram(evaluator.actions, x="size")
                    histActionSize.update_layout(PLOT_LAYOUT)
                    histActionDeltaT = px.histogram(evaluator.actions, x="delta t")
                    histActionDeltaT.update_layout(PLOT_LAYOUT)
                    after_data = evaluator.after4scatter
                    scatterAfterFig = px.scatter_3d(
                        after_data,
                        x="num req",
                        y="size",
                        z="filename",
                        color="delta t",
                        size="size",
                        opacity=0.9,
                    )
                    scatterAfterFig.update_layout(PLOT_LAYOUT)
                    return (
                        [dcc.Graph(figure=scatterActionsFig)],
                        [dcc.Graph(figure=histActionNumReq)],
                        [dcc.Graph(figure=histActionSize)],
                        [dcc.Graph(figure=histActionDeltaT)],
                        [dcc.Graph(figure=scatterAfterFig)],
                    )
            else:
                return (
                    [
                        dbc.Alert(
                            f"No tick found in simulation {num_sim}", color="danger"
                        )
                    ],
                    [""],
                    [""],
                    [""],
                    [""],
                )
        except (IndexError, TypeError):
            return (
                [dbc.Alert(f"No simulation found at index {num_sim}", color="danger")],
                [""],
                [""],
                [""],
                [""],
            )
    else:
        return [dbc.Alert("No results", color="warning")], [""], [""], [""], [""]


def toggle_collapse_table(*args):
    ctx = dash.callback_context

    cache_manager = DashCacheManager()
    results = cache_manager.get("results", hash_="data")

    if not ctx.triggered:
        return [False] * len(results)
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    button_idx = int(button_id.split("-")[1])  # "group-idx-toggle"

    res = [False] * len(results)
    for idx in range(len(res)):
        # update all is open to current status
        res[idx] = args[idx + len(results)]

    if args[button_idx]:  # Check input n
        res[button_idx] = not args[button_idx + len(results)]  # is open

    return res
