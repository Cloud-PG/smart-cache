import os

from bokeh.io import export_png
from bokeh.layouts import column, row
from bokeh.models import BoxZoomTool, LassoSelectTool, PanTool, ResetTool
from bokeh.plotting import output_file, save
from tqdm import tqdm

from .plotter import filter_results, plot_column, plot_measure, update_colors
from .utils import ignored


def plot_results(folder: str, results: dict, cache_size: float,
                 filters: list = [], window_size: int = 1,
                 html: bool = True, png: bool = False,
                 plot_width: int = 640,
                 plot_height: int = 480,
                 bandwidth: int = 10,
                 outer_legend: bool = False,
                 num_points: int = 52,
                 ):
    color_table = {}
    dates = []
    datetimes = []

    if html:
        output_file(
            os.path.join(
                folder,
                "results.html"
            ),
            "Results",
            mode="inline"
        )

    # Tools
    tools = [
        BoxZoomTool(dimensions='width'),
        BoxZoomTool(dimensions='height'),
        LassoSelectTool(),
        PanTool(dimensions='width'),
        PanTool(dimensions='height'),
        ResetTool(),
    ]

    # Update colors
    for cache_name, _ in filter_results(
        results, 'run_full_normal', filters
    ):
        update_colors(cache_name, color_table)

    # Get dates
    for cache_name, values in filter_results(
        results, 'run_full_normal', filters
    ):
        if not dates:
            dates = [
                elm.split(" ")[0]
                for elm
                in values['date'].astype(str)
            ]
            datetimes = values['date']
            break

    figs = []
    run_full_normal_hit_rate_figs = []
    run_full_normal_size_figs = []
    run_full_normal_throughput_figs = []
    run_full_normal_cost_figs = []
    run_full_normal_net_figs = []
    run_full_normal_epsilon_figs = []
    run_full_normal_value_functions_figs = []
    run_full_normal_eviction_stat_figs = []
    run_full_normal_agent_action_figs = []
    run_full_normal_data_rw_figs = []
    run_full_normal_data_read_stats_figs = []
    run_full_normal_cpu_eff_figs = []
    run_single_window_figs = []
    run_next_period_figs = []

    pbar = tqdm(total=35, desc="Plot results", ascii=True)

    ###########################################################################
    # Size plot of full normal run
    ###########################################################################
    with ignored(Exception):
        size_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            column="size",
            title="Size - Full Normal Run",
            plot_width=plot_width,
            plot_height=plot_height,
            y_axis_label="MB",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_hit_rate_figs.append(size_fig)
    pbar.update(1)

    ###########################################################################
    # Hit Rate plot of full normal run
    ###########################################################################
    with ignored(Exception):
        hit_rate_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="hit rate",
            title="Hit Rate - Full Normal Run",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_hit_rate_figs.append(hit_rate_fig)
    pbar.update(1)

    ###########################################################################
    # Occupancy percentage
    ###########################################################################
    with ignored(Exception):
        occupancy_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Occupancy percentage",
            plot_width=plot_width,
            plot_height=plot_height,
            target="sizePerc",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_size_figs.append(occupancy_fig)
    pbar.update(1)

    ###########################################################################
    # Avg Free space percentage
    ###########################################################################
    with ignored(Exception):
        avg_free_space_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Avg. Free space percentage",
            plot_width=plot_width,
            plot_height=plot_height,
            target="avgFreeSpacePerc",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_size_figs.append(avg_free_space_fig)
    pbar.update(1)

    ###########################################################################
    # Std Dev Free space
    ###########################################################################
    with ignored(Exception):
        std_dev_free_space_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Std. Dev. Free space",
            plot_width=plot_width,
            plot_height=plot_height,
            target="stdDevFreeSpace",
            y_axis_label="",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_size_figs.append(std_dev_free_space_fig)
    pbar.update(1)

    ###########################################################################
    # Global Cost plot of full normal run
    ###########################################################################
    with ignored(Exception):
        global_cost_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Cost with read on miss",
            plot_width=plot_width,
            plot_height=plot_height,
            target="costFunction",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_cost_figs.append(global_cost_fig)
    pbar.update(1)

    ###########################################################################
    # Cache Cost plot of full normal run
    ###########################################################################
    with ignored(Exception):
        cache_cost_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Cost",
            plot_width=plot_width,
            plot_height=plot_height,
            target="cacheCost",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_cost_figs.append(cache_cost_fig)
    pbar.update(1)

    ###########################################################################
    # Global Cost vs lru plot of full normal run
    ###########################################################################
    with ignored(Exception):
        global_cost_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Cost vs LRU",
            plot_width=plot_width,
            plot_height=plot_height,
            target="costFunctionVs",
            y_axis_label="ratio",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_cost_figs.append(global_cost_fig)
    pbar.update(1)

    ###########################################################################
    # Cache Cost vs lru plot of full normal run
    ###########################################################################
    with ignored(Exception):
        cache_cost_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Cache write and delete cost vs LRU",
            plot_width=plot_width,
            plot_height=plot_height,
            target="cacheCostVs",
            y_axis_label="ratio",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_cost_figs.append(cache_cost_fig)
    pbar.update(1)

    ###########################################################################
    # Throughput plot of full normal run
    ###########################################################################
    with ignored(Exception):
        throughtput_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Throughput",
            plot_width=plot_width,
            plot_height=plot_height,
            target="throughput",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_throughput_figs.append(throughtput_fig)
    pbar.update(1)

    ###########################################################################
    # Throughput vs LRU plot of full normal run
    ###########################################################################
    with ignored(Exception):
        throughtput_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Throughput vs LRU",
            plot_width=plot_width,
            plot_height=plot_height,
            target="throughputVs",
            y_axis_label="ratio",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_throughput_figs.append(throughtput_fig)
    pbar.update(1)

    ###########################################################################
    # Miss plot of full normal run
    ###########################################################################
    with ignored(Exception):
        miss_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Miss",
            plot_width=plot_width,
            plot_height=plot_height,
            target="miss",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_throughput_figs.append(miss_fig)
    pbar.update(1)

    ###########################################################################
    # Day network input saturation full normal run
    ###########################################################################
    with ignored(Exception):
        net_in = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_label="%",
            title=f"Day network input saturation - {bandwidth}Gbit/s",
            plot_width=plot_width,
            plot_height=plot_height,
            target="network_in_saturation",
            bandwidth=bandwidth,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_net_figs.append(net_in)
    pbar.update(1)

    ###########################################################################
    # Day network output saturation full normal run
    ###########################################################################
    with ignored(Exception):
        net_in = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_label="%",
            title=f"Day network output saturation - {bandwidth}Gbit/s",
            plot_width=plot_width,
            plot_height=plot_height,
            target="network_out_saturation",
            bandwidth=bandwidth,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_net_figs.append(net_in)
    pbar.update(1)

    ###########################################################################
    # Addition Agent Epsilon
    ###########################################################################
    with ignored(Exception):
        add_epsilon_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            column="Addition epsilon",
            title="Addition Agent Epsilon",
            plot_width=plot_width,
            plot_height=plot_height,
            y_axis_label="ε",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_epsilon_figs.append(add_epsilon_fig)
    pbar.update(1)

    ###########################################################################
    # Eviction Agent Epsilon
    ###########################################################################
    with ignored(Exception):
        evc_epsilon_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            column="Eviction epsilon",
            title="Eviction Agent Epsilon",
            plot_width=plot_width,
            plot_height=plot_height,
            y_axis_label="ε",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_epsilon_figs.append(evc_epsilon_fig)
    pbar.update(1)

    ###########################################################################
    # Addition value function
    ###########################################################################
    with ignored(Exception):
        net_in = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_label="qvalue",
            title=f"Addition qvalue function",
            column="Addition qvalue function",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_value_functions_figs.append(net_in)
    pbar.update(1)

    ###########################################################################
    # Eviction value function
    ###########################################################################
    with ignored(Exception):
        net_in = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_label="qvalue",
            title=f"Eviction qvalue function",
            column="Eviction qvalue function",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_value_functions_figs.append(net_in)
    pbar.update(1)

    ###########################################################################
    # Eviction num. calls
    ###########################################################################
    with ignored(Exception):
        evc_calls_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_label="#",
            title=f"Eviction # calls",
            column="Eviction calls",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_eviction_stat_figs.append(evc_calls_fig)
    pbar.update(1)

    ###########################################################################
    # Eviction num. forced calls
    ###########################################################################
    with ignored(Exception):
        evc_fcalls_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_label="#",
            title=f"Eviction # forced calls",
            column="Eviction forced calls",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_eviction_stat_figs.append(evc_fcalls_fig)
    pbar.update(1)

    ###########################################################################
    # Eviction step
    ###########################################################################
    with ignored(Exception):
        evc_step_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_label="k",
            title=f"Eviction step (k)",
            column="Eviction step",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_eviction_stat_figs.append(evc_step_fig)
    pbar.update(1)

    ###########################################################################
    # Addition actions
    ###########################################################################
    with ignored(Exception):
        addition_action_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Addition actions",
            plot_width=plot_width,
            plot_height=plot_height,
            target="additionActions",
            y_axis_label="#",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_agent_action_figs.append(addition_action_fig)
    pbar.update(1)

    ###########################################################################
    # eviction actions
    ###########################################################################
    with ignored(Exception):
        eviction_action_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            y_axis_type="log",
            title="Eviction actions",
            plot_width=plot_width,
            plot_height=plot_height,
            target="evictionActions",
            y_axis_label="#",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_agent_action_figs.append(eviction_action_fig)
    pbar.update(1)

    ###########################################################################
    # Written data ratio plot of full normal run
    ###########################################################################
    with ignored(Exception):
        written_data_ratio_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Written data ratio",
            plot_width=plot_width,
            plot_height=plot_height,
            target="writtenRatio",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_rw_figs.append(written_data_ratio_fig)
    pbar.update(1)

    ###########################################################################
    # Deleted data ratio plot of full normal run
    ###########################################################################
    with ignored(Exception):
        deleted_data_ratio_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Deleted data ratio",
            plot_width=plot_width,
            plot_height=plot_height,
            target="deletedRatio",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_rw_figs.append(deleted_data_ratio_fig)
    pbar.update(1)

    ###########################################################################
    # Written data plot of full normal run
    ###########################################################################
    with ignored(Exception):
        written_data_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="written data",
            # normalize="read data",
            # title="Written data / Read data - Full Normal Run",
            title="Written data - Full Normal Run",
            # y_axis_label="Written data %",
            y_axis_label="Written data",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_rw_figs.append(written_data_fig)
    pbar.update(1)

    ###########################################################################
    # Deleted data plot of full normal run
    ###########################################################################
    with ignored(Exception):
        deleted_data_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="deleted data",
            # normalize="read data",
            # title="Deleted data / Read data - Full Normal Run",
            title="Deleted data - Full Normal Run",
            # y_axis_label="Deleted data %",
            y_axis_label="Deleted data",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_rw_figs.append(deleted_data_fig)
    pbar.update(1)

    ###########################################################################
    # Read data plot of full normal run
    ###########################################################################
    with ignored(Exception):
        read_data_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="read data",
            title="Read data - Full Normal Run",
            y_axis_label="Read data (MB)",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_rw_figs.append(read_data_fig)
    pbar.update(1)

    ###########################################################################
    # Read on hit ratio plot of full normal run
    ###########################################################################
    with ignored(Exception):
        read_on_hit_ratio_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Read on hit ratio",
            plot_width=plot_width,
            plot_height=plot_height,
            target="readOnHitRatio",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_read_stats_figs.append(read_on_hit_ratio_fig)
    pbar.update(1)

    ###########################################################################
    # Read on miss ratio plot of full normal run
    ###########################################################################
    with ignored(Exception):
        read_on_miss_ratio_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Read on miss ratio",
            plot_width=plot_width,
            plot_height=plot_height,
            target="readOnMissRatio",
            y_axis_label="%",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_read_stats_figs.append(read_on_miss_ratio_fig)
    pbar.update(1)

    ###########################################################################
    # Read on hit data plot of full normal run
    ###########################################################################
    with ignored(Exception):
        read_data_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="read on hit data",
            # normalize="read data",
            # title="Read on hit data / Read data - Full Normal Run",
            title="Read on hit data - Full Normal Run",
            # y_axis_label="%",
            y_axis_label="read (MB)",
            plot_width=plot_width,
            plot_height=plot_height,
            upper_bound="read data",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_read_stats_figs.append(read_data_fig)
    pbar.update(1)

    ###########################################################################
    # Read on miss data plot of full normal run
    ###########################################################################
    with ignored(Exception):
        read_data_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="read on miss data",
            # normalize="read data",
            # title="Read on miss data / Read data - Full Normal Run",
            title="Read on miss data - Full Normal Run",
            # y_axis_label="%",
            y_axis_label="miss (MB)",
            plot_width=plot_width,
            plot_height=plot_height,
            upper_bound="read data",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_data_read_stats_figs.append(read_data_fig)
    pbar.update(1)

    ###########################################################################
    # CPU eff full normal run
    ###########################################################################
    with ignored(Exception):
        cpu_eff = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="CPU efficiency",
            title="CPU efficiency",
            y_axis_label="%",
            plot_width=plot_width,
            plot_height=plot_height,
            upper_bound="CPU efficiency upper bound",
            lower_bound="CPU efficiency lower bound",
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_cpu_eff_figs.append(cpu_eff)
    pbar.update(1)

    ###########################################################################
    # Read on hit CPU eff full normal run
    ###########################################################################
    with ignored(Exception):
        read_on_hit_eff = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="CPU hit efficiency",
            title="CPU efficiency on hit",
            y_axis_label="%",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_cpu_eff_figs.append(read_on_hit_eff)
    pbar.update(1)

    ###########################################################################
    # Read on miss CPU eff full normal run
    ###########################################################################
    with ignored(Exception):
        read_on_miss_eff = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="CPU miss efficiency",
            title="CPU efficiency on miss",
            y_axis_label="%",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_full_normal_cpu_eff_figs.append(read_on_miss_eff)
    pbar.update(1)

    ###########################################################################
    # Hit Rate compare single and next window plot
    ###########################################################################
    with ignored(Exception):
        hit_rate_comp_snw_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="hit rate",
            title="Hit Rate - Compare single and next window",
            run_type="run_single_window",
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_single_window_figs.append(hit_rate_comp_snw_fig)
    pbar.update(1)

    ###########################################################################
    # Read on Write data data compare single and next window plot
    ###########################################################################
    with ignored(Exception):
        ronwdata_comp_snw_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Read data / Written data Compare single and next window",
            run_type="run_single_window",
            plot_width=plot_width,
            plot_height=plot_height,
            read_on_hit=False,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_single_window_figs.append(ronwdata_comp_snw_fig)
    pbar.update(1)

    ###########################################################################
    # Read on Hit on Write data data compare single and next window plot
    ###########################################################################
    with ignored(Exception):
        rhonwdata_comp_snw_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Read on Hit data / Written data Compare single and next window",
            run_type="run_single_window",
            plot_width=plot_width,
            plot_height=plot_height,
            read_on_hit=True,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_single_window_figs.append(rhonwdata_comp_snw_fig)
    pbar.update(1)

    ###########################################################################
    # Hit Rate compare single window and next period plot
    ###########################################################################
    with ignored(Exception):
        hit_rate_comp_swnp_fig = plot_column(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            column="hit rate",
            title="Hit Rate - Compare single window and next period",
            run_type="run_next_period",
            datetimes=datetimes,
            plot_width=plot_width,
            plot_height=plot_height,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_next_period_figs.append(hit_rate_comp_swnp_fig)
    pbar.update(1)

    ###########################################################################
    # Read on Write data data compare single window and next period plot
    ###########################################################################
    with ignored(Exception):
        ronwdata_comp_swnp_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Read data / Written data - Compare single window and next period",
            run_type="run_next_period",
            datetimes=datetimes,
            plot_width=plot_width,
            plot_height=plot_height,
            read_on_hit=False,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_next_period_figs.append(ronwdata_comp_swnp_fig)
    pbar.update(1)

    ###########################################################################
    # Read on Hit on Write data data compare single window and next period plot
    ###########################################################################
    with ignored(Exception):
        rhonwdata_comp_swnp_fig = plot_measure(
            tools,
            results,
            dates,
            filters,
            color_table,
            window_size,
            x_range=size_fig.x_range,
            title="Read on hit data / Written data - Compare single window and next period",
            run_type="run_next_period",
            datetimes=datetimes,
            plot_width=plot_width,
            plot_height=plot_height,
            read_on_hit=True,
            outer_legend=outer_legend,
            num_points=num_points,
        )
        run_next_period_figs.append(rhonwdata_comp_swnp_fig)
    pbar.update(1)

    figs.append(column(
        row(*run_full_normal_hit_rate_figs),
        row(*run_full_normal_size_figs),
        row(*run_full_normal_throughput_figs),
        row(*run_full_normal_cost_figs),
        row(*run_full_normal_net_figs),
        row(*run_full_normal_epsilon_figs),
        row(*run_full_normal_value_functions_figs),
        row(*run_full_normal_agent_action_figs),
        row(*run_full_normal_eviction_stat_figs),
        row(*run_full_normal_data_rw_figs),
        row(*run_full_normal_data_read_stats_figs),
        row(*run_full_normal_cpu_eff_figs),
        row(*run_single_window_figs),
        row(*run_next_period_figs),
    ))

    if html:
        save(column(*figs))
    if png:
        export_png(column(*figs), filename=os.path.join(
            folder, "results.png"))

    pbar.close()
