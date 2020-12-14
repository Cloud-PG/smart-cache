import pandas as pd
import numpy as np
from sys import argv

if len(argv) == 2:
    source_file = argv[1]
else:
    source_file = "table.csv"

df = pd.read_csv(source_file)
del df["Unnamed: 0"]

results = {}

for row in df.itertuples():
    # Pandas(Index=113, file='sizeSmall_10T_10Gbit_it', Throughput=-4.723705000000001, Cost=12.372252, _4=18.809344, Bandwidth=60.07386700000001, _6=1214.0881539999998, _7=57.096814)
    name = row.file
    size = None
    epsilon = None
    if name.find("epsilon") != -1:
        epsilon, name = name.split("] ")
        epsilon = epsilon.replace("[epsilon", "").strip()
    name = name.replace("aiRL_", "")
    method, size, *_ = name.split("_")

    print(name, size, epsilon)

    if (
        True
        # int(size.replace("T", "")) == 100
        # and method.find("onK") == -1
        # and method.find("SCDL2") != -1
    ):
        if size not in results:
            results[size] = {}

        if method not in results[size]:
            results[size][method] = {"fast": {}, "slow": {}, "normal": {}}

        if epsilon is None:
            epsilon = "normal"

        results[size][method][epsilon] = {
            "throughput": row.Throughput,
            "cost": row.Cost,
            "readOnHitRatio": row._4,
            "bandwidth": row.Bandwidth,
        }

        # print(results[size][name][epsilon])

fast_throughput_diffs = []
fast_cost_diffs = []
fast_readOnHitRatio_diffs = []
fast_bandwidth_diffs = []
slow_throughput_diffs = []
slow_cost_diffs = []
slow_readOnHitRatio_diffs = []
slow_bandwidth_diffs = []

calc_diff_perc = lambda a, b: ((abs(a - b) / ((a + b) / 2.0)) * 100.0) * (
    -1.0 if a > b else 1.0
)

for size, methods in results.items():
    for method, values in methods.items():
        if values["slow"] and values["fast"]:
            throughput_diff_slow = calc_diff_perc(
                values["normal"]["throughput"], values["slow"]["throughput"]
            )
            throughput_diff_fast = calc_diff_perc(
                values["normal"]["throughput"], values["fast"]["throughput"]
            )
            cost_diff_slow = calc_diff_perc(
                values["normal"]["cost"], values["slow"]["cost"]
            )
            cost_diff_fast = calc_diff_perc(
                values["normal"]["cost"], values["fast"]["cost"]
            )
            readOnHitRatio_diff_slow = calc_diff_perc(
                values["normal"]["readOnHitRatio"], values["slow"]["readOnHitRatio"]
            )
            readOnHitRatio_diff_fast = calc_diff_perc(
                values["normal"]["readOnHitRatio"], values["fast"]["readOnHitRatio"]
            )
            bandwidth_diff_slow = calc_diff_perc(
                values["normal"]["bandwidth"], values["slow"]["bandwidth"]
            )
            bandwidth_diff_fast = calc_diff_perc(
                values["normal"]["bandwidth"], values["fast"]["bandwidth"]
            )

            # print(values)
            print(
                f"|{size}",
                method,
                "throughput diff\n|-> ",
                "fast:",
                throughput_diff_fast,
                "\tslow:",
                throughput_diff_slow,
            )
            print(
                f"|{size}",
                method,
                "cost diff\n|-> ",
                "fast:",
                cost_diff_fast,
                "\tslow:",
                cost_diff_slow,
            )
            print(
                f"|{size}",
                method,
                "readOnHitRatio diff\n|-> ",
                "fast:",
                readOnHitRatio_diff_fast,
                "\tslow:",
                readOnHitRatio_diff_slow,
            )
            print(
                f"|{size}",
                method,
                "bandwidth diff\n|-> ",
                "fast:",
                bandwidth_diff_fast,
                "\tslow:",
                bandwidth_diff_slow,
            )

            fast_throughput_diffs.append(throughput_diff_fast)
            fast_cost_diffs.append(cost_diff_fast)
            fast_readOnHitRatio_diffs.append(readOnHitRatio_diff_fast)
            fast_bandwidth_diffs.append(bandwidth_diff_fast)
            slow_throughput_diffs.append(throughput_diff_slow)
            slow_cost_diffs.append(cost_diff_slow)
            slow_readOnHitRatio_diffs.append(readOnHitRatio_diff_slow)
            slow_bandwidth_diffs.append(bandwidth_diff_slow)

fast_throughput_diffs = np.array(fast_throughput_diffs)
fast_cost_diffs = np.array(fast_cost_diffs)
fast_readOnHitRatio_diffs = np.array(fast_readOnHitRatio_diffs)
fast_bandwidth_diffs = np.array(fast_bandwidth_diffs)
slow_throughput_diffs = np.array(slow_throughput_diffs)
slow_cost_diffs = np.array(slow_cost_diffs)
slow_readOnHitRatio_diffs = np.array(slow_readOnHitRatio_diffs)
slow_bandwidth_diffs = np.array(slow_bandwidth_diffs)

print("---")
print("fast throughput mean diff %", np.mean(fast_throughput_diffs))
print("fast cost mean diff %", np.mean(fast_cost_diffs))
print("fast readOnHitRatio mean diff %", np.mean(fast_readOnHitRatio_diffs))
print("fast bandwidth mean diff %", np.mean(fast_bandwidth_diffs))
print("slow throughput mean diff %", np.mean(slow_throughput_diffs))
print("slow cost mean diff %", np.mean(slow_cost_diffs))
print("slow readOnHitRatio mean diff %", np.mean(slow_readOnHitRatio_diffs))
print("slow bandwidth mean diff %", np.mean(slow_bandwidth_diffs))
