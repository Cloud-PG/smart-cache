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
    if name.find("feature") != -1:
        epsilon, name = name.split("feature] ")
        epsilon = epsilon.replace("[NO", "").replace("_", "").strip()
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
            results[size][method] = {"size": {}, "normal": {}}

        if epsilon is None:
            epsilon = "normal"

        results[size][method][epsilon] = {
            "throughput": row.Throughput,
            "cost": row.Cost,
            "readOnHitRatio": row._4,
            "bandwidth": row.Bandwidth,
        }

        # print(results[size][name][epsilon])

no_size_throughput_diffs = []
no_size_cost_diffs = []
no_size_readOnHitRatio_diffs = []
no_size_bandwidth_diffs = []

calc_diff_perc = lambda a, b: ((abs(a - b) / ((a + b) / 2.0)) * 100.0) * (
    -1.0 if a > b else 1.0
)

for size, methods in results.items():
    for method, values in methods.items():
        if values["size"]:
            throughput_diff_no_size = calc_diff_perc(
                values["normal"]["throughput"], values["size"]["throughput"]
            )
            cost_diff_no_size = calc_diff_perc(
                values["normal"]["cost"], values["size"]["cost"]
            )
            readOnHitRatio_diff_no_size = calc_diff_perc(
                values["normal"]["readOnHitRatio"], values["size"]["readOnHitRatio"]
            )
            bandwidth_diff_no_size = calc_diff_perc(
                values["normal"]["bandwidth"], values["size"]["bandwidth"]
            )

            # print(values)
            print(
                f"|{size}",
                method,
                "throughput diff\n|-> ",
                "no size:",
                throughput_diff_no_size,
            )
            print(
                f"|{size}",
                method,
                "cost diff\n|-> ",
                "no size:",
                cost_diff_no_size,
            )
            print(
                f"|{size}",
                method,
                "readOnHitRatio diff\n|-> ",
                "no size:",
                readOnHitRatio_diff_no_size,
            )
            print(
                f"|{size}",
                method,
                "bandwidth diff\n|-> ",
                "no size:",
                bandwidth_diff_no_size,
            )

            no_size_throughput_diffs.append(throughput_diff_no_size)
            no_size_cost_diffs.append(cost_diff_no_size)
            no_size_readOnHitRatio_diffs.append(readOnHitRatio_diff_no_size)
            no_size_bandwidth_diffs.append(bandwidth_diff_no_size)

no_size_throughput_diffs = np.array(no_size_throughput_diffs)
no_size_cost_diffs = np.array(no_size_cost_diffs)
no_size_readOnHitRatio_diffs = np.array(no_size_readOnHitRatio_diffs)
no_size_bandwidth_diffs = np.array(no_size_bandwidth_diffs)

print("---")
print("no size throughput mean diff %", np.mean(no_size_throughput_diffs))
print("no size cost mean diff %", np.mean(no_size_cost_diffs))
print("no size readOnHitRatio mean diff %", np.mean(no_size_readOnHitRatio_diffs))
print("no size bandwidth mean diff %", np.mean(no_size_bandwidth_diffs))
