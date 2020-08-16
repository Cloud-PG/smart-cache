#!/usr/bin/env python

_TEMPLATE = """--- # Simulation parameters
sim:
  data: ../../../../datasets/source2018_numeric_it_with_avro_order/
  outputFolder: ../../../../results/sim_weight_function_italian_dataset/100T
  type: normal
  window:
    start: 0
    stop: 52
  region: it
  overwrite: true
  cache:
    type: weightFunLRU
    watermarks: false
    size:
      value: 100
      unit: T
    bandwidth:
      value: 10
      redirect: true
  weightfunc:
    name: {function}
    alpha: {alpha}
    beta: {beta}
    gamma: {gamma}
"""

_VALUES = [0, 0.33, 0.5, 0.67, 1, 2, 4, 8, 16, 32, 64]
_FUNCTIONS = [
    "FuncAdditive",
    "FuncAdditiveExp",
    "FuncMultiplicative",
]


def main():
    for function in _FUNCTIONS:
        conf_num = 0
        print(f"==> Make function {function} configs...")
        tot_configs = len(_VALUES)**3
        for alpha_value in _VALUES:
            for beta_value in _VALUES:
                for gamma_value in _VALUES:
                    with open(
                        f"weightFun_{function}_test{conf_num:05d}.yml", "w"
                    ) as cur_config:
                        cur_config.write(
                            _TEMPLATE.format(
                                function=function,
                                alpha=alpha_value,
                                beta=beta_value,
                                gamma=gamma_value,
                            )
                        )
                        conf_num += 1
                        print(
                            f"==> Written config {conf_num}/{tot_configs} of {function}",
                            end="\r"
                        )


if __name__ == "__main__":
    main()
