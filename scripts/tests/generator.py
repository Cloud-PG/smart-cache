import argparse
import pathlib
from copy import copy
from dataclasses import dataclass
from shutil import rmtree
from os import makedirs

_TEMPLATE = """--- # Simulation parameters
sim:
  data: {conf.data_path}/source2018_numeric_{conf.region}_with_avro_order
  outputFolder: ./results/{conf.out_folder}
  type: normal
  overwrite: true
  window:
    start: {conf.w_start}
    stop: {conf.w_stop}
  region: {conf.region}
  cache:
    type: {conf.cache_type}
    watermarks: {conf.cache_watermark}
    size:
      value: {conf.cache_size}
      unit: T
    bandwidth:
      value: 10
      redirect: true
"""

_TEMPLATE_SCDL = """--- # Simulation parameters
sim:
  data: {conf.data_path}/source2018_numeric_{conf.region}_with_avro_order
  outputFolder: ./results/{conf.out_folder}
  type: normal
  overwrite: true
  window:
    start: {conf.w_start}
    stop: {conf.w_stop}
  region: {conf.region}
  cache:
    type: aiRL
    watermarks: {conf.cache_watermark}
    size:
      value: {conf.cache_size}
      unit: T
    bandwidth:
      value: 10
      redirect: true
  ai:
    rl:
      type: SCDL
      epsilon:
        decay: {conf.epsilonDecay}
        unleash: false
      addition:
        featuremap: {conf.featureMap}
"""

_TEMPLATE_SCDL2 = """--- # Simulation parameters
sim:
  data: {conf.data_path}/source2018_numeric_{conf.region}_with_avro_order
  outputFolder: ./results/{conf.out_folder}
  type: normal
  overwrite: true
  window:
    start: {conf.w_start}
    stop: {conf.w_stop}
  region: {conf.region}
  cache:
    type: aiRL
    watermarks: {conf.cache_watermark}
    size:
      value: {conf.cache_size}
      unit: T
    bandwidth:
      value: 10
      redirect: true
  ai:
    rl:
      type: SCDL2
      epsilon:
        unleash: false
      addition:
        featuremap: {conf.additionFeatureMap}
        epsilon:
            decay: {conf.additionEpsilonDecay}
      eviction:
        type: {conf.evictionType}
        k: {conf.k}
        featuremap: {conf.evictionFeatureMap}
        epsilon:
            decay: {conf.evictionEpsilonDecay}
"""

_CONFIG_FOLDER = "./configs"


@dataclass
class ConfigParameters:
    data_path: str
    region: str = "it"
    w_start: int = 0
    w_stop: int = 52
    cache_type: str = "lru"
    cache_watermark: bool = True
    cache_size: int = 100
    ai_type: str = ""
    evictionType: str = ""
    epsilonDecay: float = 0.1
    featureMap: str = ""
    additionFeatureMap: str = ""
    evictionFeatureMap: str = ""
    additionEpsilonDecay: float = 0.1
    evictionEpsilonDecay: float = 0.1
    k: int = 1

    @property
    def is_AI(self):
        return self.cache_type.lower() == "airl"

    @property
    def is_SCDL(self):
        return self.is_AI and self.ai_type == "SCDL"

    @property
    def is_SCDL2(self):
        return self.is_AI and self.ai_type == "SCDL2"

    @property
    def is_onK(self):
        return self.is_AI and self.evictionType.lower() == "onk"

    @property
    def is_noEviction(self):
        return self.is_AI and self.evictionType.lower() == "noeviction"

    @property
    def out_folder(self):
        if self.cache_type.lower() != "airl":
            return (
                pathlib.Path(".")
                .joinpath(
                    self.region,
                    f"{self.w_start}_{self.w_stop}",
                    "watermarks" if self.cache_watermark else "noWatermarks",
                    f"{self.cache_size}",
                )
                .as_posix()
            )
        elif self.ai_type == "SCDL":
            return (
                pathlib.Path(".")
                .joinpath(
                    self.region,
                    f"{self.w_start}_{self.w_stop}",
                    "watermarks" if self.cache_watermark else "noWatermarks",
                    f"{self.cache_size}",
                    f"{self.epsilonDecay:0.6f}",
                )
                .as_posix()
            )
        elif self.ai_type == "SCDL2":
            return (
                pathlib.Path(".")
                .joinpath(
                    self.region,
                    f"{self.w_start}_{self.w_stop}",
                    "watermarks" if self.cache_watermark else "noWatermarks",
                    f"{self.cache_size}",
                    f"A{self.additionEpsilonDecay:0.6f}",
                    f"E{self.additionEpsilonDecay:0.6f}",
                )
                .as_posix()
            )


def compose(
    base: list["ConfigParameters"], param, list_: list, condition=None
) -> list["ConfigParameters"]:
    new_list = []
    if condition is not None:
        new_list = [elm for elm in base if not condition(elm)]
        base = [elm for elm in base if condition(elm)]
    for value in list_:
        for elm in base:
            tmp = copy(elm)
            if isinstance(param, list):
                for idx, parameter in enumerate(param):
                    setattr(tmp, parameter, value[idx])
            else:
                setattr(tmp, param, value)
            new_list.append(tmp)
    return new_list


def generator(data_path: "pathlib.Path", args):
    assert data_path.is_dir()
    base = [
        ConfigParameters(
            data_path=data_path.as_posix(),
            featureMap=args.featureMap,
            additionFeatureMap=args.additionFeatureMap,
            evictionFeatureMap=args.evictionFeatureMap,
        )
    ]

    regions = [
        "it",
        "us",
    ]
    windows = [
        (0, 4),
        (4, 8),
        (8, 12),
        (12, 16),
        (16, 20),
        (20, 24),
        (24, 28),
        (28, 32),
        (32, 36),
        (36, 40),
        (40, 44),
        (44, 48),
        (48, 52),
        # Trimester
        (0, 12),
        (12, 24),
        (24, 36),
        (36, 48),
        (48, 52),
        # Quadrimester
        (0, 16),
        (16, 32),
        (32, 48),
        (48, 52),
    ]
    cache_types = [
        ("aiRL", "SCDL", ""),
        ("aiRL", "SCDL2", "onK"),
        ("aiRL", "SCDL2", "onFree"),
        ("aiRL", "SCDL2", "NoEviction"),
        ("lru", "", ""),
        ("lfu", "", ""),
        ("sizeBig", "", ""),
        ("sizeSmall", "", ""),
    ]
    cache_watermarks = [
        True,
        False,
    ]
    cache_sizes = [
        100,
        200,
    ]
    epsilon = [
        0.1,
        0.001,
        0.0001,
    ]
    k = [
        1024,
        2048,
        4096,
    ]

    print("Compose parameters...")
    configs = compose(base, "region", regions)
    configs = compose(configs, ["cache_type", "ai_type", "evictionType"], cache_types)
    configs = compose(configs, "cache_watermark", cache_watermarks)
    configs = compose(configs, "cache_size", cache_sizes)
    configs = compose(configs, ["w_start", "w_stop"], windows)
    configs = compose(configs, "epsilonDecay", epsilon, lambda elm: elm.is_SCDL)
    configs = compose(
        configs, "additionEpsilonDecay", epsilon, lambda elm: elm.is_SCDL2
    )
    configs = compose(
        configs,
        "evictionEpsilonDecay",
        epsilon,
        lambda elm: elm.is_SCDL2 and not elm.is_noEviction,
    )
    configs = compose(configs, "k", k, lambda elm: elm.is_SCDL2 and elm.is_onK)

    print("Remove previous configurations")
    config_out_folder = pathlib.Path(_CONFIG_FOLDER)
    if config_out_folder.exists():
        rmtree(config_out_folder)

    makedirs(config_out_folder)

    print("Make configs...")
    for idx, conf in enumerate(sorted(configs, key=lambda elm: elm.region)):
        file_ = config_out_folder.joinpath(f"{conf.region}_{idx}.yml")
        with open(file_, "w") as out_file:
            if conf.ai_type == "SCDL":
                out_file.write(_TEMPLATE_SCDL.format(conf=conf))
            elif conf.ai_type == "SCDL2":
                out_file.write(_TEMPLATE_SCDL2.format(conf=conf))
            else:
                out_file.write(_TEMPLATE.format(conf=conf))
        print(f"[{idx+1}/{len(configs)}] written", end="\r")
    else:
        print("All configuration files are created...")

    print("Now you can run all the simulation with the following command:")
    print("$ find configs/* | xargs -I conf simulator.exe sim conf")


def main():

    parser = argparse.ArgumentParser(description="a config generator")

    parser.add_argument("data_path", type=str, help="the main folder of the datasets")
    parser.add_argument(
        "--featureMap", type=str, default="", help="the SCDL feature map"
    )
    parser.add_argument(
        "--additionFeatureMap",
        type=str,
        default="",
        help="the SCDL2 addition feature map",
    )
    parser.add_argument(
        "--evictionFeatureMap",
        type=str,
        default="",
        help="the SCDL2 eviction feature map",
    )

    args = parser.parse_args()

    generator(pathlib.Path(args.data_path), args)


if __name__ == "__main__":
    main()
