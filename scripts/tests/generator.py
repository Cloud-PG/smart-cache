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

    @property
    def out_folder(self):
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


def compose(
    base: list["ConfigParameters"], param, list_: list
) -> list["ConfigParameters"]:
    new_list = []
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


def generator(data_path: "pathlib.Path"):
    assert data_path.is_dir()
    base = [ConfigParameters(data_path=data_path.as_posix())]

    regions = ["it", "us"]
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
    ]
    cache_types = ["lru", "lfu", "sizeBig", "sizeSmall"]
    cache_watermarks = [True, False]
    cache_sizes = [100, 200]

    print("Compose parameters...")
    configs = compose(base, "region", regions)
    configs = compose(configs, "cache_type", cache_types)
    configs = compose(configs, "cache_watermark", cache_watermarks)
    configs = compose(configs, "cache_size", cache_sizes)
    configs = compose(configs, ["w_start", "w_stop"], windows)

    print("Remove previous configurations")
    config_out_folder = pathlib.Path(_CONFIG_FOLDER)
    if config_out_folder.exists():
        rmtree(config_out_folder)

    makedirs(config_out_folder)

    print("Make configs...")
    for idx, conf in enumerate(configs):
        file_ = config_out_folder.joinpath(f"{idx}.yml")
        with open(file_, "w") as out_file:
            out_file.write(_TEMPLATE.format(conf=conf))
        print(f"[{idx+1}/{len(configs)}] written", end="\r")
    else:
        print("All configuration files are created...")

    print("Now you can run all the simulation with the following command:")
    print("$ find configs/* | xargs -I conf simulator.exe sim conf")


def main():

    parser = argparse.ArgumentParser(description="a config generator")

    parser.add_argument("data_path", type=str, help="the main folder of the datasets")

    args = parser.parse_args()

    generator(pathlib.Path(args.data_path))


if __name__ == "__main__":
    main()
