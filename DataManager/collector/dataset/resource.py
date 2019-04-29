import os
from os import makedirs, path

from tqdm import tqdm
from yaspin import yaspin

from ...agent.api import HTTPFS
from ..api import DataFile
from ..datafile.json import JSONDataFileWriter
from .generator import Resource
from .utils import gen_window_dates


class CMSResourceManager(Resource):

    def __init__(
        self,
        start_date: str,
        window_size: int,
        spark_conf: dict={},
        resource: dict={}
    ):
        super(CMSResourceManager, self).__init__(spark_conf=spark_conf)

        self._year, self._month, self._day = [
            int(elm) for elm in start_date.split()
        ]
        self._window_size = window_size

        # Default values
        self._httpfs = None
        self._hdfs_base_path = ""
        self._local_folder = ""

        if 'httpfs' in resource:
            self._httpfs = HTTPFS(
                resource['httpfs'].get('url'),
                resource['httpfs'].get('user', None),
                resource['httpfs'].get('password', None)
            )
            self._httpfs_base_path = resource['httpfs'].get(
                'base_path', "/project/awg/cms/jm-data-popularity/avro-snappy/"
            )
        elif 'hdfs' in resource:
            self._hdfs_base_path = resource['hdfs'].get(
                'hdfs_base_path', "hdfs://analytix/project/awg/cms/jm-data-popularity/avro-snappy"
            )
        elif 'local' in resource:
            self._local_folder = resource['local'].get(
                'folder', "data",
            )

    @property
    def type(self):
        if self._httpfs:
            return 'httpfs'
        elif self._hdfs_base_path:
            return 'hdfs'
        elif self._local_folder:
            return 'local'
        else:
            raise Exception("Cannot determine type...")

    def get(self) -> 'DataFile':
        for year, month, day in gen_window_dates(
                self._year, self._month, self._day, self._window_size):
            if self._httpfs is not None:
                for type_, name, full_path in self._httpfs.liststatus(
                        "{}year={}/month={}/day={}".format(
                            self._httpfs_base_path, year, month, day
                        )
                ):
                    cur_file = self._httpfs.open(full_path)
                    collector = DataFile(cur_file)
            elif self._hdfs_base_path:
                sc = self.spark_context
                binary_file = sc.binaryFiles("{}/year={:4d}/month={:d}/day={:d}/part-m-00000.avro".format(
                    self._hdfs_base_path, year, month, day)
                ).collect()
                collector = DataFile(binary_file[0])
            elif self._local_folder:
                cur_file_path = path.join(
                    path.abspath(self._local_folder),
                    "year={}".format(year),
                    "month={}".format(month),
                    "day={}".format(day),
                    "part-m-00000.avro"
                )
                collector = DataFile(cur_file_path)
            else:
                raise Exception("No methods to retrieve data...")
            yield collector

    def set(self, data, stage_name: str='', out_dir: str='cache'):
        out_name = "dataset_y{}-m{}-d{}_ws{}_stage-{}.json.gz".format(
            self._year,
            self._month,
            self._day,
            self._window_size,
            stage_name
        )
        tmp_name = "tmp_{}".format(out_name)

        with JSONDataFileWriter(tmp_name) as tmp_file:
            for record in tqdm(data, desc="Create temporary file"):
                tmp_file.append(record)

        with yaspin(text="[Save Dataset]") as spinner:
            if self.type == 'local':
                cur_base_path = path.join(
                    self._local_folder,
                    out_dir
                )
                makedirs(cur_base_path, exist_ok=True)
                with open(tmp_name, 'rb') as cur_file:
                    with open(path.join(cur_base_path, out_name), 'wb'
                              ) as out_file:
                        out_file.write(cur_file.read())
            elif self.type == 'httpfs':
                self._httpfs.create(
                    path.join(out_dir, out_name),
                    tmp_name,
                    overwrite=True
                )
            else:
                raise Exception(
                    "Save to '{}' not implemented...".format(self.type))

            spinner.text = "[Remove temporary data]"
            os.remove(tmp_name)

            spinner.write("[Dataset saved]")
