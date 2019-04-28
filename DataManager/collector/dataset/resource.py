

from .generator import Resource


class CMSResource(Resource):

    def __init__(
        self,
        spark_conf: dict={},
        httpfs: "HTTPFS" = None,
        httpfs_base_path: str = "",
        hdfs_base_path: str = "",
        local_folder: str = ""
    ):
        super(CMSResource, self).__init__(spark_conf=spark_conf)

    @property
    def type(self):
        if self.httpfs:
            return 'httpfs'
        elif self.hdfs_base_path:
            return 'hdfs'
        elif self.local_folder:
            return 'local'
        else:
            raise Exception("Cannot determine type...")
