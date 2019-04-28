from .generator import Stage


class CMSRawStage(Stage):

    def __init__(
        self,
        name: str,
        input_source: 'Resource'=None,
        save_stage: bool=False,
        spark_conf: dict={}
    ):
        super(CMSRawStage, self).__init__(
            name,
            input_source=input_source,
            save_stage=save_stage,
            spark_conf=spark_conf
        )
