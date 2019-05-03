import sys
from os import chdir

sys.path.append("..")

from DataManager.collector.dataset.generator import Composer
from DataManager.collector.dataset.resource import CMSResourceManager
from DataManager.collector.dataset.stage import CMSRawStage


if __name__ == "__main__":
    # needed until the libraries are installed as a package
    chdir("..")

    cms_resource_manager = CMSResourceManager(
        sys.argv[1], int(sys.argv[2]),
        resource=eval(sys.argv[3])
    )

    raw_stage = CMSRawStage(
        save_stage=True,
        source=cms_resource_manager,
        spark_conf=eval(sys.argv[4]) if len(sys.argv) == 5 else {}
    )

    composer = Composer(
        dataset_name="CMS-RAW-Dataset",
        stages=[
            raw_stage
        ],
        spark_conf={
            'master': "local[4]",
            'config': {
                'spark.driver.memory': "8g",
                'spark.driver.maxResultSize': "2g",
                'spark.executor.memory': "2g"
            }
        }
    )

    composer.compose(use_spark=True)
    composer.save()
