import sys
from os import chdir

from DataManager.collector.dataset.generator import Pipeline
from DataManager.collector.dataset.resource import CMSDatasetResourceManager
from DataManager.collector.dataset.stage import CMSRecordTest0Stage

if __name__ == "__main__":
    # needed until the libraries are installed as a package
    chdir("..")

    cms_resource_manager = CMSDatasetResourceManager(
        sys.argv[1]
    )

    test0_stage = CMSRecordTest0Stage()

    composer = Pipeline(
        dataset_name="CMS-Test0-Dataset",
        stages=[
            test0_stage,
        ],
        source=cms_resource_manager,
        spark_conf={
            'master': "local[6]",
            'config': {
                'spark.driver.memory': "8g",
                'spark.driver.maxResultSize': "6g",
                'spark.executor.memory': "2g"
            }
        }
    )

    composer.run(use_spark=True, save_stage=False)
    composer.save()
