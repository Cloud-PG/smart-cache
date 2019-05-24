import sys
from os import chdir

from DataManager.collector.api import DataFile
from DataManager.collector.dataset.reader import CMSDatasetTest0Reader
from SmartCache.ai.models.evaluator import Evaluator
from SmartCache.ai.models.generator import CMSTest0ModelGenerator

if __name__ == "__main__":
    # needed until the libraries are installed as a package
    chdir("..")

    dataset = CMSDatasetTest0Reader(sys.argv[1])
    dataset.gen_support_table()
    # print(dataset.support_table)
    # dataset.score_show()

    model = CMSTest0ModelGenerator(epochs=10)
    model.train(dataset, k_fold=5)
    model.save("model_test0_kfold10_epochs10")
    model.load("model_test0_kfold10_epochs10")
    model.load("model_test0")

    test_file = DataFile(sys.argv[2])
    evaluator = Evaluator(
        test_file, model, dataset.support_table,
        cache_type='lru', ai_cache_type='lru',
        cache_settings={
            'max_size': 100000
        }
    )
    evaluator.compare(show=False, filename="cache_compare_kfold10.png")
