import sys
from os import chdir

sys.path.append("..")

from SmartCache.ai.models.generator import CMSTest0ModelGenerator
from SmartCache.ai.models.evaluator import Evaluator
from DataManager.collector.dataset.reader import CMSDatasetTest0Reader
from DataManager.collector.api import DataFile


if __name__ == "__main__":
    # needed until the libraries are installed as a package
    chdir("..")

    dataset = CMSDatasetTest0Reader(sys.argv[1])
    dataset.gen_support_table()
    # print(dataset.support_table)
    # dataset.score_show()

    model = CMSTest0ModelGenerator()
    model.train(dataset)
    model.save("model_test0")
    model.load("model_test0")

    test_file = DataFile(sys.argv[2])
    evaluator = Evaluator(
        test_file, model, dataset.support_table,
        cache_type='lru', ai_cache_type='lru'
    )
    evaluator.compare(show=False)
