import sys
from os import chdir

sys.path.append("..")

from DataManager.collector.dataset.reader import CMSDatasetTest0Reader


if __name__ == "__main__":
    # needed until the libraries are installed as a package
    chdir("..")

    data_reader = CMSDatasetTest0Reader(sys.argv[1])
    data_reader.score_show()
