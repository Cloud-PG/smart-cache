import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


class Evaluator(object):

    def __init__(self, dataset, model):
        self._dataset = dataset
        self._model = model

    def compare_window(self, show: bool=False):
        raise NotImplemented

    def compare_next_window(self, show: bool=False):
        raise NotImplemented

    def compare_all(self, show: bool=False):
        raise NotImplemented
    
    @staticmethod
    def __plot_num_file(size_cache, size_ai_cache, stride: int=100):
        plt.clf()
        plt.plot(range(len(size_cache)), size_cache, label="cache", alpha=0.9)
        plt.plot(range(len(size_ai_cache)), size_ai_cache,
                 label="ai_cache", alpha=0.9)
        plt.ylabel("Num. files in cache")
        plt.xlabel("Num. request x {}".format(stride))
        plt.legend()


class SimpleCacheInfiniteSpace(Evaluator):

    def __init__(self, dataset, model):
        super(SimpleCacheInfiniteSpace, self).__init__(dataset, model)

    def _compare(
        self, initial_values: set=(), next_window: bool=False, stride: int=100
    ):
        cache = set()
        ai_cache = set()

        size_cache = []
        size_ai_cache = []

        if initial_values:
            init_cache, init_size_cache, init_ai_cache, init_size_ai_cache = initial_values
            cache |= set(init_cache)
            ai_cache |= set(init_ai_cache)
            size_cache += init_size_cache
            size_ai_cache += init_size_ai_cache

        tmp_file_names = []
        tmp_tensors = []

        generator = None
        if not next_window:
            generator = self._dataset.get_raw_window()
        else:
            generator = self._dataset.get_raw_next_window()

        for idx, obj in tqdm(enumerate(generator), desc="Simulation"):
            try:
                FileName = obj['data']['FileName']
                tensor = obj['tensor']
                tmp_file_names.append(FileName)
                tmp_tensors.append(tensor)

                if idx % stride == 0:
                    cache |= set(tmp_file_names)

                    predictions = self._model.predict(np.array(tmp_tensors))
                    for pred_idx, prediction in enumerate(predictions):
                        if prediction != 0:
                            ai_cache |= set((tmp_file_names[pred_idx],))

                    size_cache.append(len(cache))
                    size_ai_cache.append(len(ai_cache))

                    tmp_file_names = []
                    tmp_tensors = []
            except KeyError:
                print(idx, obj)

        return cache, size_cache, ai_cache, size_ai_cache

    def compare_window(self, show: bool=False, stride: int=100):
        cache, size_cache, ai_cache, size_ai_cache = self._compare(
            stride=stride)

        self.__plot_num_file(size_cache, size_ai_cache)
        if show:
            plt.show()
        else:
            plt.savefig("compare_window.png")

    def compare_next_window(self, show: bool=False, stride: int=100):
        cache, size_cache, ai_cache, size_ai_cache = self._compare(
            next_window=True, stride=stride
        )

        self.__plot_num_file(size_cache, size_ai_cache)
        if show:
            plt.show()
        else:
            plt.savefig("compare_next_window.png")

    def compare_all(self, show: bool=False, stride: int=100):
        cache, size_cache, ai_cache, size_ai_cache = self._compare(
            stride=stride)
        separator = len(size_cache)
        cache, size_cache, ai_cache, size_ai_cache = self._compare(
            initial_values=(cache, size_cache, ai_cache, size_ai_cache),
            next_window=True, stride=stride
        )

        self.__plot_num_file(size_cache, size_ai_cache)
        plt.axvline(x=separator)
        if show:
            plt.show()
        else:
            plt.savefig("compare_all.png")
