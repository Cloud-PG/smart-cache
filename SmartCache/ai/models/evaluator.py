import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


class SimpleCacheInfiniteSpace(object):

    def __init__(self, dataset, model):
        self._dataset = dataset
        self._model = model

    def _compare(self, next_window: bool=False, stride: int=100):
        cache = set()
        ai_cache = set()

        size_cache = []
        size_ai_cache = []

        tmp_file_names = []
        tmp_tensors = []

        generator = None
        if not next_window:
            generator = self._dataset.get_raw_window()
        else:
            generator = self._dataset.get_raw_next_window()

        for idx, obj in tqdm(enumerate(generator), desc="Simulation"):
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

        return cache, size_cache, ai_cache, size_ai_cache

    def compare_window(self, show: bool=False):
        cache, size_cache, ai_cache, size_ai_cache = self._compare()

        plt.clf()
        plt.plot(range(len(size_cache)), size_cache, label="cache")
        plt.plot(range(len(size_ai_cache)), size_ai_cache, label="ai_cache")
        plt.legend()
        if show:
            plt.show()
        else:
            plt.savefig("compare_window.png")

    def compare_next_window(self, show: bool=False):
        cache, size_cache, ai_cache, size_ai_cache = self._compare(
            next_window=True
        )

        plt.clf()
        plt.plot(range(len(size_cache)), size_cache, label="cache")
        plt.plot(range(len(size_ai_cache)), size_ai_cache, label="ai_cache")
        plt.legend()
        if show:
            plt.show()
        else:
            plt.savefig("compare_next_window.png")

    def compare_all(self):
        pass
