import matplotlib.pyplot as plt
from tqdm import tqdm


class SimpleCacheInfiniteSpace(object):

    def __init__(self, dataset, model):
        self._dataset = dataset
        self._model = model

    def _compare(self, next_window: bool=False):
        cache = set()
        ai_cache = set()

        size_cache = []
        size_ai_cache = []

        # for idx in tqdm(range(self._dataset.meta.len_raw_week)):
        for idx in tqdm(range(10)):
            obj, tensor = self._dataset.get_raw(
                idx, as_tensor=True, next_window=next_window
            )
            FileName = obj['features']['FileName']

            cache |= set((FileName, ))

            if self._model.predict_single(tensor) != 0:
                ai_cache |= set((FileName, ))

            size_cache.append(len(cache))
            size_ai_cache.append(len(ai_cache))

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
