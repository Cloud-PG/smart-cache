import random
import time
from stats_mod import Stats as RustStats
from stats_mod import FileStats as RustFileStats
import numpy as np


class FileStats(object):
    '''
    object that contains all information about a file: size, number of hits,
     number of miss, last request id, recency (distance from last request), datatype (mc or data)
    '''

    __slots__ = ["_size", "_hit", "_miss",
                 "_last_request", "_datatype"]

    def __init__(self, size: float, datatype: int):
        self._size: float = size
        self._hit: int = 0
        self._miss: int = 0
        self._last_request: int = 0
        self._datatype: int = datatype

    def update(self, hit: bool = False):
        '''add hit or miss '''
        if hit:
            self._hit += 1
        else:
            self._miss += 1

    @property
    def tot_requests(self):
        '''returns total number of requests'''
        return self._hit + self._miss

    @property
    def last_request(self):
        '''returns total number of requests'''
        return self._last_request

    @last_request.setter
    def last_request(self, new_val: int):
        '''update total number of requests'''
        self._last_request = new_val

    @property
    def datatype(self):
        '''returns total number of hits'''
        return self._datatype

    @property
    def hit(self):
        '''returns total number of hits'''
        return self._hit

    @property
    def miss(self):
        '''returns total number of miss'''
        return self._miss

    @property
    def size(self):
        '''returns size'''
        return self._size


class Stats(object):
    ''' object that contains a list of all filestats '''

    def __init__(self):
        self._files = {}

    def get_or_set(self, filename: str, size: float, datatype: int, request: int) -> 'FileStats':
        '''return filestat for that file: if no stat for that file is in list, creates a new stat and adds it to stats, otherwise it simply gets it '''
        stats = None
        if filename not in self._files:
            stats = FileStats(size, datatype)
            # print(f"request -> {request}")
            stats.last_request = request
            self._files[filename] = stats
        else:
            stats = self._files[filename]
            #stats._last_request = request
        return stats


def main():

    np.random.seed(42)
    random.seed(42)

    num_req = 1000000
    hits = np.random.randint(2, size=num_req).astype(bool)
    num_files = 10000
    files = [idx for idx in range(num_files)]
    sizes = [random.randint(100, 4000) for _ in range(num_files)]
    data_types = [random.randint(0, 2) for _ in range(num_files)]

    requests = list(zip(files, sizes, data_types))
    random.shuffle(requests)

    python_stats = Stats()
    rust_stats = RustStats()
    python_results = []
    rust_results = []

    start = time.time()
    for idx in range(num_req):
        cur_req = requests[idx % len(requests)]
        cur_stat = python_stats.get_or_set(*cur_req, idx)
        if hits[idx]:
            cur_stat.update(True)
        else:
            cur_stat.update(False)
        # print(cur_stat.last_request)
        curValues = np.array([
            cur_stat.size,
            cur_stat.tot_requests,
            idx - cur_stat.last_request,
            0. if cur_stat.datatype == 0 else 1.,
        ])
        python_results.append(curValues)

    print(f"Python: {time.time() - start}s")

    start = time.time()
    for idx in range(num_req):
        cur_req = requests[idx % len(requests)]
        cur_stat = rust_stats.get_or_set(*cur_req, idx)
        if hits[idx]:
            cur_stat.update(True)
        else:
            cur_stat.update(False)
        curValues = np.array([
            cur_stat.size,
            cur_stat.tot_requests,
            idx - cur_stat.last_request,
            0. if cur_stat.datatype == 0 else 1.,
        ])
        rust_results.append(curValues)

    print(f"Rust Mod: {time.time() - start}s")

    for idx, (python_res, rust_res) in enumerate(zip(python_results, rust_results)):
        # print("---")
        for in_idx in range(len(python_res)):
            # print(python_res[in_idx], rust_res[in_idx],
            #       requests[idx % len(requests)])
            assert python_res[in_idx] == rust_res[in_idx]


if __name__ == "__main__":
    main()
