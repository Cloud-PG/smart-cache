class FileStats(object):

    def __init__(self, size: float):
        self._size: float = size
        self._hit: int = 0
        self._miss: int = 0

    def update(self, size: float, hit: bool = False):
        self._size = size
        if hit:
            self._hit += 1
        else:
            self._miss += 1

    @property
    def tot_requests(self):
        return self._hit + self._miss

    @property
    def hit(self):
        return self._hit

    @property
    def miss(self):
        return self._miss

    @property
    def size(self):
        return self._size


class Stats(object):

    def __init__(self):
        self._files = {}

    def get_or_set(self, filename: str, size: float) -> 'FileStats':
        stats = None
        if filename not in self._files:
            stats = FileStats(size)
            self._files[filename] = stats
        else:
            stats = self._files[filename]
        return stats


class LRU(object):

    def __init__(self, size: float = 104857600):
        """Initialize cache.

        Args:
            size (float): cache size in MB. Default = 10T

        """
        self._size: float = 0.0
        self._max_size = size
        self._files = {}
        self._queue = []

        self._stats = Stats()
        # Stat attributes
        self._hit: int = 0
        self._miss: int = 0
        self._written_data: float = 0.0
        self._deleted_data: float = 0.0
        self._read_data: float = 0.0

    def hit_rate(self) -> float:
        if self._hit:
            return self._hit / (self._hit + self._miss)
        return 0.0

    def check(self, filename: str) -> bool:
        return filename in self._files

    def get(self, filename: str, *args) -> bool:
        """Requesta a file to the cache.

        Args:
            args (list): list of tuple with:
                (size, ...)
        """
        hit = self.check(filename)
        stats = self.before_request(filename, hit, *args)
        added = self.update_policy(filename, stats, hit)
        self.after_request(stats, hit, added)
        pass

    def before_request(self, filename, hit: bool, *args) -> 'FileStats':
        stats = self._stats.get_or_set(filename, *args)
        stats.update(args[0], hit)
        return stats

    def update_policy(self, filename, file_stats, hit: bool) -> bool:
        if not hit:
            if self._size + file_stats.size <= self._max_size:
                self._files[filename] = file_stats
                self._queue.append(filename)
                self._size += file_stats.size
                return True
            else:
                while self._size + file_stats.size > self._max_size:
                    to_remove = self._queue.pop(0)
                    file_size = self._files[to_remove].size
                    self._size -= file_size
                    self._deleted_data += file_size
                    del self._files[to_remove]
                else:
                    self._files[filename] = file_stats
                    self._queue.append(filename)
                    self._size += file_stats.size
                    return True
        else:
            self._queue.remove(filename)
            self._queue.append(filename)

        return False

    def after_request(self, fileStats, hit: bool, added: bool):
        if hit:
            self._hit += 1
        else:
            self._miss += 1

        if added:
            self._written_data += fileStats.size

        self._read_data += fileStats.size


if __name__ == "__main__":
    cache = LRU(1000)
    cache.get("FILE A", 500.0)
    cache.get("FILE B", 500.0)
    cache.get("FILE A", 500.0)
    cache.get("FILE C", 500.0)
    print(cache._files.keys())
