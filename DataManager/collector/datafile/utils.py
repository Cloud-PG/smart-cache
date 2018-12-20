
__all__ = ['gen_increasing_slice']


def gen_increasing_slice(slice):
    """Generate a sequence of indexes from a slice.

    Args:
        slice (slice): the slice object to expand

    Returns:
        generator: the indexes in increasing order

    """
    start = slice.start if slice.start else 0
    stop = slice.stop
    step = slice.step if slice.step else 1
    if start > stop:
        start, stop = stop + 1, start + 1
    cur = start
    while cur < stop:
        assert cur >= 0, "Negative index not supported..."
        yield cur
        cur += step
