from datetime import datetime


def date_from_timestamp_ms(timestamp: (int, float)) -> 'datetime':
    """Convert a millisecond timestamp to a date.

    Args:
        timestamp (int or float): the timestamp in milliseconds

    Returns:
        datetime: The corresponding date of the timestamp

    NOTE: for example, millisecond timestamp is used in HDFS
    """
    return datetime.fromtimestamp(float(timestamp) / 1000.)
