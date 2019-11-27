import numpy as np
import pandas as pd


def get2PTAS(dataframe, cache_size: float):
    greedy_selection = np.zeros(len(dataframe)).astype(bool)

    cur_size = 0.
    cur_score = 0.

    for idx, cur_row in enumerate(dataframe.itertuples()):
        file_size = cur_row.size
        if cur_size + file_size <= cache_size:
            cur_size += file_size
            cur_score += cur_row.value
            greedy_selection[idx] = True
        else:
            break

    return pd.Series(greedy_selection)
