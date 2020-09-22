import pandas as pd
from tqdm import tqdm
import pathlib


def main():
    main_folder = pathlib.Path(".").resolve()
    print(main_folder.as_posix(), list(main_folder.glob("**/*.csv.gz")))

    for dir_ in main_folder.iterdir():
        if dir_.is_dir():
            for file_ in tqdm(dir_.glob("*.csv.gz"),
                              desc=f"{dir_}"):
                df = pd.read_csv(file_.as_posix())
                df = df[df.JobSuccess]

                sum_sizes = df.Size.sum()
                file_sizes = df[['Filename', 'Size']
                                ].drop_duplicates('Filename').Size.sum()
                num_req = len(df.index)
                num_files = df.Filename.nunique()

                max_hit_rate = ((num_req-num_files) / num_req) * 100.
                max_read_on_hit = ((sum_sizes - file_sizes) / sum_sizes) * 100.

                print(file_, max_hit_rate, max_read_on_hit)


if __name__ == "__main__":
    main()
