import gzip
from os import path, walk, makedirs
from sys import argv

import pandas as pd


def main():
    source_folder = argv[1]
    out_folder = f"{source_folder}_clean"
    makedirs(out_folder, exist_ok=True)
    for root, _, files in walk(source_folder):
        for file_ in files:
            _, ext = path.splitext(file_)
            if ext == ".gz":
                print(f"=> {file_}")
                with gzip.GzipFile(path.join(root, file_)) as gzipfile:
                    df = pd.read_csv(gzipfile)
                    print(f"==> Cur. schape: {df.shape}")
                    print(f"===> Remove nan...")
                    df = df.dropna()
                    if 'index' in df.columns:
                        print(f"===> Remove index...")
                        del df['index']
                    print(f"==> Cur. schape: {df.shape}")

                with gzip.GzipFile(path.join(out_folder, file_), "wb") as cleanfile:
                    cleanfile.write(df.to_csv(index=False).encode("utf-8"))


if __name__ == "__main__":
    main()
