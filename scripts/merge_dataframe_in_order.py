import pandas as pd
import hashlib


def add_checksum(df):
    checksums = []
    counters = {}

    for row in df.itertuples():
        if row.Filename not in counters:
            counters[row.Filename] = 0
        else:
            counters[row.Filename] += 1
        data = f"{row.Filename}{row.Job}{row.DataType}{row.UserID}{counters[row.Filename]}"
        hash_ = hashlib.blake2s(digest_size=8)
        hash_.update(data.encode("ascii"))
        digest = hash_.hexdigest()
        checksums.append(digest)

    df['checksum'] = checksums
    return df


def add_index(df):
    df['idx'] = list(range(df.shape[0]))
    return df


def match_order(original, current):
    return current.merge(original, on='checksum', suffixes=("_cur", "_original")).sort_values(by="idx_original")


def main():
    df = pd.DataFrame(
        [
            ["A", "test", "data", "user1"],
            ["A", "test", "data", "user1"],
            ["A", "test", "data", "user1"],
            ["B", "test", "mc", "user1"],
            ["B", "test", "mc", "user2"],
            ["C", "test", "data", "user1"],
        ],
        columns=["Filename", "Job", "DataType", "UserID"]
    )
    real_order_df = add_index(add_checksum(pd.DataFrame(
        [
            ["A", "test", "data", "user1"],
            ["B", "test", "mc", "user1"],
            ["A", "test", "data", "user1"],
            ["A", "test", "data", "user1"],
            ["A", "test", "data", "user1"],
            ["C", "test", "data", "user1"],
            ["C", "test", "data", "user4"],
            ["D", "test", "mc", "user1"],
            ["D", "test", "mc", "user2"],
            ["C", "test", "data", "user2"],
            ["B", "test", "mc", "user2"],
            ["B", "test", "mc", "user42"],
            ["C", "test", "data", "user5"],
            ["C", "test", "data", "user4"],
            ["D", "test", "mc", "user3"],
            ["D", "test", "data", "user3"],
        ],
        columns=["Filename", "Job", "DataType", "UserID"]
    )))[['Filename', 'checksum', 'idx']]
    print(df)
    print(add_index(add_checksum(df)))
    print(real_order_df)
    print(match_order(real_order_df, df))


if __name__ == "__main__":
    main()
