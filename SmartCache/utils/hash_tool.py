from hashlib import blake2b
from sys import argv


def hash_hexdigest(string: str, digest_size=8) -> str:
    cur_h = blake2b(
        digest_size=digest_size
    )
    cur_h.update(string.encode("ascii"))
    return cur_h.hexdigest()


def hash_int(string, digest_size=8, num_digits=10) -> int:
    cur_hash = hash_hexdigest(string, digest_size=digest_size)
    return int(cur_hash, 16) % 10**num_digits


if __name__ == "__main__":
    if len(argv) == 3:
        if argv[1] == "hex":
            print(hash_hexdigest(argv[2]))
        elif argv[1] == "int":
            print(hash_int(argv[2]))
        else:
            print(f"Error: Unrecognized target {argv[1]}")
    else:
        print("Too many arguments... usage: target[hex, int] string_value")
