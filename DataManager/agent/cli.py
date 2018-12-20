import argparse
from sys import exit
from time import time

from ..collector.interface import DataFileInterface
from .api import ElasticSearchHttp


def cli_main():
    parser = argparse.ArgumentParser(
        description='Data Manager Agent')

    cmd_subparsers = parser.add_subparsers(
        title="jsongz commands", help="The library commands", dest="command")

    subcmd_get = cmd_subparsers.add_parser(
        "get", help="Get a JSON from the source")
    subcmd_get.add_argument("source", metavar="source", type=str,
                            help="The source target")
    subcmd_get.add_argument("index", metavar="range", type=str,
                            help="An integer index or a slice like [start:stop:step]")
    subcmd_get.add_argument("--type", metavar="source_type", choices=['file'], default="file",
                            help="A resource type in the following list: 'file', ... ")

    subcmd_put = cmd_subparsers.add_parser(
        "put", help="Put a JSON into the archive")
    subcmd_put.add_argument("source", metavar="source", type=str,
                            help="The source target")
    subcmd_put.add_argument("dest", metavar="destination", type=str,
                            help="The destination target")
    subcmd_put.add_argument("--source-type", metavar="source_type", choices=['file'], default="file",
                            help="A resource type in the following list: 'file', ... ")
    subcmd_put.add_argument("--dest-type", metavar="dest_type", choices=['elasticsearchhttp'], default="elasticsearchhttp",
                            help="A destination type in the following list: 'elasticsearchhttp', ... ")
    subcmd_put.add_argument("--auth", metavar="auth", type=str, default="",
                            help="User and password of destination resource: 'user:password'")

    subcmd_del = cmd_subparsers.add_parser(
        "del", help="Delete a JSON from the archive")

    args, _ = parser.parse_known_args()

    print(args)

    if args.command == "get":
        if args.type == "file":
            collector = DataFileInterface(args.source)
            try:
                index = int(args.index)
            except ValueError as err:
                index = slice(
                    *[int(elm) if elm else None for elm in args.index[1:-1].split(":")]
                )
            print(collector[index])
    elif args.command == "put":
        if args.source_type == "file":
            source = DataFileInterface(args.source)
        if args.dest_type == "elasticsearchhttp":
            dest = ElasticSearchHttp(args.dest, args.auth)

        command_start = time()
        for idx, data in enumerate(source):
            start = time()

            res = dest.put(data)

            if res.status_code > 201:
                print("[ERROR][STATUS CODE][{}]-> Problem with resource index {}".format(
                    res.status_code, idx)
                )
                print("[ERROR][DETAILS][\n\n{}\n]".format(res.text))

            print("Inserted element {} in {:0.5f}s".format(
                idx, time()-start), end='\r')

        print("[COMMAND][PUT][DONE in {:0.5f}s]-> All elements have been inserted...".format(
            time()-command_start)
        )


if __name__ == "__main__":
    exit(cli_main())
