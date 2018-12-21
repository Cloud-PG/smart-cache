import argparse
from datetime import timedelta
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
    subcmd_put.add_argument("--bulk", metavar="bulk", type=int, default=0,
                            help="Size of the bulk bucket")
    subcmd_put.add_argument("--start-from-index", metavar="initial_index", type=int, default=0,
                            help="The index of the first item")

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
        BULK_SIZE = args.bulk

        if args.source_type == "file":
            source = DataFileInterface(args.source)
        if args.dest_type == "elasticsearchhttp":
            dest = ElasticSearchHttp(args.dest, args.auth)

        command_start = time()
        tot_elm_done = 0
        bucket = []

        def put_bucket(cur_dest, cur_bucket, prev_tot_elm_done):
            start = time()
            res = cur_dest.put(cur_bucket)
            if res.status_code > 201:
                print("[ERROR][STATUS CODE][{}]-> Problem with bulk insertion".format(
                    res.status_code)
                )
                print("[ERROR][DETAILS][\n\n{}\n]".format(res.text))
                exit(-1)

            tot_elm_done = prev_tot_elm_done + len(bucket)
            print("[COMMAND][PUT][INSERT][{} elements in {:0.5f}s][Tot elements inserted {}][Elapsed time: {:0>8}s]".format(
                len(cur_bucket), time()-start, tot_elm_done, str(timedelta(seconds=time()-command_start))), end='\r')

            return tot_elm_done

        INITIAL_INDEX = args.start_from_index
        for idx, data in enumerate(source):
            if idx >= INITIAL_INDEX:
                if args.bulk == 0:
                    start = time()
                    res = dest.put(data)

                    if res.status_code > 201:
                        print("[ERROR][STATUS CODE][{}]-> Problem with resource index {}".format(
                            res.status_code, idx)
                        )
                        print("[ERROR][DETAILS][\n\n{}\n]".format(res.text))
                        exit(-1)

                    tot_elm_done += 1
                    print("[COMMAND][PUT][INSERT][Element {}][DONE in {:0.5f}s][Elapsed time: {:0>8}s]".format(
                        idx, time()-start,  str(timedelta(seconds=time()-command_start))), end='\r')
                else:
                    if len(bucket) < BULK_SIZE:
                        bucket.append(data)
                    else:
                        tot_elm_done = put_bucket(dest, bucket, tot_elm_done)
                        bucket = []
            else:
                tot_elm_done += 1
                print("[COMMAND][PUT][SKIP][index {}]".format(idx), end="\r")

        if len(bucket) != 0:
            tot_elm_done = put_bucket(dest, bucket, tot_elm_done)

        print("[COMMAND][PUT][DONE in {:0>8}s][{} items]".format(
            str(timedelta(seconds=time()-command_start)),
            tot_elm_done
        ))


if __name__ == "__main__":
    exit(cli_main())
