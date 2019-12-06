import subprocess
from contextlib import contextmanager
from os import path, walk

import pandas as pd


@contextmanager
def ignored(*exceptions):
    try:
        yield
    except exceptions as exc:
        print(exc)
        pass


def str2bool(v):
    return v.lower() in ("yes", "true", "True", "t", "1")


def wait_jobs(processes):
    while job_run(processes):
        for _, process in processes:
            try:
                process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass


def read_output_last_line(output):
    buffer = ""
    cur_char = output.read(1).decode("ascii")
    while cur_char not in ["\r", "\n", '']:
        buffer += cur_char
        cur_char = output.read(1).decode("ascii")
    return buffer


def job_run(processes: list) -> bool:
    running_processes = []
    for task_name, process in processes:
        running = process.returncode is None
        running_processes.append(running)
        if running:
            print(
                f"[{process.pid}][RUNNING][{task_name}]{read_output_last_line(process.stdout)}\x1b[0K", flush=True)
        else:
            print(
                f"[{process.pid}][DONE][{task_name}][Return code -> {process.returncode}]\x1b[0K", flush=True)
            if process.returncode != 0:
                print(
                    f"[{process.pid}][DONE][{task_name}][Return code -> {process.returncode}]", flush=True)
                print(f"{process.stdout.read().decode('ascii')}", flush=True)
                print(f"{process.stderr.read().decode('ascii')}", flush=True)
                exit(process.returncode)

    print(f"\x1b[{len(processes)+1}F")

    return any(running_processes)


def get_result_section(cur_path: str, source_folder: str):
    head, tail = path.split(cur_path)
    section = []
    while head != source_folder:
        section.append(tail)
        head, tail = path.split(head)
    section.append(tail)
    return section


def load_results(folder: str) -> dict:
    results = {}
    for root, _, files in walk(folder):
        for file_ in files:
            _, ext = path.splitext(file_)
            if ext == ".csv":
                section = get_result_section(root, folder)
                cur_section = results
                while len(section) > 1:
                    part = section.pop()
                    if part not in cur_section:
                        cur_section[part] = {}
                    cur_section = cur_section[part]
                last_section = section.pop()
                file_path = path.join(root, file_)
                df = pd.read_csv(
                    file_path
                )
                cur_section[last_section] = df

    return results
