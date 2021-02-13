from typing import List

import typer

from .function import service
from .utils import MinCacheServer

app = typer.Typer(name="probe.results.dashboard", add_completion=False)


@app.command()
def dashboard(folders: "List[str]", dash_ip: str = "localhost", lazy: bool = False):
    MinCacheServer().start()
    service(folders, dash_ip, lazy)


if __name__ == "__main__":
    app(prog_name="probe.results.dashboard")
