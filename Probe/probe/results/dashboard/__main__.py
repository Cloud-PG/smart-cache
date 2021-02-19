from typing import List

import typer

from .function import service

app = typer.Typer(name="probe.results.dashboard", add_completion=False)


@app.command()
def dashboard(
    folders: "List[str]",
    dash_ip: str = "localhost",
    dash_port: int = 8050,
    lazy: bool = False,
    debug: bool = False,
):
    service(folders, dash_ip, dash_port, lazy, debug)


if __name__ == "__main__":
    app(prog_name="probe.results.dashboard")
