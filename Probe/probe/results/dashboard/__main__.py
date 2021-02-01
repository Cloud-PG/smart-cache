from typing import List

import typer
from .function import service

app = typer.Typer(name="probe.results.dashboard", add_completion=False)


@app.command()
def dashboard(folders: "List[str]", dash_ip: str = "localhost"):
    service(folders, dash_ip)


if __name__ == "__main__":
    app(prog_name="probe.results.dashboard")
