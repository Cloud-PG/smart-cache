from plotly.graph_objs import Layout
from colorama import Fore, Style

DASH_CACHE_DIRS = ["columns", "measures", "agents", "tables", "compare", "results"]

PLOT_LAYOUT = Layout(
    paper_bgcolor="rgb(255,255,255)",
    plot_bgcolor="rgb(255,255,255)",
    yaxis={"gridcolor": "black"},
    xaxis={"gridcolor": "black"},
)

STATUS_ARROW = f"{Style.BRIGHT + Fore.MAGENTA}==> {Style.RESET_ALL}"
