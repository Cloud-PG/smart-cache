from plotly.graph_objs import Layout
from colorama import Fore, Style

PLOT_LAYOUT = Layout(
    paper_bgcolor="rgb(255,255,255)",
    plot_bgcolor="rgb(255,255,255)",
    yaxis={"gridcolor": "black"},
    xaxis={"gridcolor": "black"},
)

STATUS_ARROW = f"{Style.BRIGHT + Fore.MAGENTA}==> {Style.RESET_ALL}"

LOG_FORMAT = "<green>{time}</green>\t| <level>{level}</level> | <magenta>{file}</magenta>:<yellow>{function}</yellow>:<yellow>{line}</yellow> -> {message}"

SORTING_COLUMNS = [
    "Score",
    "Throughput",
    "Cost",
    "Score (TB)",
    "Throughput (TB)",
    "Cost (TB)",
    "Read on hit ratio",
    "Read on hit (TB)",
    "Bandwidth",
    "Bandwidth (TB)",
    "Redirect Vol.",
    "Avg. Free Space",
    "Std. Dev. Free Space",
    "Hit over Miss",
    "Num. miss after del.",
    "Hit rate",
    "CPU Eff.",
]
