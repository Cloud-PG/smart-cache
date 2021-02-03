from typing import Any

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html

from ..data import Results


def compare() -> "Any":
    return dbc.Card(
        [
            dbc.Spinner(
                dbc.CardBody(
                    id="compare",
                ),
                color="primary",
                type="grow",
            ),
            dbc.CardBody(
                [
                    dbc.Input(
                        id="sel-num-sim",
                        placeholder="Number of simulation",
                        type="number",
                    ),
                    dbc.Input(
                        id="sel-tick", placeholder="tick of simulation", type="number"
                    ),
                    dbc.Spinner(
                        [
                            dbc.CardBody(
                                id="sel-sim-compare-plot-actions",
                            ),
                            dbc.CardBody(
                                id="sel-sim-compare-plot-actions-hist-numReq",
                            ),
                            dbc.CardBody(
                                id="sel-sim-compare-plot-actions-hist-size",
                            ),
                            dbc.CardBody(
                                id="sel-sim-compare-plot-actions-hist-deltaT",
                            ),
                            dbc.CardBody(
                                id="sel-sim-compare-plot-after",
                            ),
                        ],
                        color="warning",
                        type="grow",
                    ),
                ],
                id="compare-row",
            ),
            dbc.CardBody(
                id="compare-tables",
            ),
        ]
    )


def table() -> "Any":
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    dbc.ListGroup(
                        [
                            dbc.ListGroupItem(
                                dbc.Button(
                                    "",
                                    color="info",
                                    disabled=True,
                                    id="toggle-extended-table-output",
                                ),
                            ),
                            dbc.ListGroupItem(
                                daq.ToggleSwitch(
                                    id="toggle-extended-table",
                                    value=False,
                                )
                            ),
                            dbc.ListGroupItem(
                                dbc.Button(
                                    "",
                                    color="info",
                                    disabled=True,
                                    id="toggle-sort-by-roh-first-output",
                                ),
                            ),
                            dbc.ListGroupItem(
                                daq.ToggleSwitch(
                                    id="toggle-sort-by-roh-first",
                                    value=False,
                                )
                            ),
                            dbc.ListGroupItem(
                                dbc.Button(
                                    "",
                                    color="info",
                                    disabled=True,
                                    id="toggle-new-metrics-output",
                                ),
                            ),
                            dbc.ListGroupItem(
                                daq.ToggleSwitch(
                                    id="toggle-new-metrics",
                                    value=True,
                                )
                            ),
                        ],
                        horizontal=True,
                    ),
                    dbc.CardBody(
                        dbc.ListGroup(
                            [
                                dbc.ListGroupItem(
                                    dcc.Link(
                                        "Download as CSV",
                                        refresh=True,
                                        href="/table/csv",
                                        target="_blank",
                                    )
                                ),
                                dbc.ListGroupItem(
                                    dcc.Link(
                                        "Download as Latex table",
                                        refresh=True,
                                        href="/table/tex",
                                        target="_blank",
                                    )
                                ),
                                dbc.ListGroupItem(
                                    dcc.Link(
                                        "Download as html",
                                        refresh=True,
                                        href="/table/html",
                                        target="_blank",
                                    )
                                ),
                            ],
                            horizontal=True,
                        ),
                    ),
                ],
            ),
            dbc.Spinner(
                [
                    dbc.CardBody(
                        id="table",
                    ),
                ],
                color="primary",
            ),
        ],
    )


def columns() -> "Any":
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.Span("group size = 1", id="columns-group-size-text"),
                    dcc.Slider(
                        id="columns-group-size",
                        min=1,
                        max=30,
                        step=1,
                        value=1,
                        marks={
                            1: "1",
                            7: "7",
                            14: "14",
                            21: "21",
                            28: "28",
                            30: "30",
                        },
                    ),
                ]
            ),
            dbc.Spinner(
                dbc.CardBody(
                    id="graphs-columns",
                ),
                color="primary",
            ),
        ]
    )


def measures() -> "Any":
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.Span("group size = 1", id="measures-group-size-text"),
                    dcc.Slider(
                        id="measures-group-size",
                        min=1,
                        max=30,
                        step=1,
                        value=1,
                        marks={
                            1: "1",
                            7: "7",
                            14: "14",
                            21: "21",
                            28: "28",
                            30: "30",
                        },
                    ),
                ]
            ),
            dbc.Spinner(
                dbc.CardBody(
                    id="graphs-measures",
                ),
                color="primary",
            ),
        ]
    )


def agents() -> "Any":
    return dbc.Card(
        dbc.Spinner(
            dbc.CardBody(
                id="graphs-agents",
            ),
            color="primary",
        ),
    )


def filters(results: "Results") -> "Any":
    return dbc.Card(
        dbc.CardBody(
            [
                dcc.Input(
                    id="num-of-results",
                    type="number",
                    placeholder="Max number of results",
                    value=0,
                ),
                html.Hr(),
                html.H2("All"),
                dcc.Checklist(
                    options=[
                        {"label": f" {component}", "value": component}
                        for component in results.components
                    ],
                    value=[],
                    labelStyle={"display": "block"},
                    id="selected-filters-all",
                ),
                html.Br(),
                html.H2("Any"),
                dcc.Checklist(
                    options=[
                        {"label": f" {component}", "value": component}
                        for component in results.components
                    ],
                    value=[],
                    labelStyle={"display": "block"},
                    id="selected-filters-any",
                ),
            ]
        ),
    )


def files(results: "Results") -> "Any":
    return dbc.Card(
        dbc.CardBody(
            [
                dbc.Button(
                    "Unselect all",
                    color="warning",
                    block=True,
                    id="unselect-files",
                ),
                dbc.Button(
                    "Select all",
                    color="success",
                    block=True,
                    id="select-files",
                ),
                html.Hr(),
                dcc.Checklist(
                    options=[
                        {"label": f" {filename}", "value": filename}
                        for filename in results.files
                    ],
                    value=results.files,
                    labelStyle={"display": "block"},
                    id="selected-files",
                ),
            ]
        ),
    )
