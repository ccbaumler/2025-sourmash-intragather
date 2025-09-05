#! /usr/bin/env python

from dash import Dash, dcc, html, Input, Output, ctx, State, callback
import plotly.express as px
import plotly.graph_objects as go

import argparse
import pandas as pd
import numpy as np
from math import ceil
from dash import no_update
import io
import csv


p = argparse.ArgumentParser()
p.add_argument("input", type=str, help="Path to CSV file")
p.add_argument("--max", type=int, help='The intersect max to display')
p.add_argument("--min", type=int, help='The intersect min to display')
args = p.parse_args()

df = pd.read_csv(args.input, sep=',', index_col=0)

data_min = int(df.min().min())
data_max = int(df.max().max())

initial_min = args.min if args.min is not None else data_min
initial_max = args.max if args.max is not None else data_max

range_span = data_max - data_min

if range_span > 1000:
    step = max(1, range_span // 1000)
else:
    step = 1

# Build marks for slider; limit number of marks to prevent overcrowding
num_marks = 10
mark_step = max(1, range_span // num_marks)

marks = {
    i: f"{i:,}"  # Adds commas for thousands (e.g. 1,000)
    for i in range(data_min, data_max + 1, mark_step)
}

slider = dcc.RangeSlider(
    id='threshold-range-slider',
    min=data_min,
    max=data_max,
    step=step,
    value=[initial_min, initial_max],
    marks=marks,
    tooltip={"placement": "bottom", "always_visible": True}
)

app = Dash(__name__)

app.layout = html.Div([
    html.H4("Intersect Heatmap (Sourmash Sketches)"),
    dcc.Graph(id="intersect-heatmap"),
    html.Div([
        html.P("Intersection size range:"),
        slider,
        html.Div([
            html.Label("Min:"),
            dcc.Input(id='min-input', type='number', value=initial_min),
            html.Label("Max:"),
            dcc.Input(id='max-input', type='number', value=initial_max),
        ])
    ]),
#    html.H5("Selected Intersection Cells:"),
#    html.Div(id="selected-labels", style={"whiteSpace": "pre-wrap", "fontFamily": "monospace"}),
    html.Div([
        html.H5("Selected Cells:", style={"marginRight": "20px"}),
        html.Button("Download Selected as CSV", id="download-btn", n_clicks=0),
    ], style={
        "display": "flex",
        "alignItems": "center",
        "marginTop": "20px",
        "marginBottom": "10px",
    }),
    html.Div(id="selected-labels", style={
        "whiteSpace": "pre-wrap",
        "fontFamily": "monospace",
        "maxHeight": "300px",
        "overflowY": "auto",
        "border": "1px solid #ccc",
        "padding": "10px"
    }),
    dcc.Store(id="selected-data-store"),

    # Also keep your download component
    dcc.Download(id="download-selected")
])


@app.callback(
    Output('threshold-range-slider', 'value'),
    Output('min-input', 'value'),
    Output('max-input', 'value'),
    Input('threshold-range-slider', 'value'),
    Input('min-input', 'value'),
    Input('max-input', 'value')
)
def sync_threshold_inputs(slider_range, min_val, max_val):
    trigger_id = ctx.triggered_id

#    data_min = int(df.min().min())
#    data_max = int(df.max().max())

    if trigger_id == 'threshold-range-slider':
        return slider_range, slider_range[0], slider_range[1]

    elif trigger_id in ('min-input', 'max-input'):
        min_val = data_min if min_val is None else max(data_min, min(min_val, data_max))
        max_val = data_max if max_val is None else max(data_min, min(max_val, data_max))

        if min_val > max_val:
            min_val, max_val = max_val, min_val

        return [min_val, max_val], min_val, max_val

    else:
        return slider_range, min_val, max_val

@app.callback(
    Output("intersect-heatmap", "figure"),
    Input("threshold-range-slider", "value"))
def update_heatmap(threshold_range):
    min_val, max_val = threshold_range

    filtered_df = df.where((df >= min_val) & (df <= max_val))

    filtered_df = filtered_df.dropna(axis=0, how='all')  # drop rows
    filtered_df = filtered_df.dropna(axis=1, how='all')  # drop cols

    if filtered_df.empty:
        fig = go.Figure()
        fig.update_layout(
            title="No data in selected similarity range.",
            xaxis={'visible': False},
            yaxis={'visible': False}
        )
        return fig

    hover_text = [
        [f"Row: {row}<br>Col: {col}<br>Intersect_BP: {filtered_df.loc[row, col]:.4f}"
         if not pd.isna(filtered_df.loc[row, col]) else ""
         for col in filtered_df.columns]
        for row in filtered_df.index
    ]

    fig = go.Figure(data=go.Heatmap(
        z=filtered_df.values,
        x=filtered_df.columns,
        y=filtered_df.index,
        colorscale='Viridis',
        colorbar=dict(title="Intersect_BP"),
        text=hover_text,
        hoverinfo='text',
        zmin=min_val,
        zmax=max_val
    ))

    fig.update_layout(
        title=f"Intersect (Range: {min_val:.2f} – {max_val:.2f})",
        xaxis_title="Sketch",
        yaxis_title="Sketch",
        xaxis={'side': 'top'},
        autosize=True,
        #margin=dict(l=40, r=40, t=60, b=40),
        hovermode='closest',
        dragmode='select',
    )

    fig.update_xaxes(showticklabels=False)
    fig.update_yaxes(showticklabels=False)

    return fig


@app.callback(
    Output("selected-labels", "children"),
    Output("selected-data-store", "data"),
    Input("intersect-heatmap", "selectedData"),
    Input("threshold-range-slider", "value"))
def display_selected_data(selectedData, threshold_range):
    print(selectedData)
    if not selectedData or "range" not in selectedData:
        return "No selection made.", []

    min_val, max_val = threshold_range

    filtered_df = df.where((df >= min_val) & (df <= max_val))

    filtered_df = filtered_df.dropna(axis=0, how='all')  # drop rows
    filtered_df = filtered_df.dropna(axis=1, how='all')  # drop cols

    x_range = selectedData['range']['x']
    y_range = selectedData['range']['y']

    x_vals = list(filtered_df.columns)
    y_vals = list(filtered_df.index)

    selected_x_indices = [i for i, _ in enumerate(x_vals) if x_range[0] <= i <= x_range[1]]
    selected_y_indices = [i for i, _ in enumerate(y_vals) if y_range[0] <= i <= y_range[1]]

    selected = []
    for yi in selected_y_indices:
        for xi in selected_x_indices:
            row = y_vals[yi]
            col = x_vals[xi]
            val = filtered_df.loc[row, col]
            if pd.notna(val):
                selected.append({"row": row, "col": col, "value": val})

    if not selected:
        return "No points in selection.", []

    selected_labels = [
        f"Row: {d['row']}, Col: {d['col']}, Value: {d['value']}" for d in selected
    ]

    return html.Pre("\n".join(selected_labels)), selected

@app.callback(
    Output("download-selected", "data"),
    Input("download-btn", "n_clicks"),
    State("selected-data-store", "data"),
    prevent_initial_call=True
)
def download_selected(n_clicks, data):
    if not data:
        return no_update

    df = pd.DataFrame(data)
    return dcc.send_data_frame(df.to_csv, "selected_cells.csv", index=False)


if __name__ == "__main__":
    app.run(debug=True)
