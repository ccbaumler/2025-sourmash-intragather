#! /usr/bin/env python

from dash import Dash, dcc, html, Input, Output, ctx
import plotly.express as px
import plotly.graph_objects as go

import pandas as pd
import numpy as np

df = pd.read_csv("podar.x.gtdb-k31.csv", sep=',', index_col=0)[:1000]

app = Dash(__name__)

app.layout = html.Div([
    html.H4("Jaccard Similarity Heatmap (Sourmash Sketches)"),
    dcc.Graph(id="jaccard-heatmap"),

    html.Div([
        html.P("Jaccard similarity range to show:"),
        dcc.RangeSlider(
            id='threshold-range-slider',
            min=0.0,
            max=1.0,
            step=0.0001,
            value=[0.0, 1.0],
            marks={i/10: f"{i/10:.1f}" for i in range(0, 11)},
            tooltip={"placement": "bottom", "always_visible": True}
        ),
        html.Div([
            html.Label("Min:"),
            dcc.Input(id='min-input', type='number', min=0.0, max=1.0, step=0.0001, value=0.0),
            html.Label("Max:"),
            dcc.Input(id='max-input', type='number', min=0.0, max=1.0, step=0.0001, value=1.0),
        ], style={'marginTop': '10px'})
    ])
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

    if trigger_id == 'threshold-range-slider':
        # Slider changed → update input boxes
        return slider_range, slider_range[0], slider_range[1]
    elif trigger_id in ('min-input', 'max-input'):
        # Inputs changed → update slider
        min_val = max(0.0, min(min_val, 1.0))
        max_val = max(0.0, min(max_val, 1.0))
        if min_val > max_val:
            min_val, max_val = max_val, min_val
        return [min_val, max_val], min_val, max_val
    else:
        # Initial load
        return slider_range, min_val, max_val

@app.callback(
    Output("jaccard-heatmap", "figure"),
    Input("threshold-range-slider", "value"))
def update_heatmap(threshold_range):
    min_val, max_val = threshold_range

    # Mask values outside the range
    filtered_df = df.where((df >= min_val) & (df <= max_val))

    # Drop rows and columns that are fully NaN
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

    # Generate hover text
    hover_text = [
        [f"Row: {row}<br>Col: {col}<br>Jaccard: {filtered_df.loc[row, col]:.4f}"
         if not pd.isna(filtered_df.loc[row, col]) else ""
         for col in filtered_df.columns]
        for row in filtered_df.index
    ]

    fig = go.Figure(data=go.Heatmap(
        z=filtered_df.values,
        x=filtered_df.columns,
        y=filtered_df.index,
        colorscale='Viridis',
        colorbar=dict(title="Jaccard"),
        text=hover_text,
        hoverinfo='text',
        zmin=min_val,
        zmax=max_val
    ))

    fig.update_layout(
        title=f"Jaccard Similarity (Range: {min_val:.2f} – {max_val:.2f})",
        xaxis_title="Sketch",
        yaxis_title="Sketch",
        xaxis={'side': 'top'},
        autosize=True,
        margin=dict(l=40, r=40, t=60, b=40)
    )

    # Optional: Hide axis tick labels for large datasets
    fig.update_xaxes(showticklabels=False)
    fig.update_yaxes(showticklabels=False)

    return fig

if __name__ == "__main__":
    app.run(debug=True)
