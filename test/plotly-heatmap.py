#! /usr/bin/env python

from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go

import pandas as pd
import numpy as np

# Load your Jaccard similarity matrix
# Assume it's a square CSV with headers and index
df = pd.read_csv("podar_matrix.tsv", sep='\t', index_col=0)

# Setup the Dash app
app = Dash(__name__)

app.layout = html.Div([
    html.H4("Jaccard Similarity Heatmap (Sourmash Sketches)"),
    dcc.Graph(id="jaccard-heatmap"),

    html.P("Jaccard similarity range to show:"),
    dcc.RangeSlider(
        id='threshold-range',
        min=0.0,
        max=1.0,
        step=0.01,
        value=[0.0, 1.0],  # Default range: full
        marks={i/10: f"{i/10:.1f}" for i in range(0, 11)},
        tooltip={"placement": "bottom", "always_visible": True}
    )
])


@app.callback(
    Output("jaccard-heatmap", "figure"),
    Input("threshold-range", "value"))
def update_heatmap(threshold_range):
    min_val, max_val = threshold_range
    filtered_df = df.where((df >= min_val) & (df <= max_val))

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
        hoverinfo='text'
    ))

    fig.update_layout(
        title=f"Jaccard Similarity (Range: {min_val:.2f} – {max_val:.2f})",
        xaxis_title="Sketch",
        yaxis_title="Sketch",
        xaxis={'side': 'top'},
        autosize=True
    )

    fig.update_xaxes(showticklabels=False)
    fig.update_yaxes(showticklabels=False)

    fig.write_html("jaccard_heatmap.html", full_html=True)

    return fig

if __name__ == "__main__":
    app.run(debug=True)

