#! /usr/bin/env python

from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import numpy as np

# Load your Jaccard similarity matrix
# Assume it's a square CSV with headers and index
df = pd.read_csv("podar_matrix.csv", index_col=0)

# Setup the Dash app
app = Dash(__name__)

app.layout = html.Div([
    html.H4("Jaccard Similarity Heatmap (Sourmash Sketches)"),
    dcc.Graph(id="jaccard-heatmap"),

    html.P("Minimum Jaccard similarity to show:"),
    dcc.Slider(
        id='threshold',
        min=0.0,
        max=1.0,
        step=0.01,
        value=0.0,
        marks={i/10: f"{i/10:.1f}" for i in range(0, 11)},
        tooltip={"placement": "bottom", "always_visible": True}
    )
])


@app.callback(
    Output("jaccard-heatmap", "figure"),
    Input("threshold", "value"))
def update_heatmap(threshold):
    # Apply threshold filter — mask values below threshold
    masked_df = df.where(df >= threshold)

    fig = px.imshow(
        masked_df,
        text_auto=True,
        color_continuous_scale="Viridis",
        aspect="auto",
        labels=dict(color="Jaccard"),
        title=f"Jaccard Similarity (≥ {threshold:.2f})"
    )
    fig.update_xaxes(side="top")  # Show labels on top
    return fig


if __name__ == "__main__":
    app.run(debug=True)

