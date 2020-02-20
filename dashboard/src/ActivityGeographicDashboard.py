import os 
import json
import pandas as pd

import altair as alt
import panel as pn
from vega_datasets import data

from jinja2 import Environment, FileSystemLoader

from pyspark import SparkContext
from pyspark.sql import SparkSession

class ActivityGeographicDashboard:

    def __init__(self, **kwargs):

        self.data_path = kwargs['data_path']
        self.activity_code_path = kwargs["activity_code_labels_path"]
        self.postal_codes_lat_long_labels_path = kwargs["postal_codes_lat_long_labels_path"]

    def run(self):
        
        self.load_data()
        self.check_dataframe_shape(multiplier=100)

        return self.generate_dashboard()
    
    def load_data(self):
        self.df = pd.read_csv(self.data_path, index_col=[0])
        self.df.columns = ["codes_postaux", 'Activité', 'Total', 'latitude', 'longitude']
        counts = self.df['Activité'].value_counts()
        counts = counts.loc[counts > counts.quantile(0.2)] # Filter low counts categories
        self.df = self.df[self.df['Activité'].isin(list(counts.index))]

    def check_dataframe_shape(self, multiplier = 10):
        # Check dataframe shape before disabling MaxRowsError
        #assert self.df.shape[0] < 5000 * multiplier # 5000 is the default threshold for Altair
        # Disable MaxRowsError.
        alt.data_transformers.disable_max_rows()

    def generate_dashboard(self):
        tickers = sorted(list(self.df['Activité'].astype(str).unique()))
        ticker = pn.widgets.Select(name="Activité", options=tickers)

        @pn.depends(ticker.param.value)
        def get_plot(ticker):

            map_geojson = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/regions.geojson"
            map_geojson = alt.Data(url=map_geojson, format=alt.DataFormat(property='features',type='json'))

            basemap = alt.Chart(
                map_geojson
            ).mark_geoshape(
                fill = 'lightgray',
                stroke = 'white',
            ).properties(
                projection={'type': 'mercator'}
            )

            points = alt.Chart(self.df).mark_circle().encode( 
                longitude = 'longitude:Q',
                latitude='latitude:Q',
                size = alt.Size(
                    'Total:Q', 
                    scale = alt.Scale(
                        domain = [0.125, 50], 
                        range = [1, 400], 
                        clamp = True, 
                        zero = False,
                        ),
                    legend=None
                    ),
                color = alt.Color('Total:Q',
                    scale=alt.Scale(
                        scheme='reds',
                        domain = [
                            0,
                            int(self.df.Total.quantile(0.8))
                        ]
                    )
                ),
            ).transform_filter(
                    (alt.datum['Activité'] == ticker)
            )

            chart = alt.layer(
                basemap, 
                points, 
                data=self.df,
                width = 500,
                height = 500
            ).facet(
                column='Activité:N'
            ).transform_filter(
                    (alt.datum['Activité'] == ticker)
            )

            return chart

        title = "### Répartition des entreprises en fonction du secteur d'activité"
        dashboard = pn.Row(
            pn.Column(title, ticker, height = 50), 
            pn.Column(get_plot, margin=(0, 0, 0, 150)),
            width = 1000,
            height = 600
        )
        return dashboard
    