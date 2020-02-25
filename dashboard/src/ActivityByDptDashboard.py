import os 
import json
import pandas as pd

import altair as alt
import panel as pn

from jinja2 import Environment, FileSystemLoader


class ActivityByDptDashboard:

    def __init__(self, **kwargs):

        self.data_path = kwargs['data_path']
        self.activity_code_path = kwargs["activity_code_labels_path"]
    
    def run(self):
        
        self.load_data()

        return self.generate_dashboard()

    def load_data(self):
        self.df = pd.read_csv(self.data_path, index_col=[0])
        self.get_categories_labels()
        self.df = self.get_top_categories(self.df, n = 10)
        self.df.columns = ['Activité', 'Total', 'Departement']

    def get_categories_labels(self):
        with open(self.activity_code_path) as f:
            self.categories_labels = json.load(f)

    def get_top_categories(self, df, n = 10):
        df.activitePrincipaleUniteLegale = df.activitePrincipaleUniteLegale.astype(str)
        # Filter not registered values from INSEE label file
        df = df.loc[df.activitePrincipaleUniteLegale.apply(
            lambda x: x in self.categories_labels.keys()
        )]
        # Replace category code by label
        df.activitePrincipaleUniteLegale = df.activitePrincipaleUniteLegale.apply(
            lambda x: self.categories_labels[x]
        )
        # Keeping top 10 values
        df = df.groupby('dpt').apply(
            lambda x: x.sort_values(by="count", ascending=False).iloc[:n]
        )
        df.index = df.index.get_level_values(0)
        return df

    def generate_dashboard(self):
        tickers = sorted(self.df.Departement.astype(str).unique())
        ticker = pn.widgets.Select(name="Departement", options=tickers)

        @pn.depends(ticker.param.value)
        def get_plot(ticker):
            chart = alt.Chart(self.df).mark_bar().encode(alt.X('Activité:N', sort='-y'),
                                                    y="Total:Q",
                                                    tooltip=alt.Tooltip([
                                                        "Activité:N",
                                                        "Total:Q"
                                                    ])).transform_filter(
                (alt.datum.Departement == ticker) # this ties in the filter 
            ).properties(
                width=800,
                height=100
            )
            return chart

        title = "### Secteurs d'activité selon le département"
        dashboard = pn.Row(
            pn.Column(title, ticker), 
            pn.Column(get_plot, margin=(0, 260, 0, 0)),
            width = 1000,
            height = 400
        )
        return dashboard