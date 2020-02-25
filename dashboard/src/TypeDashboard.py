import os 
import json
import pandas as pd

import altair as alt
import panel as pn

from jinja2 import Environment, FileSystemLoader


class TypeDashboard:

    def __init__(self, **kwargs):

        self.data_path = kwargs['data_path']
        self.code_categoriejuridique_path = kwargs["type_code_labels_path"]
    
    def run(self):
        
        self.load_data()
        # self.check_dataframe_shape()

        return self.generate_dashboard()

    def load_data(self):

        self.df = pd.read_csv(self.data_path, index_col=[0]).dropna()
        self.get_categories_labels()
        self.df = self.get_top_categories(self.df, n = 10)
        self.df.columns = ["Catégorie Juridique", 'Total', 'Departement']

    def get_categories_labels(self):
        with open(self.code_categoriejuridique_path) as f:
            self.categories_labels = json.load(f)

    def get_top_categories(self, df, n = 10):
        df.categorieJuridiqueUniteLegale = df.categorieJuridiqueUniteLegale.astype(int).astype(str)
        # Filter not registered values from INSEE label file
        df = df.loc[df.categorieJuridiqueUniteLegale.apply(
            lambda x: x in self.categories_labels.keys()
        )]
        # Replace category code by label
        df.categorieJuridiqueUniteLegale = df.categorieJuridiqueUniteLegale.apply(
            lambda x: self.categories_labels[x]
        )
        # Keeping top 10 values
        df = df.groupby('dpt').apply(
            lambda x: x.sort_values(by="count", ascending=False).iloc[:n]
        )
        df.index = df.index.get_level_values(0)
        return df

    def generate_dashboard(self):
        tickers = sorted(self.df.Departement.unique())
        ticker = pn.widgets.Select(name="Departement", options=tickers)

        @pn.depends(ticker.param.value)
        def get_plot(ticker):
            chart = alt.Chart(self.df).mark_bar().encode(alt.X('Catégorie Juridique:N', sort='-y'),
                                                    y="Total:Q",
                                                    tooltip=alt.Tooltip([
                                                        "Catégorie Juridique:N",
                                                        "Total:Q"
                                                    ])).transform_filter(
                (alt.datum.Departement == ticker)
            ).properties(
                width=800,
                height=100
            )
            return chart

        title = "### Catégories juridiques selon le département"
        dashboard = pn.Row(
            pn.Column(title, ticker), 
            pn.Column(get_plot, margin=(0, 260, 0, 0)),
            width = 1000,
            height = 400
        )
        return dashboard