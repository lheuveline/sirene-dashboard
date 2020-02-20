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
        self.check_dataframe_shape()

        return self.generate_dashboard()

    def load_data(self):
        self.get_data_path_list()
        self.get_categories_labels()
        self.merge_datasets()

    def get_data_path_list(self):
        # Nested list comprehensions to extract { dpt_code STR : csv file path STR }
        self.file_path_list = dict([
            (x, os.path.join(os.path.join(self.data_path, x), [
                x for x in os.listdir(os.path.join(self.data_path, x)) if "csv" in x
            ][0]))
            for x in os.listdir(self.data_path)
        ])

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
        return df.sort_values(by="count", ascending=False).iloc[:n]

    def merge_datasets(self):
        annotated_data = []
        for dpt, file_path in self.file_path_list.items():
            df = pd.read_csv(file_path).dropna()
            annotated_rows = list(map(lambda x: [dpt] + x.tolist(), df.values))
            # Filtering top 10 categories
            df = self.get_top_categories(
                pd.DataFrame(annotated_rows, 
                            columns = [
                                'dpt', 'categorieJuridiqueUniteLegale', 'count']
                            )
            ).sort_values(by="count", ascending=False)
            annotated_data.append(df)

        self.df = pd.concat(annotated_data)
        # Rename columns for display
        self.df.columns = ['Departement', 'Catégorie Juridique', 'Total']

    def check_dataframe_shape(self):
        # Check dataframe shape before disabling MaxRowsError
        multiplier = 10
        assert self.df.shape[0] < 5000 * multiplier # 5000 is the default threshold for Altair
        # Disable MaxRowsError.
        alt.data_transformers.disable_max_rows()

    def generate_dashboard(self):
        tickers = sorted(list(self.file_path_list.keys()))
        ticker = pn.widgets.Select(name="Departement", options=tickers)

        @pn.depends(ticker.param.value)
        def get_plot(ticker):
            chart = alt.Chart(self.df).mark_bar().encode(alt.X('Catégorie Juridique:N', sort='-y'),
                                                    y="Total:Q",
                                                    tooltip=alt.Tooltip([
                                                        "Catégorie Juridique",
                                                        "Total"
                                                    ])).transform_filter(
                (alt.datum.Departement == ticker) # this ties in the filter 
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