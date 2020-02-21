import os

import pandas as pd
from tqdm import tqdm

import json

class ActivityPostProcessor:
    
    def __init__(self):

        self._target_columns = ['activitePrincipaleUniteLegale']

        self.category_labels_path = '../data/naf_2008.json'
        self.categories_labels = self.get_categories_labels(self.category_labels_path)
        
    def run(self):
        
        if not os.path.exists('../data/clean_{}'.format(self.folders_path)):
            os.mkdir('../data/clean_{}'.format(self.folders_path))
            
        self.load_data()
        self.df = self.group_by_postal_codes(self.df)
        self.df = self.set_category_labels(self.df)
        self.save_dataframe(self.df)
        
    def load_data(self, df):
        self.df = df
        self.df = pd.concat(df_list).reset_index()
        self.df.latitude = df.latitude.apply(format_lat_long)
        self.df.longitude = df.longitude.apply(format_lat_long)
        
    def get_data_path_list(self, data_path):
        # Nested list comprehensions to extract { dpt_code STR : csv file path STR }
        return dict([
            (x, os.path.join(os.path.join(data_path, x), [
                x for x in os.listdir(os.path.join(data_path, x)) if "csv" in x
            ][0]))
            for x in os.listdir(data_path)
        ])

    def clean_dataset(self, df):
        # Remove duplicates for df1_activitePrincipaleUniteLegale
        df = df.groupby('df1_activitePrincipaleUniteLegale').apply(
            lambda x : x.drop_duplicates(subset = 'df1_activitePrincipaleUniteLegale')
        )[[
            'df1_activitePrincipaleUniteLegale', 
            'df1_count', 
            'latitude', 
            'longitude'
        ]].set_index(
            'df1_activitePrincipaleUniteLegale', 
            drop=True
        )
        return df

    def format_lat_long(self, x):
        try:
            return float(str(x).replace(',', '.'))
        except:
            pass

    def get_categories_labels(self, filepath):
        with open(filepath) as f:
            return json.load(f)

    def filter_dataframe(self, df):
        df = df.sort_values(by='df1_count', ascending=False).iloc[:10]
        if df.shape[0] == 0: # If len = 1, add row for display in panel
            df = df.append(df.iloc[0])
        return df
    
    def group_by_postal_codes(self, df):
        # Group by postal codes and filter 10 top categories
        df = df.groupby(
            'codes_postaux', 
            group_keys=False
        ).apply(
            self.filter_dataframe
        )
        return df
    
    def set_category_labels(self, df):
        df = df.loc[df.df1_activitePrincipaleUniteLegale.apply(
            lambda x: x in self.categories_labels.keys()
        )]

        # Replace category code by label
        df.df1_activitePrincipaleUniteLegale = df.df1_activitePrincipaleUniteLegale.apply(
            lambda x: self.categories_labels[x]
        )
        return df
        
    def save_dataframe(self, df, folders_path):
        df.to_csv('../data/clean_{}/dataset.csv'.format(folders_path))
        