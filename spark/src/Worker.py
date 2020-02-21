from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import pandas as pd

import time
from tqdm import tqdm
import os

from SireneParser import SireneParser

class Worker:

    def __init__(self):

        # Spark env
        self.sc = SparkContext()
        self.sc.setLogLevel("ERROR")
        self.spark = SparkSession.builder.appName('stock-etablissement').getOrCreate()
        print('\n\n\n')
        print('[WORKER] : Init finished.')

        self.parser = SireneParser(self.spark)

        # Input files
        self.sirene_unite_legale = "StockUniteLegale_utf8.csv"
        self.sirene_etab = "StockEtablissement_utf8.csv"
        self.postal_codes_coordinates = "EUCircos_Regions_departements_circonscriptions_communes_gps.csv"
        
        # Output files
        self.base_merge_out_file = "unite_etab_joined"
        self.postal_codes_out_file = "activity_category_postal_codes"
        self.panel_data_path = "../../dashboard/data"

        # Targeted columns for aggregations
        self.target_columns = {
            'categorieJuridiqueUniteLegale' : "count",
            'activitePrincipaleUniteLegale' : "count"
        }

    def merge_on_siren(self, save = True, coalesce = 1):
        print("[WORKER] : Merging datasets on SIREN column")
        self.parser.load_file(self.sirene_unite_legale)
        self.parser.load_file(self.sirene_etab)
        self.merged = self.parser.merge(
            df1 = self.parser.loaded_files[self.sirene_etab],
            df2 = self.parser.loaded_files[self.sirene_unite_legale],
            on = "siren",
            prefix = "etab_" 
        )
        self.merged = self.parser.format_dpt_code(
            self.parser.loaded_files[self.sirene_etab], 
            "etab_codeCommuneEtablissement"
        )
        if save:
            self.parser.save_dataframe(self.merged, self.base_merge_out_file)

    def merge_on_postal_codes(self, save = True, clear = True, coalesce = 1):

        # Depends on merge_on_siren output dataframe

        print("[WORKER] : Merging datasets on POSTAL CODES column")
        self.parser.load_file(self.postal_codes_coordinates, sep=';')
        # Overwrite previous self.merged dataframe
        self.merged = self.parser.merge(
            df1 = self.merged, 
            df2 = self.parser.loaded_files[self.postal_codes_coordinates],
            on = self.merged.etab_codeCommuneEtablissement == self.parser.loaded_files[self.postal_codes_coordinates].codes_postaux,
            prefix = "etab_"
        )
        if save:
            self.parser.save_dataframe(
                df = self.merged.select(
                    [
                        'categorieJuridiqueUniteLegale',
                        'activitePrincipaleUniteLegale',
                        'latitude',
                        'longitude',
                        'codes_postaux'
                        ],
                ),
                file_name = self.postal_codes_out_file
            )
        if clear:
            self.parser.loaded_files[self.postal_codes_coordinates].unpersist()

    def get_counts(self, df, by, additional_data = None, post_processor = None):
        # Add persist on distinct_values
        distinct_values = df.select(by).distinct().collect()
        for col, agg_function in self.target_columns.items():
            print('\n')
            print('[WORKER] : get_counts on {} by {}'.format(col, by))
            res = []
            for row in tqdm(distinct_values):
                grouped_data_df = df.filter(df[by] == row[by])
                if agg_function == "count":
                    counts = self.parser.count_values(grouped_data_df, col)
                    if additional_data is not None:
                        counts = counts.withColumn('codes_postaux', lit(row[by]))
                        counts = self.parser.merge(counts, additional_data, by)
                    # If enabled, pass df to post_processor instead of direct write
                    if post_processor is not None:
                        if col in post_processor._target_columns:
                            counts = post_processor.group_by_postal_codes(counts.toPandas()) # Data are already grouped !
                            counts = post_processor.set_category_labels(counts)
                            counts.latitude = counts.latitude.apply(post_processor.format_lat_long)
                            counts.longitude = counts.longitude.apply(post_processor.format_lat_long)
                            if counts.longitude.astype(float).sum() != 0:
                                res.append(counts)
                        else:
                            print('\n')
                            print('[WORKER] : Skipping {} columns. Not registered in post_processor._target_columns'.format(col))
                            break
                    else:
                        res.append(counts.toPandas())
            # Output to pandas dataframe for direct communication with Panel
            output_file_name = "{}_by_{}".format(col, by)
            if len(res) > 0: # To prevent skipped column case
                print('[WORKER] : Saving {} to disk.'.format(output_file_name))
                pd.concat(res).to_csv(os.path.join(self.panel_data_path, output_file_name))
            else:
                print('[WORKER] : No data to write for {}'.format(output_file_name))