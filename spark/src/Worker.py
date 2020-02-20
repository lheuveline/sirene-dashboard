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

    def run(self):
        # Load and merge data
        self.merge_on_siren(coalesce = 8, save = False)

        # Load and label longitude / latitude by postal code
        self.merge_on_postal_codes(coalesce = 8)

        # Tasks
        self.dpt_codes = self.get_distinct_dpt_codes(self.merged)
        self.extract_counts(self.merged, self.dpt_codes)

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

    def get_distinct_dpt_codes(self, df):
        return df.select('dpt').distinct().collect()

    def extract_counts(self, df, dpt_codes):
        print('\n\n\n')
        for row in dpt_codes:
            dpt_df = df.filter(df.dpt == row.dpt)
            for col, agg_function in self.target_columns.items():
                if agg_function == "count":
                    print('EXTRACTING {} INFOS'.format(col))
                    start = time.time()
                    counts = self.parser.count_values(df, col)
                    self.parser.save_dataframe(counts, '{}_by_dpt/{}'.format(col, row.dpt))
                    end = time.time()
                    elapsed = round((end - start) / 60, 2)
                    print('EXTRACTION FOR {} DONE !'.format(col))
                    print('TOOK : {}m'.format(elapsed))

        # Parallel grouping. Causing OOM Error if run on local
        # self.parser.load_file("activity_category_postal_codes")
        # df = self.parser.loaded_files['activity_category_postal_codes']
        # df = self.parser.format_dpt_code(df, df.codes_postaux)
        # grouping_cols = ["dpt", "categorieJuridiqueUniteLegale"]
        # other_cols = [c for c in df.columns if c not in grouping_cols]
        # df.groupBy(grouping_cols).agg(*[collect_list(c).alias(c) for c in other_cols]).show()

    def get_counts(self, df, by, additional_data = None, post_processor = None):
        # Add persist on distinct_values
        distinct_values = df.select(by).distinct().collect()
        res = []
        for row in tqdm(distinct_values): # DEV  - remove slicing
            dpt_df = df.filter(df[by] == row[by])
            for col, agg_function in self.target_columns.items():
                if agg_function == "count":
                    counts = self.parser.count_values(dpt_df, col)
                    if additional_data is not None:
                        counts = counts.withColumn('codes_postaux', lit(row[by]))
                        counts = self.parser.merge(counts, additional_data, by)
                    # If enabled, pass df to post_processor instead of direct write
                    if post_processor is not None:
                        if col in post_processor._target_columns:
                            counts = post_processor.group_by_postal_codes(counts.toPandas())
                            counts = post_processor.set_category_labels(counts)
                            counts.latitude = counts.latitude.apply(post_processor.format_lat_long)
                            counts.longitude = counts.longitude.apply(post_processor.format_lat_long)
                            if counts.longitude.astype(float).sum() != 0:
                                # self.parser.save_dataframe(
                                #     self.spark.createDataFrame(counts),
                                #     '{}_by_{}/{}'.format(col, by, row[by])
                                # )
                                res.append(counts)
                    else:
                        #self.parser.save_dataframe(counts, '{}_by_{}/{}'.format(col, by, row[by]))
                        res.append(counts)
        # Output to pandas dataframe for direct communication with Panel
        pd.concat(res).to_csv(os.path.join(self.panel_data_path, 'activity_by_postal_codes.csv'))

    
def main():
    worker = Worker()
    worker.run()

if __name__ == '__main__':
    main()