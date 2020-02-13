from pyspark import SparkContext
from pyspark.sql import SparkSession

import time

from SireneParser import SireneParser


class Worker:

    def __init__(self):
        self.sc = SparkContext()
        self.sc.setLogLevel("ERROR")
        self.spark = SparkSession.builder.appName('stock-etablissement').getOrCreate()

        self.sirene_unite_legale = "StockUniteLegale_utf8.csv"
        self.sirene_etab = "StockEtablissement_utf8.csv"
        self.merge_out_file = "unite_etab_joined"

        self.parser = SireneParser(self.spark)

        self.target_columns = {
            'categorieJuridiqueUniteLegale' : "count",
            'activitePrincipaleUniteLegale' : "count"
        }

    def run(self):
        self.merge_on_siren()
        self.dpt_codes = self.get_distinct_dpt_codes(self.merged)
        self.extract_counts(self.merged)

    def merge_on_siren(self):
        self.parser.load_file(self.sirene_unite_legale)
        self.parser.load_file(self.sirene_etab)
        self.merged = self.parser.merge(
            df1 = self.parser.loaded_files[self.sirene_etab],
            df2 = self.parser.loaded_files[self.sirene_unite_legale],
            on = "siren",
            prefix = "etab_" 
        )
        self.parser.save_dataframe(self.merged, self.merge_out_file)

    def get_distinct_dpt_codes(self, df):
        return self.df.select('dpt').distinct().collect()

    def extract_counts(self, df):
        print('\n\n\n')
        for row in self.dpt_codes:
            dpt_df = df.filter(df.dpt == row.dpt)
            for col, agg_function in self.target_columns.items():
                if agg_function == "count":
                    print('EXTRACTING {} INFOS'.format(col))
                    start = time.time()
                    counts = parser.count_values(df, col)
                    self.parser.save_dataframe(counts, '{}_by_dpt/{}'.format(col, row.dpt))
                    end = time.time()
                    elapsed = round((end - start) / 60, 2)
                    print('EXTRACTION FOR {} DONE !'.format(col))
                    print('TOOK : {}m'.format(elapsed))

def main():
    worker = Worker()
    worker.run()

if __name__ == '__main__':
    main()