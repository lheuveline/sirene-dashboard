import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

class SireneParser:

    def __init__(self):
        self.sc = SparkContext()
        self.sc.setLogLevel("ERROR")
        self.spark = SparkSession.builder.appName('stock-etablissement').getOrCreate()

        self.loaded_files = {}

    def load_file(self, file_path):
        self.loaded_files[file_path] = self.spark.read.csv(file_path, header=True)

    def save_dataframe(self, df, file_name):
        df.coalesce(1).write.csv(file_name, header=True)

    def filter_by_dpt(self, df, dpt_code):
        return df.filter(df.codeCommuneEtablissement.rlike('^{}'.format(dpt_code)))

    def count_values(self, df, col):
        return df.groupby(col).count()