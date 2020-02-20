from pyspark.sql.types import *
from pyspark.sql.functions import udf, collect_list

class SireneParser:

    def __init__(self, spark):
        self.spark = spark
        self.loaded_files = {}

    def load_file(self, file_path, sep=","):
        self.loaded_files[file_path] = self.spark.read.csv(file_path, header=True, sep=sep)

    def save_dataframe(self, df, file_name, coalesce = 1):
        df.coalesce(coalesce).write.csv(file_name, header=True)

    def filter_by_dpt(self, df, dpt_code):
        return df.filter(df.codeCommuneEtablissement.rlike('^{}'.format(dpt_code)))

    def count_values(self, df, col):
        return df.groupby(col).count()

    def merge(self, df1, df2, on, prefix="df1_"):
        # Renaming columns to avoid duplicate names
        if isinstance(on, str):
            col_names = [prefix + x if x != on else x for x in df1.schema.names]
            df1 = df1.toDF(*col_names)
        # Parsing codeCommune to dptCode on origin dataframe
        #df1 = self.format_dpt_code(df1, "etab_codeCommuneEtablissement")
        return df1.join(df2, on=on)

    def format_dpt_code(self, df, col):
        
        def parse_dpt_code(x):
            if x:
                return x[:2]
        
        udf_parse = udf(parse_dpt_code, StringType())
        return df.withColumn("dpt", udf_parse(col))


        