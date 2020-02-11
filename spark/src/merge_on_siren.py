import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession.builder.appName('stock-etablissement').getOrCreate()

base_file = "StockEtablissement_utf8.csv"
siren_file = "StockUniteLegale_utf8.csv"

base_df = spark.read.csv(base_file, header=True)
siren_df = spark.read.csv(siren_file, header=True)

# Renaming columns to avoid duplicate names
col_names = ['siren'] + ["etab_" + x for x in base_df.schema.names[1:]]
base_df = base_df.toDF(*col_names)

joined_df = base_df.join(siren_df, on="siren")
joined_df.write.csv('unite_etab_joined.csv', header=True)