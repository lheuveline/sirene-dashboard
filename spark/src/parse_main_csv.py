import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession.builder.appName('stock-etablissement').getOrCreate()

file_path = "StockEtablissement_utf8.csv"

df = spark.read.csv(file_path, header=True)
df = df.filter(df.codeCommuneEtablissement.rlike('^69'))
df.write.csv('StockEtablissement_utf8_69.csv', header=True)