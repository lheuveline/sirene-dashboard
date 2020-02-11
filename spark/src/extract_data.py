from SireneParser import SireneParser

from pyspark.sql.types import *
from pyspark.sql.functions import udf


parser = SireneParser()
parser.load_file('unite_etab_joined.csv')

df = parser.loaded_files['unite_etab_joined.csv']

def parse_dpt_code(x):
    if x:
        return x[:2]

udf_myFunction = udf(parse_dpt_code, StringType()) # if the function returns an int
df = df.withColumn("dpt", udf_myFunction("etab_codeCommuneEtablissement"))

dpt_codes = df.select('dpt').distinct().collect()

for row in dpt_codes:
    dpt_df = df.filter(df.dpt == row.dpt)
    # category_counts = parser.count_values(dpt_df, 'categorieJuridiqueUniteLegale')
    # parser.save_dataframe(category_counts, 'category_by_dpt/{}'.format(row.dpt))
    category_counts = parser.count_values(dpt_df, 'activitePrincipaleUniteLegale')
    parser.save_dataframe(category_counts, 'activity_by_dpt/{}'.format(row.dpt))
