UNITE_LEGALE_FILE_URL="https://www.data.gouv.fr/fr/datasets/r/e83dfb8f-0cd7-48f6-933f-bad321563980"
UNITE_LEGALE_ARCHIVE_NAME="StockUniteLegale_utf8.zip"
UNITE_LEGALE_FILE_NAME="StockUniteLegale_utf8.csv"

ETAB_FILE_URL="https://www.data.gouv.fr/fr/datasets/r/d710d803-1f62-41b4-9fc7-c83e39b42f63"
ETAB_LEGALE_ARCHIVE_NAME="StockEtablissement_utf8.zip"
ETAB_LEGALE_FILE_NAME="StockEtablissement_utf8.csv"

hadoop fs -rm $UNITE_LEGALE_FILE_NAME
curl -L $UNITE_LEGALE_FILE_URL --output $UNITE_LEGALE_ARCHIVE_NAME
unzip $UNITE_LEGALE_ARCHIVE_NAME
rm $UNITE_LEGALE_ARCHIVE_NAME
hadoop fs -put $UNITE_LEGALE_FILE_NAME
rm $UNITE_LEGALE_FILE_NAME

hadoop fs -rm $ETAB_LEGALE_FILE_NAME
curl -L $ETAB_FILE_URL --output $ETAB_LEGALE_ARCHIVE_NAME
unzip $ETAB_LEGALE_ARCHIVE_NAME
rm $ETAB_LEGALE_ARCHIVE_NAME
hadoop fs -put $ETAB_LEGALE_FILE_NAME
rm $ETAB_LEGALE_FILE_NAME
