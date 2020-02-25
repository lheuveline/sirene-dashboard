from Worker import Worker
from PostProcessor import ActivityPostProcessor

def extract_counts_by_dpt(worker):
    worker.parser.load_file("unite_etab_joined")
    df = worker.parser.loaded_files["unite_etab_joined"]
    worker.get_counts(df, 'dpt')

def groupby_postal_code(worker):

    activity_category_file = "activity_category_postal_codes"
    lat_long_postal_codes_files = "EUCircos_Regions_departements_circonscriptions_communes_gps.csv"

    worker.parser.load_file(activity_category_file)
    df = worker.parser.loaded_files[activity_category_file]
    
    worker.parser.load_file(lat_long_postal_codes_files, sep=";")
    additional_data_df = worker.parser.loaded_files[lat_long_postal_codes_files].select(['codes_postaux', 'latitude', 'longitude'])
    
    post_processor = ActivityPostProcessor()
    worker.get_counts(df, 'codes_postaux', additional_data_df, post_processor)

def main():
    worker = Worker()
    extract_counts_by_dpt(worker)
    groupby_postal_code(worker)


if __name__ == "__main__":
    main()