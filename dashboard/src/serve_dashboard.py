from BasePanel import BasePanel
from TypeDashboard import TypeDashboard
from ActivityByDptDashboard import ActivityByDptDashboard
from ActivityGeographicDashboard import ActivityGeographicDashboard

TEMPLATE_PATH = "template.html" # problems when file in diffrent location (jinja2.FileSystemLoader)

type_dashboard_params = {
    "data_path" : "../data/category_by_dpt",
    "type_code_labels_path" : '../data/code_categoriejuridique.json'
}

activity_dashboard_params = {
    "data_path" : "../data/activity_by_dpt",
    "activity_code_labels_path" : '../data/naf_2008.json'
}

activity_geographic_dashboard_params = {
    #"data_path" : "../data/clean_activitePrincipaleUniteLegale_by_codes_postaux/dataset.csv",
    "data_path" : "../data/activity_by_postal_codes.csv",
    "activity_code_labels_path" : '../data/naf_2008.json',
    "postal_codes_lat_long_labels_path" : "../data/EUCircos_Regions_departements_circonscriptions_communes_gps.csv"
}


dashboard_dict = {
    "type_dashboard" : TypeDashboard(**type_dashboard_params).run(),
    "activity_dashboard" : ActivityByDptDashboard(**activity_dashboard_params).run(),
    "activity_geographic_dashboard" : ActivityGeographicDashboard(**activity_geographic_dashboard_params).run()
}

panel = BasePanel(
    dashboard_dict = dashboard_dict,
    template = TEMPLATE_PATH
)

panel.servable()
