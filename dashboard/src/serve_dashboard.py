from BasePanel import BasePanel
from TypeDashboard import TypeDashboard
from ActivityDashboard import ActivityDashboard

TEMPLATE_PATH = "template.html" # problems when file in diffrent location (jinja2.FileSystemLoader)

dashboard_dict = {
    "type_dashboard" : TypeDashboard().run(),
    "activity_dashboard" : ActivityDashboard().run()
}

panel = BasePanel(
    dashboard_dict = dashboard_dict,
    template = TEMPLATE_PATH
)

panel.servable()
