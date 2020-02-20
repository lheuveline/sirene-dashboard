import os 

import altair as alt
import panel as pn

from jinja2 import Environment, FileSystemLoader

class BasePanel:

    def __init__(self, dashboard_dict, template = "./template.html"):
        self.dashboard_dict = dashboard_dict
        self.jinja2_fs_loader_path = '/'.join(template.split('/')[:-1])
        self.template = template

    def load_template(self):
        env = Environment(loader=FileSystemLoader(self.jinja2_fs_loader_path))
        template = env.get_template(self.template)
        self.tmpl = pn.Template(template)

    def render_template(self):
        for dashboard_name, dashboard in self.dashboard_dict.items():
            self.tmpl.add_panel(dashboard_name, dashboard)
    
    def servable(self):
        self.load_template()
        self.render_template()
        self.tmpl.servable(title="Sirene Exploration")

