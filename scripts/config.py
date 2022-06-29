"""class to store paths to different files and directories"""

import os

class Paths:

    def __init__(self, project_dir):
        self.project_dir = project_dir

    @property
    def scripts(self):
        return os.path.join(self.project_dir, "scripts")

    @property
    def output(self):
        return os.path.join(self.project_dir, "output")

    @property
    def raw_data(self):
        return os.path.join(self.project_dir, "raw_data")

    @property
    def glossaries(self):
        return os.path.join(self.project_dir, "glossaries")

paths = Paths(os.path.dirname(os.path.dirname(__file__)))

class Urls:

    @property
    def OWID_CO2_URL(self):
        return 'https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv'

    @property
    def OWID_ENERGY_URL(self):
        return 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv'

urls = Urls()

CLIMATE_EVENTS = ['Drought', 'Storm', 'Flood']  # 'Wildfire', 'Extreme temperature ', 'Insect infestation'