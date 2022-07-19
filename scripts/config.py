"""class to store paths to different files and directories"""

import os

class Paths:
    """File paths"""

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
    """Source urls"""

    @property
    def OWID_CO2_URL(self):
        return 'https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv'

    @property
    def OWID_ENERGY_URL(self):
        return 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv'

    @property
    def UN_POP_PROSPECTS(self):
        return "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_Demographic_Indicators_Medium.zip"

    @property
    def MINERALS(self):
        return "https://www.world-mining-data.info/wmd/downloads/XLS/6.5.%20Share_of_World_Mineral_Production_2020_by_Countries.xlsx"

    @property
    def ND_GAIN(self):
        return "https://gain.nd.edu/assets/437409/resources.zip"

    @property
    def TEMPERATURE(self):
        return 'https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv'

urls = Urls()

CLIMATE_EVENTS = ['Drought', 'Storm', 'Flood']  # 'Wildfire', 'Extreme temperature ', 'Insect infestation'