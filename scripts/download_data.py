"""functions to extract data"""

import pandas as pd
from typing import Optional
from scripts import utils, config
import country_converter as coco

# ====================================================
#Our World In Data - CO2 and Greenhouse gas emissions
# ====================================================

def get_owid(indicators: Optional[list] = None):
    """read data from OWID into a dataframe"""

    URL = 'https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv'

    try:
        df = pd.read_csv(URL)
    except ConnectionError:
        raise ConnectionError('Could not read OWID data')

    if indicators is not None:
        for indicator in indicators:
            if indicator not in df.columns:
                raise ValueError(f'{indicator} is not found in the dataset')

        return df[['iso_code', 'country', 'year'] + indicators]

    else:
        return df



# ===========================================
# Disaster events database
# ============================================

climate_events = ['Drought', 'Storm', 'Flood', 'Wildfire', 'Extreme temperature ', 'Insect infestation']


def _clean_emdat(df:pd.DataFrame, start_year = 1950) -> pd.DataFrame:
    """Cleaning function for EMDAT"""

    columns = {'Year':'year', 'Disaster Type':'disaster_type', 'ISO':'iso_code', 'Region':'region',
               'Start Year': 'start_year', 'Start Month': 'start_month', 'Start Day': 'start_day',
               'End Year': 'end_year', 'End Month': 'end_month', 'End Day': 'end_day', 'Total Affected': 'total_affected'}

    df = (df[columns.keys()]
          .rename(columns=columns)
          .loc[lambda d: d.year>=start_year])

    return df


def get_emdat(*, start_year:Optional[int] = 2000) -> pd.DataFrame:
    """ """


    df = pd.read_excel(f'{config.paths.raw_data}/emdat.xlsx', skiprows=6)
    df = _clean_emdat(df)

    return df




