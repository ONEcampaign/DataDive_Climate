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

def get_emdat(*, start_year:Optional[int] = 2000) -> pd.DataFrame:
    """ """

    columns = {'Year':'year', 'Disaster Type':'disaster_type', 'ISO':'iso_code'}

    df = pd.read_excel(f'{config.paths.raw_data}/emdat.xlsx', skiprows=6)

    df = (df[columns.keys()]
          .rename(columns=columns)
          .loc[lambda d: (d.disaster_type.isin(['Drought', 'Flood']))&(d.year>=start_year)]
          .groupby(['iso_code', 'disaster_type'], as_index=False)
          .agg('count')
          .pivot(index='iso_code', columns = 'disaster_type', values='year')
          .reset_index()
          .fillna(0)
          .rename(columns = {'Drought':'drought', 'Flood':'flood'})
          .assign(total = lambda d: d.drought + d.flood)
          .assign(country = lambda d: coco.convert(d.iso_code, to = 'name_short'))
          .loc[lambda d: d.country != 'not found'])

    return df






