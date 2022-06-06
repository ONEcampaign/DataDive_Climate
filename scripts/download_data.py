"""functions to extract data"""

import pandas as pd
from typing import Optional
from scripts import utils, config
import country_converter as coco
from zipfile import ZipFile

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



# ==========================================================
# ND-GAIN
# ==========================================================

def _clean_ndgain(df:pd.DataFrame, index_name:str) -> pd.DataFrame:
    """returns a clean dataframe with latest year data"""

    latest_year = df.columns[-1]
    return (df[['ISO3', latest_year]]
            .rename(columns={'ISO3':'iso_code', latest_year:index_name}))


def read_ndgain_index(folder: ZipFile, index: str, path: str):
    """parse folder structure and read csv for an indicator"""

    if f'{path}{index}.csv' not in list(folder.NameToInfo.keys()):
        raise ValueError(f"Invalid path for {index}: {path}{index}")

    df = pd.read_csv(folder.open(f"{path}{index}.csv"), low_memory=False).pipe(_clean_ndgain, index)

    return df

def get_ndgain_data():
    """pipeline to extract all relevant nd-gain data"""

    url = 'https://gain.nd.edu/assets/437409/resources.zip'
    folder = utils.unzip_folder(url)

    df = read_ndgain_index(folder, 'gain', 'resources/gain/') # get main gain index


    #vulnerability
    vulnerability_indicators = ['vulnerability', 'water', 'food', 'health', 'ecosystems', 'infrastructure', 'habitat']
    for vul_index in vulnerability_indicators:
        df_index = read_ndgain_index(folder, vul_index, 'resources/vulnerability/')
        if len(df) != len(df_index):
            raise ValueError('wrong length')
        df = pd.merge(df, df_index, on = 'iso_code', how='left')


    # readiness
    readiness_indicators = ['readiness', 'economic', 'governance']
    for readiness_index in readiness_indicators:
        df_index = read_ndgain_index(folder, readiness_index, 'resources/readiness/')
        if len(df) != len(df_index):
            raise ValueError('wrong length')
        df = pd.merge(df, df_index, on = 'iso_code', how='left')

    return df