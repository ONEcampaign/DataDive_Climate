"""functions to extract data"""

import pandas as pd
from typing import Optional
from scripts import utils, config
import country_converter as coco
from zipfile import ZipFile
import numpy as np
import statsmodels.api as sm

# ====================================================
#Our World In Data - CO2 and Greenhouse gas emissions
# ====================================================



def get_owid(url: str, indicators: Optional[list] = None):
    """read data from OWID into a dataframe"""

    try:
        df = pd.read_csv(url)
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




def _clean_emdat(df:pd.DataFrame, start_year = 2000) -> pd.DataFrame:
    """Cleaning function for EMDAT"""

    columns = {'Year':'year', 'Disaster Type':'disaster_type', 'ISO':'iso_code', 'Total Affected': 'total_affected'}

    #'Region':'region','Start Year': 'start_year', 'Start Month': 'start_month', 'Start Day': 'start_day','End Year': 'end_year', 'End Month': 'end_month', 'End Day': 'end_day',

    df = (df[columns.keys()]
          .rename(columns=columns)
          .loc[lambda d: (d.year>=start_year)&(d.disaster_type.isin(config.CLIMATE_EVENTS))]
          .fillna(0)
          .reset_index(drop=True)
    )

    df['events'] = df.disaster_type
    df = df.groupby(['year', 'disaster_type', 'iso_code']).agg({'total_affected':'sum', 'events':'count'}).reset_index()

    return df


def get_emdat(*, start_year:Optional[int] = 2000) -> pd.DataFrame:
    """ """


    df = pd.read_excel(f'{config.paths.raw_data}/emdat.xlsx', skiprows=6)
    df = _clean_emdat(df, start_year)

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


def get_global_temp(lowess_frac: float = 0.25) -> pd.DataFrame:
    """Data from NASA GISS: https://data.giss.nasa.gov/gistemp/

    frac: float
        fraction of data used when estimating y values, between 0-1

    """

    url = 'https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv'
    try:
        df = pd.read_csv(url, skiprows = 1)
    except ConnectionError:
        raise ConnectionError('Could not read NASA GISS data')

    df = (df.rename(columns = {'Year':'year', 'J-D':'temp_anomaly'})
          [['year', 'temp_anomaly']]
          .replace('***', np.nan)
          .assign(temp_anomaly = lambda d: pd.to_numeric(d.temp_anomaly))
          .dropna(subset = 'temp_anomaly'))

    #apply lowess smoothing
    df['lowess'] = sm.nonparametric.lowess(df.temp_anomaly, df.year,
                                                                     return_sorted=False, frac = lowess_frac)
    return df



def get_emp_ag():
    """ """

    df = utils.get_wb_indicator('SL.AGR.EMPL.ZS')
    return (df
            .dropna(subset = 'value')
            .drop(columns = 'country_name')
            .pipe(utils.get_latest, by = 'iso_code', date_col = 'year')
            .rename(columns = {'value':'employment_agr'})
            .drop(columns = 'year')

            )
