"""functions to extract data"""

import pandas as pd
from typing import Optional
from scripts import utils, config
from zipfile import ZipFile


def get_owid(url: str, indicators: Optional[list] = None) -> pd.DataFrame:
    """read data from OWID into a dataframe

    Args:
        url (str): url to csv file
        indicators (list): list of indicators to extract

    Returns:
        pd.DataFrame
    """

    try:
        df = pd.read_csv(url)
    except ConnectionError:
        raise ConnectionError("Could not read OWID data")

    if indicators is not None:
        for indicator in indicators:
            if indicator not in df.columns:
                raise ValueError(f"{indicator} is not found in the dataset")

        return df[["iso_code", "country", "year"] + indicators]

    else:
        return df


def _clean_emdat(df: pd.DataFrame, start_year=2000) -> pd.DataFrame:
    """Cleaning function for EMDAT

    Args:
        df (pd.DataFrame): pandas dataframe to clean
        start year (int): starting year. Default = 2000

    Returns pd.DataFrame
    """

    columns = {
        "Year": "year",
        "Disaster Type": "disaster_type",
        "ISO": "iso_code",
        "Total Affected": "total_affected",
    }

    df = (
        df[columns.keys()]
        .rename(columns=columns)
        .loc[
            lambda d: (d.year >= start_year)
            & (d.disaster_type.isin(config.CLIMATE_EVENTS))
        ]
        .fillna(0)
        .reset_index(drop=True)
    )

    df["events"] = df.disaster_type
    df = (
        df.groupby(["year", "disaster_type", "iso_code"])
        .agg({"total_affected": "sum", "events": "count"})
        .reset_index()
    )

    return df


def get_emdat(*, start_year: Optional[int] = 2000) -> pd.DataFrame:
    """extract and clean emdat data

    Args:
        start_year (int): Starting year. Default = 2000

    Returns:
        pd.DataFrame
    """

    df = pd.read_excel(f"{config.paths.raw_data}/emdat.xlsx", skiprows=6)
    df = _clean_emdat(df, start_year)

    return df


def _clean_ndgain(df: pd.DataFrame, index_name: str) -> pd.DataFrame:
    """returns a clean dataframe with latest year data"

    Args:
        df (pd.DataFrame): pandas dataframe to clean
        index_name (str): name of index column

    Returns:
        pd.DataFrame
    """

    latest_year = df.columns[-1]
    return df[["ISO3", latest_year]].rename(
        columns={"ISO3": "iso_code", latest_year: index_name}
    )


def read_ndgain_index(folder: ZipFile, index: str, path: str) -> pd.DataFrame:
    """parse folder structure and read csv for an indicator

    Args:
        folder (ZiplFile): zipped folder object
        index (str): index file name
        path (str): path to file

    Returns:
        pd.DataFrame
    """

    if f"{path}{index}.csv" not in list(folder.NameToInfo.keys()):
        raise ValueError(f"Invalid path for {index}: {path}{index}")

    df = pd.read_csv(folder.open(f"{path}{index}.csv"), low_memory=False).pipe(
        _clean_ndgain, index
    )

    return df


def get_ndgain_data() -> pd.DataFrame:
    """pipeline to extract all relevant nd-gain data

    Returns:
        pd.DataFrame
    """

    folder = utils.unzip_folder(config.urls.ND_GAIN)

    df = read_ndgain_index(folder, "gain", "resources/gain/")  # get main gain index

    # vulnerability
    vulnerability_indicators = [
        "vulnerability",
        "water",
        "food",
        "health",
        "ecosystems",
        "infrastructure",
        "habitat",
    ]
    for vul_index in vulnerability_indicators:
        df_index = read_ndgain_index(folder, vul_index, "resources/vulnerability/")
        if len(df) != len(df_index):
            raise ValueError("wrong length")
        df = pd.merge(df, df_index, on="iso_code", how="left")

    # readiness
    readiness_indicators = ["readiness", "economic", "governance"]
    for readiness_index in readiness_indicators:
        df_index = read_ndgain_index(folder, readiness_index, "resources/readiness/")
        if len(df) != len(df_index):
            raise ValueError("wrong length")
        df = pd.merge(df, df_index, on="iso_code", how="left")

    return df


def get_global_temp() -> pd.DataFrame:
    """Extract temperature data from MET https://climate.metoffice.cloud/temperature.html#datasets

    Returns:
        pd.DataFrame
    """

    try:
        df = pd.read_csv(config.urls.TEMPERATURE)
    except ConnectionError:
        raise ConnectionError("Could not read data")

    df = (df.loc[:, ['Year', 'HadCRUT5 (degC)']]
          .rename(columns = {'Year':'year', 'HadCRUT5 (degC)':'temp_change'}))

    return df


def get_population(variant: str = "Medium") -> pd.DataFrame:
    """Extract population data from UN World Population Prospects

    Args:
        variant (str): variant level. Default = Medium

    Returns:
        pd.DataFrame
    """

    rename_countries = {
        "China, Hong Kong SAR": "Hong Kong",
        "China, Taiwan Province of China": "Taiwan",
        "China, Macao SAR": "Macao",
    }

    folder = utils.unzip_folder(config.urls.UN_POP_PROSPECTS)
    df = pd.read_csv(
        folder.open("WPP2022_Demographic_Indicators_Medium.csv"), low_memory=False
    )

    df = (
        df.loc[(df.Variant == variant) & (df.Time.isin([2022, 2050]))]
        .reset_index(drop=True)
        .pipe(utils.keep_countries, mapping_col="LocID", mapper="ISOnumeric")
        .replace(rename_countries)
        .pivot(index="Location", columns="Time", values="TPopulation1Jan")
        .reset_index()
        .assign(change=lambda d: ((d[2050] - d[2022]) / d[2022]) * 100)
    )

    return df


def get_forest_area() -> pd.DataFrame:
    """Extract forest area data from WDI

    Returns:
        pd.DataFrame
    """

    df = (
        utils.get_wb_indicator("AG.LND.FRST.ZS")
        .pipe(utils.get_latest, by=["iso_code", "country_name"], date_col="year")
        .pipe(utils.add_flourish_geometries)
    )
    return df


def get_minerals(minerals: tuple) -> pd.DataFrame:
    """Extract data from world mining data

    Args:
        minerals (tuple): minerals to extract

    Returns:
        pd.DataFrame
    """

    columns = {
        "Country": "country",
        "unit": "unit",
        "Production 2020": "prod_2020",
        "Share in %": "share_pct",
    }

    df = pd.DataFrame()
    for m in minerals:
        mineral_df = pd.read_excel(config.urls.MINERALS, sheet_name=m, skiprows=1)
        mineral_df = (
            mineral_df.rename(columns=columns)
            .loc[:, list(columns.values())]
            .assign(mineral=m)
        )

        df = pd.concat([df, mineral_df])

    df = df[df.country != "Total"]

    return df
