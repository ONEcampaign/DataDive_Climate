"""Utility functions"""

from scripts import config
import wbgapi as wb
import pandas as pd
import weo
import country_converter as coco
import requests
from zipfile import ZipFile
import io


def unzip_folder(url) -> ZipFile:
    """
    unzips a folder from the web
    and returns a zipfile object
    """

    try:
        response = requests.get(url)
        folder = ZipFile(io.BytesIO(response.content))
        return folder
    except ConnectionError:
        raise ConnectionError("Could not read file")


def add_flourish_geometries(
        df: pd.DataFrame, key_column_name: str = "iso_code"
) -> pd.DataFrame:
    """
    Adds a geometry column to a dataframe based on iso3 code
        df: DataFrame to add a column
        key_column_name: name of column with iso3 codes to merge on, default = 'iso_code'
    """

    g = pd.read_json(f"{config.paths.glossaries}/flourish_geometries_world.json")
    g = (
        g.rename(columns={g.columns[0]: "flourish_geom", g.columns[1]: key_column_name})
            .iloc[1:]
            .drop_duplicates(subset=key_column_name, keep="first")
            .reset_index(drop=True)
    )

    return pd.merge(g, df, on=key_column_name, how="left")


def remove_unnamed_cols(df: pd.DataFrame) -> pd.DataFrame:
    """removes all columns with 'Unnamed'"""

    return df.loc[:, ~df.columns.str.contains("Unnamed")]


def clean_numeric_column(column: pd.Series) -> pd.Series:
    """removes commas and transforms pandas series to numeric"""

    column = column.str.replace(",", "")
    column = pd.to_numeric(column)

    return column


def get_latest_values(
        df: pd.DataFrame, grouping_col: str, date_col: str
) -> pd.DataFrame:
    """returns a dataframe with only latest values per group"""

    return df.loc[
        df.groupby(grouping_col)[date_col].transform(max) == df[date_col]
        ].reset_index(drop=True)


def keep_countries(df: pd.DataFrame, iso_col: str = "iso_code") -> pd.DataFrame:
    """returns a dataframe with only countries"""

    cc = coco.CountryConverter()
    return df[df[iso_col].isin(cc.data["ISO3"])].reset_index(drop=True)


def filter_countries(
        df: pd.DataFrame, by: str, values: list = ["Africa"], iso_col: str = "iso_code"
) -> pd.DataFrame:
    """
    returns a filtered dataframe
        by: filtering category -'continent', UNregion etc.
        values: list of values to keep
    """

    cc = coco.CountryConverter()
    if by not in cc.data.columns:
        raise ValueError(f"{by} is not valid")

    df[by] = coco.convert(df[iso_col], to=by)
    return df[df[by].isin(values)].drop(columns=by).reset_index(drop=True)


# ============================================================================
# Income levels
# ============================================================================


def get_income_levels() -> pd.DataFrame:
    """Downloads fresh version of income levels from WB"""
    url = "https://databank.worldbank.org/data/download/site-content/CLASS.xlsx"

    df = pd.read_excel(
        url,
        sheet_name="List of economies",
        usecols=["Code", "Income group"],
        na_values=None,
    )

    df = df.dropna(subset=["Income group"])

    return df


def add_income_levels(df: pd.DataFrame, iso_col: str = "iso_code") -> pd.DataFrame:
    """Add income levels to a dataframe"""

    income_levels = (
        get_income_levels().set_index("Code").loc[:, "Income group"].to_dict()
    )
    return df.assign(income_level=lambda d: d[iso_col].map(income_levels))


# ===================================================
# World Bank API
# ===================================================


def _download_wb_data(code: str, database: int = 2) -> pd.DataFrame:
    """
    Queries indicator from World Bank API
        default database = 2 (World Development Indicators)
    """

    try:
        df = wb.data.DataFrame(
            series=code, db=database, numericTimeKeys=True, labels=True
        )
        return df

    except:
        raise Exception(f"Could not retieve {code} indicator from World Bank")


def _melt_wb_data(df: pd.DataFrame) -> pd.DataFrame:
    """Melts dataframe extracted from World Bank from wide to "long" format"""

    df = df.reset_index()
    df = df.melt(id_vars=df.columns[0:2])
    df.columns = ["iso_code", "country_name", "year", "value"]

    return df


def get_wb_indicator(code: str, database: int = 2) -> pd.DataFrame:
    """
    Steps to extract and clean an indicator from World Bank
        code: indicator code
        database: database number, default = 2 (World Development Indicators)
    """

    df = _download_wb_data(code, database).pipe(_melt_wb_data)
    print(f"Successfully extracted {code} from World Bank")

    return df


# ==========================================
# IMF
# ==============================================

WEO_YEAR = 2022
WEO_RELEASE = 1


def _download_weo(year: int = WEO_YEAR, release: int = WEO_RELEASE) -> None:
    """Downloads WEO as a csv to raw data folder as "weo_month_year.csv"""

    try:
        weo.download(
            year=year,
            release=release,
            directory=config.paths.raw_data,
            filename=f"weo_{year}_{release}.csv",
        )
    except ConnectionError:
        raise ConnectionError("Could not download weo data")


def _clean_weo(df: pd.DataFrame) -> pd.DataFrame:
    """cleans and formats weo dataframe"""

    columns = {
        "ISO": "iso_code",
        "WEO Subject Code": "indicator",
        "Subject Descriptor": "indicator_name",
        "Units": "units",
        "Scale": "scale",
    }
    cols_to_drop = [
        "WEO Country Code",
        "Country",
        "Subject Notes",
        "Country/Series-specific Notes",
        "Estimates Start After",
    ]
    return (
        df.drop(cols_to_drop, axis=1)
            .rename(columns=columns)
            .melt(id_vars=columns.values(), var_name="year", value_name="value")
            .assign(
            value=lambda d: d.value.map(
                lambda x: str(x).replace(",", "").replace("-", "")
            )
        )
            .astype({"year": "int32"})
            .assign(value=lambda d: pd.to_numeric(d.value, errors="coerce"))
    )


def get_weo_indicator_latest(
        indicator: str, target_year: int = 2022, *, min_year: int = 2018
) -> pd.DataFrame:
    """
    Retrieves values for an indicator for a target year
    """

    df = weo.WEO(f"{config.paths.raw_data}/weo_{WEO_YEAR}_{WEO_RELEASE}.csv").df

    df = (
        df.pipe(_clean_weo)
            .dropna(subset=["value"])
            .loc[
            lambda d: (d.indicator == indicator)
                      & (d.year >= min_year)
                      & (d.year <= target_year),
            ["iso_code", "year", "value"],
        ]
            .reset_index(drop=True)
    )
    return df.loc[
        df.groupby(["iso_code"])["year"].transform(max) == df["year"],
        ["iso_code", "value"],
    ]


def get_gdp_latest(per_capita: bool = False, year: int = 2022) -> pd.DataFrame:
    """
    return latest gdp values
        set per_capita = True to return gdp per capita values
    """
    if per_capita:
        return get_weo_indicator_latest(target_year=year, indicator="NGDPDPC")
    else:
        return get_weo_indicator_latest(target_year=year, indicator="NGDPD").assign(
            value=lambda d: d.value * 1e9
        )


def add_gdp_latest(
        df: pd.DataFrame, iso_col: str = "iso_code", per_capita=False, year: int = 2022
) -> pd.DataFrame:
    """adds a column with latest gdp values to a dataframe"""

    if per_capita:
        new_col_name = "gdp_per_capita"
    else:
        new_col_name = "gdp"

    gdp_df = get_gdp_latest(year=year, per_capita=per_capita)
    gdp_dict = gdp_df.set_index("iso_code")["value"].to_dict()

    df[new_col_name] = df[iso_col].map(gdp_dict)

    return df