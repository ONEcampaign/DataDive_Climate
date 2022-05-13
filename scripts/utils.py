"""Utility functions"""

from scripts import config
import pandas as pd
import weo
import wbgapi as wb


def add_flourish_geometries(df: pd.DataFrame, key_column_name:str) -> pd.DataFrame:
    """
    Adds a geometry column to a dataframe based on iso3 code
    key_column_name: name of column with iso3 codes to merge on
    """

    g = pd.read_json(f'{config.paths.glossaries}/flourish_geometries_world.json')
    g = (g
         .rename(columns={g.columns[0]: "flourish_geom", g.columns[1]: key_column_name})
         .iloc[1:]
         .drop_duplicates(subset=key_column_name, keep="first")
         .reset_index(drop=True))

    return pd.merge(g, df, on = key_column_name, how='left')



# ===============================================
# IMF
# ===============================================

WEO_YEAR: int = 2022
WEO_RELEASE: int = 1
GDP_YEAR = 2021

def _download_weo(year: int = WEO_YEAR, release: int = WEO_RELEASE) -> None:
    """Downloads WEO as a csv to glossaries folder as "weo_month_year.csv"""

    try:
        weo.download(
            year=year,
            release=release,
            directory=config.paths.raw_data,
            filename=f"weo_{year}_{release}.csv",
        )
    except ConnectionError:
        raise ConnectionError("Could not download weo data")


def __clean_weo(df: pd.DataFrame) -> pd.DataFrame:
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

def weo_to_dict(indicator: str, target_year: int = 2022, *, min_year: int = 2018) -> dict:
    """
    downloads weo data and returns a dictionary for a specific indicator and target year
    if target year value is null, the latest available year with data is used
    min year: earliest year acceptable to return a value
    """

    df = weo.WEO(f"{config.paths.raw_data}/weo_{WEO_YEAR}_{WEO_RELEASE}.csv").df

    df = (df
          .pipe(__clean_weo)
          .dropna(subset = ['value'])
          .loc[lambda d: (d.indicator == indicator)&(d.year>=min_year)&(d.year<=target_year),["iso_code", "year","value"]]
          .reset_index(drop=True)
          )

    return (df.loc[df.groupby(['iso_code'])['year'].transform(max) == df['year']]
            .set_index("iso_code")["value"]
            .to_dict()
            )



def get_gdp(gdp_year: int = GDP_YEAR) -> dict:
    """
    Retrieves gdp values for a specific year
    """

    gdp = weo_to_dict(indicator = "NGDPD", target_year=gdp_year)
    for i in gdp.keys():
        gdp[i] = gdp[i]*1e9

    return gdp


def add_gdp(df: pd.DataFrame, gdp_year:int = GDP_YEAR, iso_codes_col: str = "iso_code") -> pd.DataFrame:
    """adds gdp to a dataframe"""

    gdp: dict = get_gdp(gdp_year)
    return df.assign(gdp=lambda d: d[iso_codes_col].map(gdp))


def get_gdppc(gdp_year: int = GDP_YEAR) -> dict:
    """retrieves gdp per capita values for a given year"""

    return weo_to_dict(indicator = 'NGDPDPC', target_year=gdp_year)


def add_gdppc(df: pd.DataFrame, gdp_year:int = GDP_YEAR, iso_codes_col: str = "iso_code") -> pd.DataFrame:
    """adds gdp per capita to a dataframe"""

    gdppc: dict = get_gdppc(gdp_year)
    return df.assign(gdppc=lambda d: d[iso_codes_col].map(gdppc))


# =============================================================================
#  income levels
# =============================================================================


def _download_income_levels() -> None:
    """Downloads fresh version of income levels from WB"""

    url = "https://databank.worldbank.org/data/download/site-content/CLASS.xlsx"

    df = pd.read_excel(
        url,
        sheet_name="List of economies",
        usecols=["Code", "Income group"],
        na_values=None,
    )

    df = df.dropna(subset=["Income group"])

    df.to_csv(config.paths.glossaries + r"/income_levels.csv", index=False)
    print("Downloaded income levels")


def get_income_levels() -> dict:
    """Return income level dictionary"""
    file = config.paths.glossaries + r"/income_levels.csv"
    return pd.read_csv(file, na_values=None, index_col="Code")["Income group"].to_dict()


def add_income_levels(df: pd.DataFrame, iso_codes_col: str = "iso_code") -> pd.DataFrame:
    """Add income levels to a dataframe"""

    income_levels: dict = get_income_levels()
    return df.assign(income_level=lambda d: d[iso_codes_col].map(income_levels))


# =============================================================================
#  population
# =============================================================================


def get_wb_indicator(indicator: str) -> pd.DataFrame:
    """query wb api using a specific indicator code"""
    try:
        return wb.data.DataFrame(
            indicator,
            mrnev=1,
            numericTimeKeys=True,
            labels=False,
            columns="series",
            timeColumns=True,
        )
    except ConnectionError:
        raise ConnectionError(f"Could not retrieve indicator {indicator}")

def wb_indicator_to_dict(df: pd.DataFrame, indicator_col: str):
    """converts a wb series to a dictionary where keys are iso3 codes and values
    are indicator values"""
    return df[indicator_col].astype("int32").to_dict()


def update_wb_indicator(id_: str) -> None:

    # get population data
    get_wb_indicator(id_).to_csv(config.paths.raw_data + rf"/{id_}.csv", index=True)


def add_population(df: pd.DataFrame, iso_codes_col: str = "iso_code") -> pd.DataFrame:
    """Adds population to a dataframe"""

    id_ = "SP.POP.TOTL"
    pop_: dict = pd.read_csv(
        config.paths.raw_data + rf"/{id_}.csv", dtype={f"{id_}": float}, index_col=0
    ).pipe(wb_indicator_to_dict, id_)

    return df.assign(population=lambda d: d[iso_codes_col].map(pop_))



