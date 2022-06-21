""" """

import pandas as pd
import country_converter as coco
from scripts import utils, config
from scripts.download_data import get_emdat, get_ndgain_data, get_owid

def drought_flood_map() -> None:
    """ """

    df = get_emdat()

    (utils.add_flourish_geometries(df)
          .pipe(utils.filter_countries, by='continent')
          .to_csv(f'{config.paths.output}/drought_flood_map.csv', index=False))


def vuln_readi_scatter():
    """ """

    df = get_ndgain_data()
    df = (df
          .pipe(utils.add_income_levels)
          .pipe(utils.add_debt_distress)
          .dropna(subset = ['gain', 'vulnerability', 'readiness'])
          .assign(country = lambda d: coco.convert(d.iso_code, to = 'name_short'))
          )

    return df


def co2_per_capita_continent():
    """ """

    continents = ['Asia', 'Africa', 'Oceania', 'Europe', 'North America', 'South America']
    df = get_owid(['co2_per_capita'])
    (df[(df.country.isin(continents))&(df.year >=1800)]
     .pivot(index='year', columns='country', values='co2_per_capita')
     .reset_index()
     .to_csv(f'{config.paths.output}/co2_per_capita_continent.csv', index=False))

def co2_per_capita_income():
    """ """

    income_levels = ['Low-income countries', 'Upper-middle-income countries', 'Lower-middle-income countries', 'High-income countries']
    df = get_owid(['co2_per_capita'])
    df = df[(df.country.isin(income_levels))&(df.year >=1800)].pivot(index='year', columns='country', values='co2_per_capita').reset_index()

    df.columns = df.columns.str.replace(' countries', '')

    df.to_csv(f'{config.paths.output}/co2_per_capita_income.csv', index=False)




