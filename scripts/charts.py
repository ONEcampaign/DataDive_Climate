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


def gain() -> None:
    """ """

    df = get_ndgain_data()
    df = (df
          .dropna(subset = ['gain', 'vulnerability', 'readiness'])
          .pipe(utils.add_income_levels)
          .pipe(utils.add_debt_distress)
          .assign(country = lambda d: coco.convert(d.iso_code, to = 'name_short'))
          .assign(continent = lambda d: coco.convert(d.iso_code, to = 'continent'))
          .pipe(utils.highlight_category, 'income_level', 'Low income', True)
          .pipe(utils.highlight_category, 'continent', 'Africa', True)
          )

    df.to_csv(f'{config.paths.output}/gain.csv', index=False)


def gain_top_bar(df: pd.DataFrame):
    """ """

    df = (df[['iso_code', 'gain', 'vulnerability', 'readiness', 'country', 'continent', 'Africa']]
              .sort_values('gain')
              .iloc[:30]
        .reset_index(drop=True)
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



events = ['Drought', 'Storm', 'Flood'] #'Wildfire', 'Extreme temperature ', 'Insect infestation'

def climate_events(start_year = 2020):
    """ """

    df = get_emdat(start_year = start_year)
    df = (df
                .pivot(index=['year', 'iso_code'], columns = 'disaster_type', values = 'total_affected')
                .fillna(0)
                .assign(total_affected= lambda d: d.sum(axis=1))
                .reset_index()
                )
    df = df.groupby('iso_code', as_index=False).agg('sum').drop(columns='year')
    df = df.melt(id_vars = 'iso_code')
    df = utils.per_capita(df, 'value', percent=True)


    df['country'] = coco.convert(df.iso_code, to='name_short')
    df = utils.filter_countries(df) #keep africa


    #select only top 20 per category
    df = df.sort_values('value_per_capita', ascending=False).groupby('disaster_type').head(30)
    df = df[df['value_per_capita']>0].reset_index(drop=True)




    return df

