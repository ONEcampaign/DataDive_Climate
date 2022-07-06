""" """
import numpy as np
import pandas as pd
import country_converter as coco
from scripts import utils, config
from scripts.config import urls
from scripts.download_data import get_emdat, get_ndgain_data, get_owid, get_emp_ag, get_sahel_population


def gain() -> None:
    """ """

    df = get_ndgain_data()
    df = (df
          .dropna(subset=['gain', 'vulnerability', 'readiness'])
          .pipe(utils.add_income_levels)
          .pipe(utils.add_debt_distress)
          .assign(country=lambda d: coco.convert(d.iso_code, to='name_short'))
          .assign(continent=lambda d: coco.convert(d.iso_code, to='continent'))
          .pipe(utils.highlight_category, 'income_level', 'Low income', True)
          .pipe(utils.highlight_category, 'continent', 'Africa', True)
          .pipe(utils.add_pop_latest)
          )

    #add employment in agriculture
    ag = get_emp_ag()
    df['employment_agr'] = df.iso_code.map(ag.set_index('iso_code')['employment_agr'].to_dict())

    df.to_csv(f'{config.paths.output}/gain.csv', index=False)


def gain_map() -> None:
    """ """
    df = get_ndgain_data()
    df = (df
          .dropna(subset=['gain', 'vulnerability', 'readiness'])
          .pipe(utils.add_income_levels)
          .assign(country=lambda d: coco.convert(d.iso_code, to='name_short'))
          .pipe(utils.add_flourish_geometries)
          )

    df.to_csv(f'{config.paths.output}/gain_map.csv', index=False)



def co2_per_capita_continent():
    """ """

    continents = ['Asia', 'Africa', 'Oceania', 'Europe', 'North America', 'South America']
    df = get_owid(urls.OWID_CO2_URL, ['co2_per_capita'])
    (df[(df.country.isin(continents)) & (df.year >= 1800)]
     .pivot(index='year', columns='country', values='co2_per_capita')
     .reset_index()
     .to_csv(f'{config.paths.output}/co2_per_capita_continent.csv', index=False))

def co2_continent():
    """ """
    continents = ['Asia', 'Africa', 'Oceania', 'Europe', 'North America', 'South America']
    df = get_owid(urls.OWID_CO2_URL, ['co2'])
    (df[(df.country.isin(continents)) & (df.year >= 1800)]
     .pivot(index='year', columns='country', values='co2')
     .reset_index()
     .to_csv(f'{config.paths.output}/co2_continent.csv', index=False))



def co2_per_capita_income():
    """ """

    income_levels = ['Low-income countries', 'Upper-middle-income countries', 'Lower-middle-income countries',
                     'High-income countries']
    df = get_owid(urls.OWID_CO2_URL, ['co2_per_capita'])
    df = df[(df.country.isin(income_levels)) & (df.year >= 1800)].pivot(index='year', columns='country',
                                                                        values='co2_per_capita').reset_index()

    df.columns = df.columns.str.replace(' countries', '')

    df.to_csv(f'{config.paths.output}/co2_per_capita_income.csv', index=False)


def climate_events(start_year=2020):
    """ """

    df = get_emdat(start_year=start_year)

    #calculate affected
    affected = (df[['year', 'disaster_type', 'iso_code', 'total_affected']]
                .copy()
                .groupby(['iso_code', 'disaster_type'])
                .agg('sum')
                .reset_index()
                .drop(columns='year'))

    total_affected = affected.groupby('iso_code', as_index=False).agg('sum').assign(disaster_type = 'Total')
    affected = pd.concat([affected, total_affected])

    #calculate number of events
    numb_events = (df[['year', 'disaster_type', 'iso_code', 'events']]
              .copy()
              .groupby(['iso_code', 'disaster_type'])
              .agg('sum')
              .reset_index()
              .drop(columns='year'))
    total_events = numb_events.groupby('iso_code', as_index=False).agg('sum').assign(disaster_type = 'Total')
    numb_events = pd.concat([numb_events, total_events])

    #merge affected and events
    dff = pd.merge(affected, numb_events)

    dff = (dff
           .assign(country = lambda d: coco.convert(d.iso_code, to='name_short'))
           .pipe(utils.per_capita, target_col = 'total_affected', percent=True)
           .pipe(utils.filter_countries)
           .loc[lambda d: (d.total_affected_per_capita >= 1) #only events affected at least 1%
                          &(d.disaster_type != 'Storm')] #exclude storm events
           .assign(total_affected = lambda d: d.total_affected.astype(int))
           )

    dff.to_csv(f'{config.paths.output}/climate_events_africa.csv', index=False)


def co2_scatter() -> None:

    df = utils.get_latest(get_owid(urls.OWID_CO2_URL, ['co2_per_capita']), by = ['iso_code', 'country'], date_col='year')
    df = (utils.add_pop_latest(df)
          .pipe(utils.add_gdp_latest, per_capita = True)
          .pipe(utils.keep_countries)
          .pipe(utils.add_income_levels)
          .dropna(subset = ['co2_per_capita', 'population', 'gdp_per_capita'])
          .assign(country = lambda d: coco.convert(d.iso_code, to='name_short'))
          .assign(continent = lambda d: coco.convert(d.iso_code, to='continent'))
          .pipe(utils.highlight_category, 'continent', 'Africa', True)
          )

    df.to_csv(f'{config.paths.output}/co2_per_capita_scatter.csv', index=False)


def access_to_elect():
    """ """
    df = utils.get_wb_indicator('EG.ELC.ACCS.ZS')
    df = (df.drop(columns = 'country_name')
          .rename(columns = {'value':'access'})
          .pipe(utils.get_latest, by='iso_code', date_col='year')
          .dropna(subset = 'access')
          .pipe(utils.add_flourish_geometries)
          .assign(country = lambda d: coco.convert(d.iso_code, to='name_short', not_found = np.nan))
          .dropna(subset = 'country')

          )

    return df

def renewable():
    """ """

    df = get_owid(urls.OWID_ENERGY_URL, ['renewables_share_elec'])
    df = (df
          .loc[df.year>=1950]
          .dropna(subset = ['electricity_generation', 'renewables_electricity'])
          .pipe(utils.keep_countries)
          .assign(continent = lambda d: coco.convert(d.iso_code, to='continent'))
          .groupby(['year', 'continent'], as_index=False).agg('sum')

          )

    df = df[['year', 'continent', 'share']].pivot(index='year', columns = 'continent', values = 'share').reset_index()

    return df


def renewable_share():
    """ """
    variables = ['fossil_electricity', 'renewables_electricity', 'renewables_share_elec', 'fossil_share_elec']
    df = get_owid(urls.OWID_ENERGY_URL, variables)

    return df.loc[df.country == 'Africa'].dropna(subset = variables[0:2])

def renewables_country():
    """ """
    variables = ['renewables_share_elec', 'fossil_share_elec']
    df = get_owid(urls.OWID_ENERGY_URL, variables)

    return (df.dropna(subset = variables)
            .drop(columns = 'country')
            .pipe(utils.get_latest, by = 'iso_code', date_col = 'year')
            .pipe(utils.filter_countries)
            .melt(id_vars=['iso_code', 'year'])
            .assign(country = lambda d: coco.convert(d.iso_code, to='name_short')))


def sahel_population():
    """ """

    df = (get_sahel_population()
          .assign(iso_code = lambda d: coco.convert(d.Location))
          .pipe(utils.add_flourish_geometries)
          .dropna(subset = 'Location')
          .assign(pop_2022 = lambda d: round(d[2022]/1000,0))
          .assign(pop_2050 = lambda d: round(d[2050]/1000,0))
          .to_csv(f'{config.paths.output}/sahel_population.csv', index=False)
          )








