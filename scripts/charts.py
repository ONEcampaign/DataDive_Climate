""" """

import pandas as pd
import country_converter as coco
from scripts import utils, config
from scripts.download_data import get_emdat, get_ndgain_data

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


