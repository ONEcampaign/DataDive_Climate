""" """

import pandas as pd
import country_converter as coco
from scripts import utils, config
from scripts.download_data import get_emdat

def drought_flood_map() -> None:
    """ """

    df = get_emdat()

    (utils.add_flourish_geometries(df)
          .pipe(utils.filter_countries, by='continent')
          .to_csv(f'{config.paths.output}/drought_flood_map.csv', index=False))

