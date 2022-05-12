"""functions to extract data"""

import pandas as pd
from typing import Optional

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





