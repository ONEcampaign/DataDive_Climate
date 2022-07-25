"""Functions to create charts"""

import numpy as np
import pandas as pd
from bblocks.import_tools import world_bank
import country_converter as coco
from scripts import utils, config
from scripts.config import urls
from scripts.download_data import (
    get_emdat,
    get_ndgain_data,
    get_owid,
    get_forest_area,
    get_global_temp,
    get_minerals,
    get_population,
)


def gain() -> None:
    """Create ND-GAIN chart"""

    df = get_ndgain_data()
    df = (
        df.dropna(subset=["gain", "vulnerability", "readiness"])
        .pipe(utils.add_income_levels)
        .pipe(utils.add_debt_distress)
        .assign(country=lambda d: coco.convert(d.iso_code, to="name_short"))
        .assign(continent=lambda d: coco.convert(d.iso_code, to="continent"))
        .pipe(utils.highlight_category, "income_level", "Low income", True)
        .pipe(utils.highlight_category, "continent", "Africa", True)
        .pipe(utils.add_pop_latest)
    )

    # format debt distress
    df[df.debt_distress.isin(["Low", "Moderate"])] = np.nan
    df.debt_distress = df.debt_distress.replace({"High": "High risk of debt distress"})

    df = (
        df.sort_values(by="gain", ascending=False)
        .reset_index(drop=True)
        .assign(rank=lambda d: d.index + 1)
    )

    df = df[
        [
            "gain",
            "vulnerability",
            "readiness",
            "population",
            "country",
            "continent",
            "Low income",
            "Africa",
            "debt_distress",
            "rank",
        ]
    ]

    df = (
        df.assign(pop_annotation=lambda d: round(d.population / 1e6, 2))
        .assign(gain=lambda d: round(d.gain, 1))
        .assign(pop_vulnerability=lambda d: round(d.vulnerability, 2))
        .assign(pop_readiness=lambda d: round(d.readiness, 2))
    )

    df.to_csv(f"{config.paths.output}/gain.csv", index=False)


def co2_per_capita_continent() -> None:
    """Create CO2 emissions per capita by continent chart"""

    continents = [
        "Asia",
        "Africa",
        "Oceania",
        "Europe",
        "North America",
        "South America",
    ]
    df = get_owid(urls.OWID_CO2_URL, ["co2_per_capita"])
    (
        df[(df.country.isin(continents)) & (df.year >= 1800)]
        .pivot(index="year", columns="country", values="co2_per_capita")
        .reset_index()
        .to_csv(f"{config.paths.output}/co2_per_capita_continent.csv", index=False)
    )


def climate_events(start_year=2020) -> None:
    """Create climate event chart

    Args:
        start_year (int): starting year. Default = 2000
    """

    df = get_emdat(start_year=start_year)

    # calculate affected
    affected = (
        df[["year", "disaster_type", "iso_code", "total_affected"]]
        .copy()
        .groupby(["iso_code", "disaster_type"])
        .agg("sum")
        .reset_index()
        .drop(columns="year")
    )

    total_affected = (
        affected.groupby("iso_code", as_index=False)
        .agg("sum")
        .assign(disaster_type="Total")
    )
    affected = pd.concat([affected, total_affected])

    # calculate number of events
    numb_events = (
        df[["year", "disaster_type", "iso_code", "events"]]
        .copy()
        .groupby(["iso_code", "disaster_type"])
        .agg("sum")
        .reset_index()
        .drop(columns="year")
    )
    total_events = (
        numb_events.groupby("iso_code", as_index=False)
        .agg("sum")
        .assign(disaster_type="Total")
    )
    numb_events = pd.concat([numb_events, total_events])

    # merge affected and events
    dff = pd.merge(affected, numb_events)

    dff = (
        dff.assign(country=lambda d: coco.convert(d.iso_code, to="name_short"))
        .pipe(utils.per_capita, target_col="total_affected", percent=True)
        .pipe(utils.filter_countries)
        .loc[
            lambda d: (
                d.total_affected_per_capita >= 1
            )  # only events affected at least 1%
            & (d.disaster_type != "Storm")
        ]  # exclude storm events
        .assign(total_affected=lambda d: d.total_affected.astype(int))
    )

    dff.to_csv(f"{config.paths.output}/climate_events_africa.csv", index=False)


def electricity_cooking() -> None:
    """Create scatter plot chart for access to electricity and clean cooking fuel"""

    df = (world_bank
          .WorldBankData()
          .load_indicator('EG.ELC.ACCS.ZS', most_recent_only=True)
          .load_indicator('EG.CFT.ACCS.ZS', most_recent_only=True)
          .get_data()
          )

    df = (df.pivot(index=['iso_code'], columns = 'indicator', values='value')
          .reset_index()
          .rename(columns = {'EG.ELC.ACCS.ZS':'electricity', 'EG.CFT.ACCS.ZS': 'cooking'})
          .assign(country_name = lambda d: coco.convert(d.iso_code, to='name_short'))
          .assign(continent = lambda d: coco.convert(d.iso_code, to = 'continent'))
          .pipe(utils.add_gdp_latest, per_capita=True)
          .pipe(utils.add_pop_latest)
          .pipe(utils.keep_countries)
          )

    # clean values
    df = (df
          .assign(electricity = lambda d: round(d.electricity, 2))
          .assign(electricity = lambda d: round(d.electricity, 2))
          .assign(population_annotation = lambda d: round(d.population/1e6, 2))
          .assign(gdp_per_capita = lambda d: round(d.gdp_per_capita, 2))
          )

    df.to_csv(f"{config.paths.output}/electricity_cooking.csv", index=False)


def renewable() -> None:
    """Create renewable vs fossil fuel electricity generation chart"""

    variables = ["fossil_electricity", "renewables_electricity"]
    df = get_owid(urls.OWID_ENERGY_URL, variables)

    df = (
        df.pipe(utils.get_latest, by=["iso_code", "country"], date_col="year")
        .pipe(utils.filter_countries)
        .assign(
            share_renewables=lambda d: (
                d.renewables_electricity
                / (d.fossil_electricity + d.renewables_electricity)
                * 100
            )
        )
        .rename(
            columns={
                "renewables_electricity": "renewables",
                "fossil_electricity": "fossil fuels",
            }
        )
        .melt(id_vars=["iso_code", "country", "year", "share_renewables"])
        .dropna(subset="value")
        .sort_values("share_renewables", ascending=False)
    )
    df["country"] = df["country"].replace(
        {
            "Democratic Republic of Congo": "D.R.C",
            "Sao Tome and Principe": "Sao Tome",
            "Central African Republic": "C.A.R",
        }
    )

    df.to_csv(f"{config.paths.output}/renewables_v_fossil.csv", index=False)


def sahel_population() -> None:
    """Create top population growth chart"""

    df = (
        get_population()
        .sort_values("change", ascending=False)
        .head(20)
        .assign(pop_2022=lambda d: round(d[2022] / 1000, 0))
        .assign(pop_2050=lambda d: round(d[2050] / 1000, 0))
    )

    df = df.astype({"pop_2022": "int", "pop_2050": "int"})
    df.Location = coco.convert(df.Location, to="name_short")
    df.to_csv(f"{config.paths.output}/sahel_population.csv", index=False)


def forest_congo(congo_basin=("CMR", "CAF", "COD", "COG", "GAB", "GNQ")) -> None:
    """Create Africa (Congo Basin) forest cover chart

    Args:
        congo_basin: (tuple): list of country iso3 codes in the Congo basin
    """

    df = get_forest_area().pipe(utils.filter_countries).assign(congo_basin=np.nan)

    df.loc[df.iso_code.isin(congo_basin), "congo_basin"] = "congo_basin"

    df.to_csv(f"{config.paths.output}/forest_area.csv", index=False)


def transition_minerals(
    minerals: tuple = (
        "Cobalt",
        "Copper",
        "Chromium (Cr2O3)",
        "Manganese",
        "Platinum",
        "Aluminium",
        "Lithium (Li2O)",
    )
) -> None:
    """Create transition minerals chart

    Args:
        minerals (tuple): list of transition minerals to use
    """

    df = get_minerals(minerals)
    df.country = df.country.replace({"Congo, D.R.": "Congo, Dem. Rep."})
    df["iso_code"] = coco.convert(df.country)
    df["continent"] = coco.convert(df.iso_code, to="continent")

    df.to_csv(f"{config.paths.output}/minerals.csv", index=False)


def temperature() -> None:
    """Create temperature chart"""

    get_global_temp().to_csv(
        f"{config.paths.output}/temperature_change.csv", index=False
    )


def update_charts():
    """Pipeline to update all charts"""

    temperature()
    climate_events()
    gain()
    co2_per_capita_continent()
    sahel_population()
    electricity_cooking()
    renewable()
    transition_minerals()
    forest_congo()

    print("successfully updated charts")
