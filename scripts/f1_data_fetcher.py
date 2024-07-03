"""
This module fetches and processes Formula 1 data from the Ergast API.

It includes functions for retrieving race data, standings, and qualification results.
The module supports both synchronous and asynchronous data fetching, along with caching and logging.
"""

# === Imports ===
import asyncio
import logging
from datetime import datetime
from typing import Dict

import aiohttp
import nest_asyncio
import pandas as pd
import requests
import requests_cache

# === Config ===

# Constants
CURRENT_YEAR = datetime.now().year
BASE_URL = "http://ergast.com/api/f1"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Apply nest_asyncio
nest_asyncio.apply()

# Set up requests-cache
requests_cache.install_cache(
    "../data/cache/race_results_cache", backend="sqlite", expire_after=86400
)  # cache expires in 1 day

# === Helper Functions ===


def safe_request(url: str) -> Dict:
    """
    Safely make an HTTP GET request and return the JSON response.

    Args:
        url (str): The URL to request.

    Returns:
        Dict: The JSON response as a dictionary.

    Raises:
        requests.RequestException: If the request fails.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error("Failed to fetch data from %s: %s", url, e)
        raise


def parse_race_data(race: Dict) -> Dict:
    """
    Parse race data from a dictionary and extract relevant information.

    Args:
        race (Dict): The dictionary containing race data.

    Returns:
        Dict: Parsed race information including season, round, race name, date, time, circuit details, location, country, longitude, and latitude.
    """
    return {
        "season": race["season"],
        "round": race["round"],
        "raceName": race["raceName"],
        "date": race["date"],
        "time": race.get("time", "N/A"),
        "circuitId": race["Circuit"]["circuitId"],
        "circuit": race["Circuit"]["circuitName"],
        "location": race["Circuit"]["Location"]["locality"],
        "country": race["Circuit"]["Location"]["country"],
        "long": race["Circuit"]["Location"]["long"],
        "lat": race["Circuit"]["Location"]["lat"],
    }


def fetch_all_race_data(
    start_year: int = 1995, end_year: int = CURRENT_YEAR
) -> pd.DataFrame:
    """
    Fetch all race data from the API for a range of years and return it as a Pandas DataFrame.

    Args:
        start_year (int): The starting year for fetching race data. Defaults to 1995.
        end_year (int): The ending year for fetching race data. Defaults to the current year.

    Returns:
        pd.DataFrame: DataFrame containing all the fetched race data.
    """
    all_races = [
        parse_race_data(race)
        for year in range(start_year, end_year + 1)
        for race in safe_request(f"{BASE_URL}/{year}.json")["MRData"]["RaceTable"][
            "Races"
        ]
    ]
    return pd.DataFrame(all_races)


async def fetch_single_race(
    session: aiohttp.ClientSession, season: int, round_num: int
) -> pd.DataFrame:
    """
    Fetch race data for a specific season and round asynchronously.

    Args:
        session (aiohttp.ClientSession): The aiohttp session.
        season (int): The season year for the race data.
        round_num (int): The round number for the race data.

    Returns:
        pd.DataFrame: DataFrame containing the race results.
    """
    race_url = f"{BASE_URL}/{season}/{round_num}/results.json"
    logging.info("Fetching race data for Season: %s, Round: %s", season, round_num)
    try:
        async with session.get(race_url) as response:
            response.raise_for_status()
            race_data = await response.json()
            races = race_data["MRData"]["RaceTable"]["Races"]
            if not races:
                return pd.DataFrame()
            race_results = races[0]["Results"]
            return pd.DataFrame(race_results)
    except aiohttp.ClientError as e:
        logging.error(
            "Failed to fetch race data for Season %s, Round %s: %s",
            season,
            round_num,
            e,
        )
        return pd.DataFrame()


async def fetch_all_races_async(
    start_year: int = 1995, end_year: int = CURRENT_YEAR
) -> pd.DataFrame:
    """
    Fetch race data asynchronously for a range of years and rounds, combining the results into a single Pandas DataFrame.

    Args:
        start_year (int): The starting year for fetching race data. Defaults to 1995.
        end_year (int): The ending year for fetching race data. Defaults to the current year.

    Returns:
        pd.DataFrame: DataFrame containing all the fetched race data.
    """
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_single_race(session, year, round_num)
            for year in range(start_year, end_year + 1)
            # Assuming a maximum of 20 rounds per season
            for round_num in range(1, 21)
        ]
        race_data = await asyncio.gather(*tasks)
    return pd.concat(race_data, ignore_index=True)


def fetch_paginated_data(
    endpoint: str, offset: int = 0, limit: int = 30
) -> pd.DataFrame:
    """
    Fetch paginated data for a specific endpoint and return the results as a Pandas DataFrame.

    Args:
        endpoint (str): The endpoint to fetch data from.
        offset (int): The offset for pagination. Defaults to 0.
        limit (int): The limit for pagination. Defaults to 30.

    Returns:
        pd.DataFrame: DataFrame containing the fetched data.
    """
    all_data = []
    while True:
        url = f"{BASE_URL}/{endpoint}.json?limit={limit}&offset={offset}"
        logging.info("Fetching data with offset: %d, limit: %d", offset, limit)
        try:
            data = safe_request(url)
            items = data["MRData"][f"{endpoint.capitalize()}Table"][
                f"{endpoint.capitalize()}s"
            ]
            all_data.extend(items)
            total_items = int(data["MRData"]["total"])
            offset += limit
            if offset >= total_items:
                break
        except requests.RequestException:
            logging.error("Failed to fetch data at offset %d", offset)
            break
    return pd.DataFrame(all_data)


def fetch_all_f1_drivers(offset: int = 0, limit: int = 30) -> pd.DataFrame:
    """
    Fetch all F1 drivers' data with pagination and return the results as a Pandas DataFrame.

    Args:
        offset (int): The offset for pagination. Defaults to 0.
        limit (int): The limit for pagination. Defaults to 30.

    Returns:
        pd.DataFrame: DataFrame containing the fetched F1 drivers' data.
    """
    return fetch_paginated_data("drivers", offset, limit)


def fetch_all_f1_circuits(offset: int = 0, limit: int = 30) -> pd.DataFrame:
    """
    Fetch all F1 circuits' data with pagination and return the results as a Pandas DataFrame.

    Args:
        offset (int): The offset for pagination. Defaults to 0.
        limit (int): The limit for pagination. Defaults to 30.

    Returns:
        pd.DataFrame: DataFrame containing the fetched F1 circuits' data.
    """
    return fetch_paginated_data("circuits", offset, limit)


def fetch_all_driver_standings(race_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch all driver standings data based on the provided race data and return it as a Pandas DataFrame.

    Args:
        race_data_df (pd.DataFrame): DataFrame containing race data.

    Returns:
        pd.DataFrame: DataFrame with driver standings data.
    """
    all_top10_standings = []
    for _, row in race_data_df.iterrows():
        season = row["season"]
        round_num = row["round"]
        results_url = f"{BASE_URL}/{season}/{round_num}/driverStandings.json"
        logging.info("Fetching results for Season: %s, Round: %s", season, round_num)
        try:
            results_data = safe_request(results_url)
            race_standings = results_data["MRData"]["StandingsTable"]["StandingsLists"]
            all_top10_standings.append(race_standings)
        except (requests.RequestException, KeyError) as e:
            logging.error(
                "Failed to fetch data for season %s, round %s: %s", season, round_num, e
            )
    race_standings = pd.DataFrame(all_top10_standings, columns=["StandingsList"])[
        "StandingsList"
    ].apply(pd.Series)
    race_standings = race_standings.explode("DriverStandings", ignore_index=True)
    race_standings = race_standings.join(
        race_standings["DriverStandings"].apply(pd.Series)
    )
    race_standings = race_standings.join(race_standings["Driver"].apply(pd.Series))
    race_standings = race_standings.join(
        race_standings["Constructors"].apply(pd.Series)
    )
    race_standings = race_standings.drop(
        ["DriverStandings", "Driver", "Constructors", "StandingsList"], axis=1
    )
    return race_standings


def fetch_all_qualifiers(race_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch all qualification data based on the provided race data and return it as a Pandas DataFrame.

    Args:
        race_data_df (pd.DataFrame): DataFrame containing race data.

    Returns:
        pd.DataFrame: DataFrame with qualification data.
    """
    all_qualifications = []
    for _, row in race_data_df.iterrows():
        season = row["season"]
        round_num = row["round"]
        qualification_url = f"{BASE_URL}/{season}/{round_num}/qualifying.json"
        logging.info(
            "Fetching qualification data for Season: %s, Round: %s", season, round_num
        )
        try:
            qualification_data = safe_request(qualification_url)
            races = qualification_data["MRData"]["RaceTable"]["Races"]
            if not races:
                continue
            qualifications = races[0]["QualifyingResults"]
            for qualification in qualifications:
                qualification_info = {
                    "season": season,
                    "round": round_num,
                    "driverId": qualification["Driver"]["driverId"],
                    "driver": qualification["Driver"]["familyName"],
                    "constructorId": qualification["Constructor"]["constructorId"],
                    "q1": qualification.get("Q1", "N/A"),
                    "q2": qualification.get("Q2", "N/A"),
                    "q3": qualification.get("Q3", "N/A"),
                }
                all_qualifications.append(qualification_info)
        except (requests.RequestException, KeyError) as e:
            logging.error(
                "Failed to fetch qualification data for season %s, round %s: %s",
                season,
                round_num,
                e,
            )
    return pd.DataFrame(all_qualifications)
