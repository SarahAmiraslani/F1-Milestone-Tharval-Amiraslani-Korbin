"""
This module fetches and processes Formula 1 data from the Ergast API and Wikipedia.

It includes functions for retrieving race data, standings, and qualification results.
The module supports both synchronous and asynchronous data fetching, along with caching and logging.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional

import aiohttp
import nest_asyncio
import pandas as pd
import requests
import requests_cache
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

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
)

# === Ergast API data fetching (paginated) ===


class ServerUnavailableError(Exception):
    """Raised when the server is unavailable."""


class ServerDisconnectedError(Exception):
    """Raised when the server disconnects unexpectedly."""


def safe_request(url: str) -> Dict:
    """Safely make an HTTP GET request and return the JSON response."""
    try:
        with requests.get(url, timeout=10) as response:
            response.raise_for_status()
            return response.json()
    except requests.RequestException as e:
        logging.error("Failed to fetch data from %s: %s", url, e)
        raise


def parse_race_data(race: Dict) -> Dict:
    """Parse race data from a dictionary and extract relevant information."""
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


def fetch_race_info(
    start_year: int = 1995, end_year: int = CURRENT_YEAR
) -> pd.DataFrame:
    """Fetch race information from the API for a range of years."""
    all_races = [
        parse_race_data(race)
        for year in range(start_year, end_year + 1)
        for race in safe_request(f"{BASE_URL}/{year}.json")["MRData"]["RaceTable"][
            "Races"
        ]
    ]
    return pd.DataFrame(all_races)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=30),
    retry=retry_if_exception_type((ServerUnavailableError, ServerDisconnectedError)),
)
async def fetch_single_race_results(
    session: aiohttp.ClientSession, season: int, round_num: int
) -> pd.DataFrame:
    """Fetch race data for a specific season and round asynchronously."""
    # Construct URL
    race_url = f"{BASE_URL}/{season}/{round_num}/results.json"
    logging.info("Fetching race data for Season: %s, Round: %s", season, round_num)
    try:
        # Fetch data and extract results
        async with session.get(race_url) as response:
            if response.status == 503:
                raise ServerUnavailableError(
                    f"Service unavailable for {season} Round {round_num}"
                )
            response.raise_for_status()
            race_data = await response.json()
            races = race_data["MRData"]["RaceTable"]["Races"]
            return pd.DataFrame(races[0]["Results"]) if races else pd.DataFrame()
    except aiohttp.ClientError as e:
        if isinstance(e, aiohttp.ServerDisconnectedError):
            raise ServerDisconnectedError(
                f"Server disconnected for {season} Round {round_num}"
            ) from e
        logging.error(
            "Failed to fetch race data for Season %s, Round %s: %s",
            season,
            round_num,
            e,
        )
        raise


async def fetch_race_results(
    start_year: int = 1995, end_year: int = CURRENT_YEAR, batch_size: int = 10
) -> pd.DataFrame:
    """Fetch race data asynchronously for a range of years and rounds."""
    async with aiohttp.ClientSession() as session:
        # Create tasks
        tasks = [
            fetch_single_race_results(session, year, round_num)
            for year in range(start_year, end_year + 1)
            for round_num in range(1, 21)
        ]
        # Fetch data in batches
        all_results = []
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            results = await asyncio.gather(*batch, return_exceptions=True)
            all_results.extend([r for r in results if not isinstance(r, Exception)])
            await asyncio.sleep(5)  # Throttling
    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()


def fetch_paginated_data(
    endpoint: str, offset: int = 0, limit: int = 30
) -> pd.DataFrame:
    """Fetch paginated data for a specific endpoint."""
    all_data = []
    while True:
        # Construct URL
        url = f"{BASE_URL}/{endpoint}.json?limit={limit}&offset={offset}"
        logging.info("Fetching data with offset: %d, limit: %d", offset, limit)
        try:
            # Fetch data and extract items
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
        # Construct URL for driver standings
        season = row["season"]
        round_num = row["round"]
        results_url = f"{BASE_URL}/{season}/{round_num}/driverStandings.json"
        logging.info("Fetching results for Season: %s, Round: %s", season, round_num)
        try:
            # Fetch data and extract standings
            results_data = safe_request(results_url)
            race_standings = results_data["MRData"]["StandingsTable"]["StandingsLists"]
            all_top10_standings.append(race_standings)
        except (requests.RequestException, KeyError) as e:
            logging.error(
                "Failed to fetch data for season %s, round %s: %s", season, round_num, e
            )
    # Convert to DataFrame, flatten nested JSON, format
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


# === Wikipedia data fetching ===


def fetch_wiki_circuits(url: str, timeout: int = 10) -> Optional[str]:
    """
    Fetch HTML content from a given URL.

    Args:
    url (str): The URL to fetch the HTML content from.
    timeout (int): The timeout for the request in seconds. Default is 10.

    Returns:
    Optional[str]: The HTML content as a string if successful, None otherwise.
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        logging.info("Successfully fetched data from %s", url)
        return response.text
    except requests.RequestException as e:
        logging.error("Failed to fetch the webpage: %s", e)
        return None


def parse_wiki_html_to_dataframe(html_content: str) -> Optional[pd.DataFrame]:
    """
    Parse HTML content and extract Formula One circuit data into a DataFrame.

    Args:
    html_content (str): The HTML content to parse.

    Returns:
    Optional[pd.DataFrame]: A DataFrame containing the parsed data, or None if parsing fails.
    """
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        caption = soup.find(
            "caption",
            string=lambda text: "Formula One circuits" in text if text else False,
        )
        if not caption:
            raise ValueError("Table caption 'Formula One circuits' not found")

        table = caption.find_parent("table")
        if not table:
            raise ValueError("Table not found")

        headers = [header.text.strip() for header in table.find_all("th")]
        rows = []
        for row in table.find_all("tr")[1:]:  # Skip the header row
            cells = row.find_all(["td", "th"])
            row_data = [cell.text.strip() for cell in cells]
            rows.append(row_data)

        df = pd.DataFrame(rows, columns=headers)
        if "Map" in df.columns:
            df = df.drop(columns=["Map"])

        logging.info("Successfully parsed HTML content. DataFrame shape: %s", df.shape)
        return df
    except Exception as e:
        logging.error("Failed to parse HTML content: %s", e)
        return None


def get_wiki_circuits(
    url: str, local_file: str = "../data/wiki_circuits.csv"
) -> Optional[pd.DataFrame]:
    """
    Fetch Formula One circuit data from Wikipedia or load from a local file.

    Args:
    url (str): The URL to fetch the data from.
    local_file (str): The path to the local CSV file. Default is "../data/wiki_circuits.csv".

    Returns:
    Optional[pd.DataFrame]: A DataFrame containing the circuit data, or None if both fetching and loading fail.
    """
    if html_content := fetch_wiki_circuits(url):
        wiki_circuits = parse_wiki_html_to_dataframe(html_content)
        if wiki_circuits is not None:
            wiki_circuits.to_csv(local_file, index=False)
            logging.info("Saved fetched data to %s", local_file)
            return wiki_circuits

    logging.info("Attempting to read from the local file: %s", local_file)
    try:
        wiki_circuits = pd.read_csv(local_file)
        logging.info(f"Successfully loaded data from %s", local_file)
        return wiki_circuits
    except Exception as e:
        logging.error(f"Failed to read from the local file: %s", e)
        return None


if __name__ == "__main__":
    # Example usage and basic tests
    logging.info("Running basic tests for F1 data fetcher...")
