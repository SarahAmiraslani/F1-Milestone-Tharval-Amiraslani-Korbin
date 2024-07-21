import logging
from tqdm import tqdm
from config import WEATHER_API_KEY
from scripts.utilities import format_race_data
import requests
import sys
import pandas as pd
import httpx
import datetime

BASE_URL = "https://api.weatherapi.com/v1/history.json"

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def is_valid_date(date_str: str) -> bool:
    """
    Checks if the input date string is in the format "YYYY-MM-DD".

    Args:
        date_str (str): The input date string.

    Returns:
        bool: True if the date string is in the format "YYYY-MM-DD", False otherwise.
    """
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def construct_url(latitude: float, longitude: float, date: str) -> str:
    """
    Constructs the URL for retrieving weather data from the API.

    Args:
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.
        date (str): The date for which weather data is requested.

    Returns:
        str: The constructed URL.
    """
    if not is_valid_date(date):
        raise ValueError("Invalid date format. Please use the format 'YYYY-MM-DD'.")

    location = f"{latitude},{longitude}"
    return f"{BASE_URL}?key={WEATHER_API_KEY}&q={location}&dt={date}"


def make_request(url: str) -> dict:
    """
    Makes a GET request to the specified URL and returns the response as a JSON dictionary.

    Args:
        url (str): The URL to make the request to.

    Returns:
        dict: The response data as a JSON dictionary, or None if the request fails. This should include the weather data for the specified date and location.
    """
    try:
        with httpx.Client(timeout=5) as client:
            response = client.get(url)
            response.raise_for_status()  # Raise an exception for non-2xx status codes
            return response.json()
    except (httpx.HTTPError, httpx.TimeoutException, httpx.RequestError) as e:
        logging.error("Failed to fetch weather data: %s", e)
        return {}


def fetch_historic_weather() -> pd.DataFrame:
    """
    Fetches historic weather data for races and merges it with the race information.
    Returns:
        pd.DataFrame: The DataFrame containing race information with added weather data.
    """
    race_dfs = []  # list to store individual race DataFrames with weather data

    # Read and format the race data
    race_info_path = "data/raw/Race_Information_1995_2023.csv"
    race_df = format_race_data(pd.read_csv(race_info_path))

    # Loop through each race entry to fetch the weather data
    for _, row in tqdm(
        race_df.iterrows(), total=race_df.shape[0], desc="Fetching weather data"
    ):
        date = row["date"]
        lat = row["lat"]
        long = row["long"]

        try:
            # Construct the URL for the weather data request
            results_url = construct_url(lat, long, date)
            if weather_data := make_request(results_url):
                forecast = weather_data.get("forecast", {}).get("forecastday", [])
                if forecast and (weather_info := forecast[0].get("day", {})):
                    weather_info.update({"date": date, "lat": lat, "long": long})
                    race_dfs.append(weather_info)
                else:
                    logging.warning(
                        "No weather information found for %s, location: (%s,%s)",
                        date,
                        lat,
                        long,
                    )
            else:
                logging.warning(
                    "Failed to fetch data for %s, location: (%s,%s)", date, lat, long
                )

        except Exception as e:
            logging.error("An error occurred: %s", e)

    # Convert the list of weather data dictionaries to a DataFrame
    weather_df = pd.DataFrame(race_dfs)

    return race_df.merge(weather_df, on=["date", "lat", "long"], how="left")
