from config import WEATHER_API_KEY
from format_data import format_race_data
import requests
import sys
import pandas as pd
import httpx
import datetime


BASE_URL = "https://api.weatherapi.com/v1/history.json"

# === Weather API functions ===

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
    url = f"{BASE_URL}?key={WEATHER_API_KEY}&q={location}&dt={date}"
    return url


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
        print(f"Failed to fetch weather data: {e}")
        return None


# === Race weather request and processing functions ===

def fetch_historic_weather(url) -> dict:

    race_dfs = [] # list to store individual race DataFrames with weather data

    race_info_path = "data/raw/Race_Information_1995_2023.csv"
    race_df = format_race_data(pd.read_csv(race_info_path))
    
    for _, row in race_df.iterrows():
        date = row['date']
        lat = row['lat']
        long = row['long']
        sys.stdout.write(f"\rFetching weather for date: {date}, location: ({lat},{long})")
        sys.stdout.flush()
        results_url = construct_url(lat, long, date)
        response = requests.get(results_url, timeout=10)
        if response.status_code == 200:
            results_data = response.json()
            raise NotImplementedError("This function is not yet implemented")
            # TODO: save the results
        else:
            print(f"Failed to fetch data for {date}, location: ({lat},{long})")





