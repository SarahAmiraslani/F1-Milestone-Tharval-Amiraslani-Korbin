"""
This script fetches Formula 1 data from the Ergast API.

The script includes functions to construct URLs for the Ergast API, send GET requests, and fetch race results. 
The BASE_URL constant is the base URL for the Ergast API, and the CURRENT_YEAR constant is the current year. 
The pandas library is used to store and manipulate the fetched data.
"""

import datetime
import logging
import pandas as pd
import requests
import sys


BASE_URL = "http://ergast.com/api/f1"
CURRENT_YEAR = int(datetime.datetime.now().year) 

# === API functions ===

def construct_url(year: int, endpoint: str = '', additional_params: str = '') -> str:
    """
    Construct the Ergast API URL for a given year and endpoint.

    Parameters:
    year (int): The year to fetch Formula 1 data for.
    endpoint (str, optional): The API endpoint to fetch data from. Defaults to ''.
    additional_params (str, optional): Additional parameters to append to the URL. Defaults to ''.

    Returns:
    str: The URL to fetch Formula 1 data for the given year from the Ergast API.
    """
    url = f"{BASE_URL}/{year}" # when year is set to 0, this signals that the drivers endpoint doesn't require a year
    if endpoint:
        url += f"/{endpoint}"
    if additional_params:
        url += f"/{additional_params}"
    return url + ".json"


def make_request(url: str) -> dict:
    """
    Make a GET request to the given URL and return the JSON response.

    This function sends a GET request to the provided URL and returns the JSON response. 
    If the request fails for any reason (e.g., server error, timeout), an exception is raised.

    Parameters:
    url (str): The URL to send the GET request to.

    Returns:
    dict: The JSON response from the server.

    Raises:
    HTTPError: If the GET request fails for any reason.
    """
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # This will raise an exception if the request failed
    return response.json()

# === Race data functions ===

def parse_race_response(response: dict) -> list:
    """
    Parse the JSON response to extract race data.

    Parameters:
    response (dict): The JSON response from the Ergast API.

    Returns:
    list: A list of dictionaries, each containing data for a single race.
    """
    races = response['MRData']['RaceTable']['Races']
    return [
        {
            'season': race['season'],
            'round': race['round'],
            'raceName': race['raceName'],
            'date': race['date'],
            'time': race.get('time', 'N/A'),  # 'time' not available for all races
            'circuitId': race['Circuit']['circuitId'],
            'circuit': race['Circuit']['circuitName'],
            'location': race['Circuit']['Location']['locality'],
            'country': race['Circuit']['Location']['country'],
            'long': race['Circuit']['Location']['long'],
            'lat': race['Circuit']['Location']['lat'],
        } for race in races
    ]


def get_race_data(start_year: int = 1995) -> pd.DataFrame:
    """
    Fetch race data from the Ergast API starting from a specified year.

    This function iterates over each year from the start_year to the current year, constructs a URL to 
    access the Ergast API's JSON data for that year, sends a GET request, and parses the response to 
    extract race data.

    The extracted data for each year is appended to a list. After fetching the data for all years, 
    the function creates a DataFrame from the list of race data.

    Parameters:
    start_year (int): The starting year for fetching race data. Defaults to 1995.

    Returns:
    pd.DataFrame: A DataFrame containing the race data for each year.
    """
    all_races = []
    for year in range(start_year, CURRENT_YEAR):
        url = construct_url(year)
        response = make_request(url)
        all_races.extend(parse_race_response(response))
    return pd.DataFrame(all_races)


def fetch_race_results(race_data_df):
    """
    Fetch race results data from the Ergast API for each race in a DataFrame.

    This function iterates over each row in the input DataFrame, which should contain race data with 'season' 
    and 'round' columns. For each row, it constructs a URL to access the Ergast API's JSON data for that race's 
    season and round, sends a GET request, and parses the response to extract race results data.

    If a GET request fails, the function prints an error message. If the response data format is unexpected, 
    the function prints an error message and continues to the next row.

    Parameters:
    race_data_df (pd.DataFrame): A DataFrame containing race data with 'season' and 'round' columns.

    Returns:
    list: A list of DataFrames, each containing the race results data for a single race.
    """
    race_dfs = []  # List to store individual race DataFrames

    for _, row in race_data_df.iterrows():
        season = row['season']
        round_num = row['round']
        sys.stdout.write(f"\rFetching results for Season: {season}, Round: {round_num}")
        sys.stdout.flush()
        results_url = construct_url(season, 'results', f'{round_num}')
        response = requests.get(results_url, timeout=10)

        if response.status_code == 200:
            results_data = response.json()
            try:
                results = results_data['MRData']['RaceTable']['Races'][0]['Results']
                results_df = pd.DataFrame(results)
                results_df['season'] = season
                results_df['round'] = round_num
                race_dfs.append(results_df)
            except KeyError:
                print(f"Data format error for season {season}, round {round_num}")
        else:
            print(f"Failed to fetch data for season {season}, round {round_num}")

    return race_dfs

# === Driver and circuit data functions === 

def fetch_all_f1_drivers() -> pd.DataFrame:
    """
    Fetch all Formula 1 drivers data from the Ergast API.

    This function retrieves all drivers data from the Ergast API by sending GET requests in a loop.
    The loop continues until all pages of data have been fetched. The function uses pagination parameters
    (limit and offset) to fetch data in chunks. If a GET request fails, the function prints an error message
    and breaks the loop.

    Returns:
    pd.DataFrame: A DataFrame containing the data of all Formula 1 drivers.
    """
    all_drivers = []
    page = 0
    limit = 30
    while True:
        offset = page * limit
        url = construct_url(0, f'drivers?limit={limit}&offset={offset}') 
        response = requests.get(url)
        if response.status_code != 200:
            print(f"GET request failed with status code {response.status_code}")
            break
        data = response.json()
        try:
            drivers = data['MRData']['DriverTable']['Drivers']
            if not drivers:
                break
            all_drivers.extend(drivers)
            page += 1
        except KeyError:
            print("Unexpected response format")
            break
    return pd.DataFrame(all_drivers)


def fetch_all_f1_circuits() -> pd.DataFrame:
    """
    Fetch all Formula 1 circuits data from the Ergast API.

    This function retrieves all circuits data from the Ergast API by sending GET requests in a loop.
    The loop continues until all pages of data have been fetched. The function uses pagination parameters
    (limit and offset) to fetch data in chunks. If a GET request fails, the function prints an error message
    and breaks the loop.

    Returns:
    pd.DataFrame: A DataFrame containing the data of all Formula 1 circuits.
    """
    all_circuits = []
    page = 0
    limit = 30
    while True:
        offset = page * limit
        url = construct_url(0, 'circuits', f'?limit={limit}&offset={offset}') 
        response = requests.get(url)
        if response.status_code != 200:
            print(f"GET request failed with status code {response.status_code}")
            break
        data = response.json()
        try:
            circuits = data['MRData']['CircuitTable']['Circuits']
            if not circuits:
                break
            all_circuits.extend(circuits)
            page += 1
        except KeyError:
            print("Unexpected response format")
            break
    return pd.DataFrame(all_circuits)


def fetch_all_driver_standings() -> pd.DataFrame:
    """
    Fetch all Formula 1 driver standings data from the Ergast API.

    This function retrieves all driver standings data from the Ergast API by sending GET requests in a loop.
    The loop continues until all pages of data have been fetched. The function uses pagination parameters
    (limit and offset) to fetch data in chunks. If a GET request fails, the function prints an error message
    and breaks the loop.

    Returns:
    pd.DataFrame: A DataFrame containing the data of all Formula 1 driver standings.
    """
    all_standings = []
    page = 0
    limit = 30
    while True:
        offset = page * limit
        url = construct_url(0, 'driverStandings', f'?limit={limit}&offset={offset}') # year set to 0 because the driverStandings endpoint doesn't require a year
        response = requests.get(url)
        if response.status_code != 200:
            print(f"GET request failed with status code {response.status_code}")
            break
        data = response.json()
        try:
            standings = data['MRData']['StandingsTable']['StandingsLists']
            if not standings:
                break
            all_standings.extend(standings)
            page += 1
        except KeyError:
            print("Unexpected response format")
            break
    return pd.DataFrame(all_standings)
