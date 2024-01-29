# Imports
import sys
import requests
import pandas as pd

BASE_URL = "http://ergast.com/api/f1"
CURRENT_YEAR = 2024  # Update this to the current year


def get_race_data(start_year: int = 1995) -> pd.DataFrame:
    # Get race data starting from 1995
    all_races = []
    for year in range(start_year, CURRENT_YEAR):
        year_url = f"{BASE_URL}/{year}.json"
        response = requests.get(year_url, timeout=10)
        if response.status_code == 200:
            year_data = response.json()
            try:
                races = year_data['MRData']['RaceTable']['Races']
                for race in races:
                    race_info = {
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
                        'lat':  race['Circuit']['Location']['lat'],
                    }
                    all_races.append(race_info)
            except KeyError:
                print(f"Data format error for year {year}")
        else:
            print(f"Failed to fetch data for year {year}")

    return pd.DataFrame(all_races)


def fetch_race_results(race_data_df):

    race_dfs = []  # List to store individual race DataFrames

    for _, row in race_data_df.iterrows():
        season = row['season']
        round_num = row['round']
        sys.stdout.write(f"\rFetching results for Season: {season}, Round: {round_num}")
        sys.stdout.flush()
        results_url = f"{BASE_URL}/{season}/{round_num}/results.json"
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


def fetch_all_f1_drivers():
    all_drivers = []
    offset = 0
    limit = 30  # You can adjust the limit as needed

    while True:
        url = f"{BASE_URL}?limit={limit}&offset={offset}"
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            drivers_data = response.json()
            drivers = drivers_data['MRData']['DriverTable']['Drivers']
            all_drivers.extend(drivers)

            # Check if there are more pages of data
            total_drivers = int(drivers_data['MRData']['total'])
            offset += limit
            if offset >= total_drivers:
                break
        else:
            print(f"Failed to fetch data at offset {offset}")
            break

    return pd.DataFrame(all_drivers)


def fetch_all_f1_circuits() -> pd.DataFrame:
    all_circuits = []
    offset = 0
    limit = 30  # Adjust the limit as needed

    while True:
        url = f"{BASE_URL}?limit={limit}&offset={offset}"
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            circuits_data = response.json()
            circuits = circuits_data['MRData']['CircuitTable']['Circuits']
            all_circuits.extend(circuits)

            # Check if there are more pages of data
            total_circuits = int(circuits_data['MRData']['total'])
            offset += limit
            if offset >= total_circuits:
                break
        else:
            print(f"Failed to fetch data at offset {offset}")
            break

    return pd.DataFrame(all_circuits)


def fetch_all_driver_standings(race_data_df: pd.DataFrame) -> pd.DataFrame:

    all_top10_standings = []
    for _, row in race_data_df.iterrows():
        season = row['season']
        round_num = row['round']
        results_url = f"{BASE_URL}/{season}/{round_num}/driverStandings.json"
        sys.stdout.write(f"\rFetching results for Season: {season}, Round: {round_num}.")
        sys.stdout.flush()
        response = requests.get(results_url, timeout=10)
        if response.status_code == 200:
            results_data = response.json()
            try:
                race_standings = results_data['MRData']['StandingsTable']['StandingsLists']
                all_top10_standings.append(race_standings)
            except KeyError:
                print(f"\nData format error for season {season}, round {round_num}")
        else:
            print(f"\nFailed to fetch data for season {season}, round {round_num}")

    # Explode the columns to capture all features
    df = pd.DataFrame(
        all_top10_standings,
        columns=['StandingsList']
        )['StandingsList'].apply(pd.Series)
    df = df.explode('DriverStandings', ignore_index=True)
    df = df.join(df['DriverStandings'].apply(pd.Series))
    df = df.join(df['Driver'].apply(pd.Series))
    df = df.join(df['Constructors'].apply(pd.Series))

    # Drop the original columns
    df = df.drop(
        ['DriverStandings', 'Driver', 'Constructors', 'StandingsList'],
        axis=1
        )
    return df
