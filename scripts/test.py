import pandas as pd
import requests
import sys

def get_race_data(start_year:int=1995) -> pd.DataFrame:
    """Get race data starting from 1995"""
    base_url = "http://ergast.com/api/f1"
    current_year = 2024  # Update this to the current year
    all_races = []

    for year in range(start_year, current_year):
        year_url = f"{base_url}/{year}.json"
        response = requests.get(year_url)
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
                        'time': race.get('time', 'N/A'),  # 'time' might not be available for all races
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


def fetch_all_standings(race_data_df:pd.DataFrame,driver_standings:bool=True) -> list[pd.DataFrame]:
    base_url = "http://ergast.com/api/f1"
    all_standings = []
    for _, row in race_data_df.iterrows():
        season = row['season']
        round_num = row['round']
        if driver_standings:
            results_url = f"{base_url}/{season}/{round_num}/driverStandings.json"
        else:
            results_url = f"{base_url}/{season}/{round_num}/constructorStandings.json"
        sys.stdout.write(f"\rFetching results for Season: {season}, Round: {round_num}.")
        sys.stdout.flush()
        response = requests.get(results_url)

        if response.status_code == 200:
            results_data = response.json()
            try:
                standings = results_data['MRData']['StandingsTable']['StandingsLists']
                pos_info = {
                    "season": standings['season'],
                    "round": standings['round']
                }
                for position in standings:
                    pos_info['position'] = position['position']
                    pos_info['points'] = position['points']
                    pos_info['wins'] = position['wins']

                    driver_dict = position['Driver']
                    pos_info['driverId'] = driver_dict['driverId']
                    pos_info['driverName'] = driver_dict['driverId']['givenName'] + driver_dict['driverId']['familyName']
                    pos_info['driverCode'] = driver_dict['code']
                    
                    constructor_dict = position['Constructors']
                    pos_info['constructorId'] = constructor_dict['constructorId'],
                    pos_info['constructorName'] = constructor_dict['name'],
                    pos_info['constructorNationality'] = position[]
                    
                    all_standings.append(pos_info)


                results_df = pd.DataFrame(results)
                results_df['season'] = season
                results_df['round'] = round_num
                standing_dfs.append(results_df)
            except KeyError:
                    print(f"Data format error for season {season}, round {round_num}")
        else:
            print(f"Failed to fetch data for season {season}, round {round_num}")

    return standing_dfs


# Fetch the data and create a DataFrame
race_data_df = get_race_data()
race_data_df.to_csv("1995_data/Race_Information_1995_2023.csv")

# Fetch all driver and constructor standing data
driver_standing_list = fetch_all_standings(race_data_df)
driver_standing_df = pd.concat(driver_standing_list)
driver_standing_df.to_csv("1995_data/driver_standings.csv")

constructor_standing_list = fetch_all_standings(race_data_df,driver_standings=False)
constructor_standing_df = pd.concat(constructor_standing_list)
driver_standing_df.to_csv("1995_data/constructor_standings.csv")
