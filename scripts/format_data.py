import pandas as pd


def format_race_data(race_df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats the date column to the required 'YYYY-MM-DD' format for the weather API. 
    """
    race_df = race_df[['date','season','raceName','circuitId','circuit','lat','long']] # could add 'location' if 'long'/'lat' are not providing results
    race_df['date'] = pd.to_datetime(race_df['date'])
    return race_df


def remove_unnamed_column(df):
    """
    Remove the "Unnamed: 0" column from a pandas DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame from which the "Unnamed: 0" column will be removed.

    Returns:
    pandas.DataFrame: The DataFrame with the "Unnamed: 0" column removed, if it existed.
    """
    if "Unnamed: 0" in df.columns:
        return df.drop(columns=["Unnamed: 0"])
    return df