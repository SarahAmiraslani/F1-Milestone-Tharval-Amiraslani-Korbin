from ast import literal_eval
import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def format_race_data(race_df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats the date column to the required 'YYYY-MM-DD' format for the weather API.

    Args:
        race_df (pd.DataFrame): DataFrame containing race data.

    Returns:
        pd.DataFrame: Formatted DataFrame.
    """
    race_df = race_df[
        ["date", "season", "raceName", "circuitId", "circuit", "lat", "long"]
    ]
    race_df["date"] = pd.to_datetime(race_df["date"])
    return race_df


def remove_unnamed_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns with 'Unnamed' in their name.

    Args:
        df (pd.DataFrame): DataFrame with potential unnamed columns.

    Returns:
        pd.DataFrame: DataFrame with unnamed columns removed.
    """
    return df.loc[:, ~df.columns.str.contains("^Unnamed")]


def convert_to_seconds(time_str: str) -> float:
    """
    Convert a time string "minute:seconds" to seconds.

    Args:
        time_str (str): Time string to convert.

    Returns:
        float: Time in seconds.
    """
    if not isinstance(time_str, str):
        return float(time_str)  # Return as is or convert to float if necessary

    if ":" in time_str:
        minutes, seconds = time_str.split(":")
        return float(minutes) * 60 + float(seconds)

    return float(time_str)


def safe_literal_eval(val: str) -> dict:
    """
    Safely evaluate a string containing a Python literal expression.

    Args:
        val (str): String to be evaluated.

    Returns:
        dict: Evaluated dictionary or an empty dictionary if evaluation fails.
    """
    try:
        return literal_eval(val) if isinstance(val, str) else val
    except (ValueError, SyntaxError) as e:
        logging.error(f"Error evaluating literal for value: {val} - {e}")
        return {}


def expand_json_cols(df: pd.DataFrame, json_cols: list) -> pd.DataFrame:
    """
    Expand JSON k:v pairs to be their own columns in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame that contains the JSON columns to be expanded.
        json_cols (list): List of column names that contain JSON k:v pairs to be expanded.

    Returns:
        pd.DataFrame: DataFrame with the JSON columns expanded and the original dropped.

    Raises:
        ValueError: If any of the JSON columns cannot be parsed.
    """
    other_cols = df.drop(columns=json_cols)

    for col in json_cols:
        logging.info("Expanding column: %s", col)
        try:
            expanded_cols = df[col].apply(safe_literal_eval).apply(pd.Series)
            expanded_cols.columns = [
                f"{col}_{sub_col}" for sub_col in expanded_cols.columns
            ]
            other_cols = pd.concat([other_cols, expanded_cols], axis=1)
        except Exception as e:
            logging.error("Error expanding column %s: %s", col, e)
            raise ValueError(f"Error expanding column {col}: {e}") from e

    return other_cols


def load_and_process_csv(filepath, id_columns, rename_columns=None, fill_na_value=None):
    """
    Generic function to load and process CSV files.
    Args:
        filepath (str): The file path to the CSV file.
        id_columns (list): Columns to be used to create the unique ID.
        rename_columns (dict): Optional dictionary for renaming columns.
        fill_na_value (any): Optional value to fill NaN values.
    Returns:
        pd.DataFrame: Processed DataFrame.
    """
    df = pd.read_csv(filepath)
    df["race_id"] = df[id_columns[0]].astype(str) + "_" + df[id_columns[1]].astype(str)
    if rename_columns:
        df = df.rename(columns=rename_columns)
    if fill_na_value is not None:
        df = df.fillna(fill_na_value)
    df = remove_unnamed_col(df)
    return df


def join_dataframes(df1, df2, join_key):
    """Join two DataFrames on a specified key."""
    return df1.merge(df2, how="left", on=join_key)


def add_previous_year_results(df):
    """
    Adds previous year's average position for each driver to the DataFrame.

    Parameters:
    - df: pandas DataFrame containing race results information.

    Returns:
    - Modified DataFrame with an additional column for the previous year's average position.
    """
    # Create a copy of the DataFrame with the season incremented to match the next year's season
    prev_year_info = df.copy()
    prev_year_info["season"] += 1

    # Group by driverId, circuitId, and season, then calculate the mean position
    prev_year_avg_positions = (
        prev_year_info.groupby(["driverId", "circuitId", "season"])["position"]
        .mean()
        .reset_index()
    )

    # Rename columns to match for merging
    prev_year_avg_positions.rename(columns={"position": "prev_year_pos"}, inplace=True)

    # Merge the modified DataFrame back to the original DataFrame
    df = df.merge(
        prev_year_avg_positions, on=["driverId", "circuitId", "season"], how="left"
    )

    # Fill NaN values with 0 for drivers without a previous year position
    df["prev_year_pos"].fillna(0, inplace=True)

    return df
