import pandas as pd
from ast import literal_eval
import logging

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
        if isinstance(val, str):
            return literal_eval(val)
        return val
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
        logging.info(f"Expanding column: {col}")
        try:
            expanded_cols = (
                df[col].apply(lambda x: safe_literal_eval(x)).apply(pd.Series)
            )
            expanded_cols.columns = [
                f"{col}_{sub_col}" for sub_col in expanded_cols.columns
            ]
            other_cols = pd.concat([other_cols, expanded_cols], axis=1)
        except Exception as e:
            logging.error(f"Error expanding column {col}: {e}")
            raise ValueError(f"Error expanding column {col}: {e}")

    return other_cols
