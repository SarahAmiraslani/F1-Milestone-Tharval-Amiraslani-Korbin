"""Utility functions for data analysis in Jupyter notebooks."""

# Imports
from ast import literal_eval
import logging
import re
from typing import Dict, List, Union
import pandas as pd
import unicodedata
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def format_race_data(race_df: pd.DataFrame) -> pd.DataFrame:
    """Format the date column to 'YYYY-MM-DD' for the weather API."""
    return race_df[
        ["date", "season", "raceName", "circuitId", "circuit", "lat", "long"]
    ].assign(date=lambda x: pd.to_datetime(x["date"]))


def remove_unnamed_col(df: pd.DataFrame) -> pd.DataFrame:
    """Remove columns with 'Unnamed' in their name."""
    return df.loc[:, ~df.columns.str.contains("^Unnamed")]


def convert_to_seconds(time_str: Union[str, float]) -> float:
    """Convert a time string "minute:seconds" to seconds."""
    if not isinstance(time_str, str):
        return float(time_str)
    return sum(float(x) * 60**i for i, x in enumerate(reversed(time_str.split(":"))))


def safe_literal_eval(val: Union[str, dict]) -> dict:
    """Safely evaluate a string containing a Python literal expression."""
    if not isinstance(val, str):
        return val
    try:
        return literal_eval(val)
    except (ValueError, SyntaxError) as e:
        logging.error("Error evaluating literal for value: %s - %s", val, e)
        return {}


def expand_json_cols(df: pd.DataFrame, json_cols: List[str]) -> pd.DataFrame:
    """Expand JSON k:v pairs to be their own columns in the DataFrame."""
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


def load_and_process_csv(
    filepath: str,
    id_columns: List[str],
    rename_columns: Dict[str, str] = None,
    fill_na_value: Union[str, float, int] = None,
) -> pd.DataFrame:
    """Load and process CSV files."""
    df = pd.read_csv(filepath)
    df["race_id"] = df[id_columns[0]].astype(str) + "_" + df[id_columns[1]].astype(str)
    if rename_columns:
        df = df.rename(columns=rename_columns)
    if fill_na_value is not None:
        df = df.fillna(fill_na_value)
    return remove_unnamed_col(df)


def join_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, join_key: str
) -> pd.DataFrame:
    """Join two DataFrames on a specified key."""
    return df1.merge(df2, how="left", on=join_key)


def add_previous_year_results(df: pd.DataFrame) -> pd.DataFrame:
    """Add previous year's average position for each driver to the DataFrame."""
    prev_year_info = df.copy()
    prev_year_info["season"] += 1
    prev_year_avg_positions = (
        prev_year_info.groupby(["driverId", "circuitId", "season"])["position"]
        .mean()
        .reset_index()
        .rename(columns={"position": "prev_year_pos"})
    )
    return df.merge(
        prev_year_avg_positions, on=["driverId", "circuitId", "season"], how="left"
    ).fillna({"prev_year_pos": 0})


def load_ergast_csv(file_path: str, description: str = "data") -> pd.DataFrame:
    """
    Load any Ergast CSV file into a DataFrame.

    Args:
    file_path (str): Path to the CSV file containing Ergast data.
    description (str): Description of the data being loaded (e.g., "circuits", "races"). Default is "data".

    Returns:
    pd.DataFrame: DataFrame containing the loaded data.

    Raises:
    FileNotFoundError: If the specified file does not exist.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} {description} entries from {file_path}")
    return df


def inspect_dataframe(df: pd.DataFrame, n_rows: int = 5) -> None:
    """Perform initial inspection of the DataFrame."""
    print(f"\nDataFrame Shape: {df.shape}")
    print("\nColumn Names:", df.columns.tolist())
    print("\nData Types:\n", df.dtypes)
    print(f"\nFirst {n_rows} rows:\n", df.head(n_rows))
    print(f"\nLast {n_rows} rows:\n", df.tail(n_rows))
    print("\nSummary Statistics:\n", df.describe())
    print("\nMissing Values:\n", df.isnull().sum())


def remove_accents(text: str) -> str:
    """Remove accents and diacritical marks from a string."""
    nfkd_form = unicodedata.normalize("NFKD", text)
    only_ascii = "".join(c for c in nfkd_form if not unicodedata.combining(c))
    accent_replacements = {
        "á": "a",
        "à": "a",
        "ã": "a",
        "â": "a",
        "ä": "a",
        "é": "e",
        "è": "e",
        "ê": "e",
        "ë": "e",
        "í": "i",
        "ì": "i",
        "î": "i",
        "ï": "i",
        "ó": "o",
        "ò": "o",
        "õ": "o",
        "ô": "o",
        "ö": "o",
        "ú": "u",
        "ù": "u",
        "û": "u",
        "ü": "u",
        "ý": "y",
        "ÿ": "y",
        "ñ": "n",
        "ç": "c",
    }
    return re.sub(
        "|".join(map(re.escape, accent_replacements.keys())),
        lambda m: accent_replacements[m.group()],
        only_ascii,
    )


def extract_years(date_range: Union[str, float]) -> List[int]:
    """
    Extract years from a given date range string.

    Args:
    date_range (str or float): A string representing a range of years or seasons, or a float (for NaN values).

    Returns:
    List[int]: A list of integers representing all years in the range.
    """
    if pd.isna(date_range):
        return []

    if not isinstance(date_range, str):
        return []

    years = []
    clean_range = re.sub(r"\[.*?\]", "", date_range)
    parts = [part.strip() for part in clean_range.split(",")]

    for part in parts:
        if "–" in part:
            start, end = part.split("–")
            PATTERN = r"\d{4}"
            start_year = int(re.search(PATTERN, start).group())
            end_year = int(re.search(PATTERN, end).group())
            years.extend(range(start_year, end_year + 1))
        else:
            year = int(re.search(PATTERN, part).group())
            years.append(year)

    return sorted(set(years))


if __name__ == "__main__":
    logging.info("Utilities module loaded successfully.")
