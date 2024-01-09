# processors/transform.py

import pandas as pd
from pandas import json_normalize

def flatten_json(dataframe, column_to_flatten):
    """
    Flattens a column with nested JSON structures in a DataFrame.

    Parameters:
    - dataframe: The DataFrame containing the nested JSON.
    - column_to_flatten: The column name containing the nested JSON.

    Returns:
    - A DataFrame with the nested JSON column flattened.
    """
    # Initialize an empty DataFrame for the flattened data
    flattened_data = pd.DataFrame()

    for item in dataframe[column_to_flatten]:
        # Normalize and flatten the item, which could be a dict or a list
        if isinstance(item, dict):
            flattened = json_normalize(item)
        elif isinstance(item, list) and item:
            flattened = json_normalize(item)
        else:
            flattened = pd.DataFrame()

        # Concatenate the flattened item to the flattened_data DataFrame
        flattened_data = pd.concat([flattened_data, flattened], ignore_index=True)

    # Generate new column names
    flattened_data.columns = [f"{column_to_flatten}_{subcolumn}" for subcolumn in flattened_data.columns]

    # Merge the flattened data with the original DataFrame
    return dataframe.drop(column_to_flatten, axis=1).join(flattened_data)

def dynamically_flatten_dataframes(dataframe):
    """
    Dynamically identifies and flattens columns in a DataFrame
    that contain nested structures like dictionaries or lists.

    Parameters:
    - dataframe: The DataFrame to process.

    Returns:
    - A DataFrame with nested structures flattened.
    """
    for column in dataframe.columns:
        # Check the first non-null value in the column
        first_value = dataframe[column].dropna().iloc[0]

        if isinstance(first_value, (dict, list)):
            dataframe = flatten_json(dataframe, column)

    return dataframe


def sanitize_column_names(record):
    """Sanitize column names by replacing spaces with underscores."""
    return {key.replace(' ', '_'): value for key, value in record.items()}
