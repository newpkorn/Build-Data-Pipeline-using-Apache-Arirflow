import pandas as pd


def check_nulls(df: pd.DataFrame) -> bool:
    """
    Return True if no null values exist.
    """
    return not df.isnull().values.any()


def check_duplicates(df: pd.DataFrame) -> bool:
    """
    Return True if no duplicate rows exist.
    """
    return not df.duplicated().any()