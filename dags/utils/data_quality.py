import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

def check_nulls(df: pd.DataFrame) -> bool:
    """
    Check if DataFrame contains any null values.
    Returns True if no nulls, False otherwise.
    """
    if df.isnull().values.any():
        logging.warning("Data quality check failed: Null values found.")
        return False
    return True

def check_duplicates(df: pd.DataFrame) -> bool:
    """
    Check if DataFrame contains duplicate rows.
    Returns True if no duplicates, False otherwise.
    """
    if df.duplicated().any():
        logging.warning("Data quality check failed: Duplicate rows found.")
        return False
    return True

def check_null_rates_for_date(table_name: str, conn_id: str, date: str):
    """
    Connects to DB, fetches data for a specific date, and checks if 'rate' column contains any null values.
    Raises ValueError if null rates are found.
    """
    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    sql = f"SELECT rate FROM {table_name} WHERE rate_date = %s"
    df = mysql_hook.get_pandas_df(sql, parameters=(date,))

    if df.empty:
        logging.warning(f"Data quality check: No data found for date {date}.")
        return  # No data to check

    if "rate" in df.columns and df["rate"].isnull().any():
        logging.error("Data quality check failed: Null rates found.")
        raise ValueError("Data quality check failed: Null rates found in the database for the given date.")
    
    logging.info("Data quality check passed: No null rates found.")