from airflow.providers.mysql.hooks.mysql import MySqlHook

def check_null_rates_for_date(table_name: str, conn_id: str, date: str):
    """
    Checks for null rates in a specific table for a given date.
    Raises a ValueError if nulls are found.
    """
    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    
    sql = f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE rate IS NULL AND rate_date = %s
    """
    
    records = mysql_hook.get_records(sql, parameters=(date,))

    if records and records[0][0] > 0:
        raise ValueError(f"Data Quality Check Failed: Found {records[0][0]} null rates for date {date}.")

    return True