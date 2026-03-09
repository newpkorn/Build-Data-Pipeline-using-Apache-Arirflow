from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

def update_schema(df: pd.DataFrame, table_name: str, conn_id: str):
    """
    Dynamically creates or alters a MySQL table to match the DataFrame schema.
    """
    if df.empty:
        return

    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    
    # Create table if it doesn't exist
    mysql_hook.run(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY)")

    # Get existing columns
    existing_cols_df = mysql_hook.get_pandas_df(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
    existing_cols = existing_cols_df['COLUMN_NAME'].tolist()

    # Add missing columns
    for col in df.columns:
        if col not in existing_cols:
            # A more robust solution would map pandas dtypes to SQL types
            mysql_hook.run(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")

def load_df_to_db(df: pd.DataFrame, table_name: str, conn_id: str):
    """
    Loads a DataFrame into a MySQL table, deleting old data for the same date first.
    """
    if df.empty:
        return

    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    
    # Ensure required columns exist
    if 'rate_date' not in df.columns:
        raise ValueError("DataFrame must contain 'rate_date' column.")

    # Delete old data to ensure idempotency
    dates = df["rate_date"].unique()
    with mysql_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for d in dates:
                cursor.execute(f"DELETE FROM {table_name} WHERE rate_date = %s", (d,))

    # Use pandas to_sql for efficient bulk insertion
    # This requires sqlalchemy to be installed
    engine = mysql_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='append', index=False)