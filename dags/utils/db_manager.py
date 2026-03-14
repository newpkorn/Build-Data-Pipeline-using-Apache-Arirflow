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

    # Self-Healing: Check if 'id' is AUTO_INCREMENT, fix if not (Prevents Error 1364)
    check_sql = f"SELECT EXTRA, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}' AND COLUMN_NAME='id' AND TABLE_SCHEMA=DATABASE()"
    id_info_df = mysql_hook.get_pandas_df(check_sql)
    
    if not id_info_df.empty:
        extra = id_info_df.iloc[0]['EXTRA']
        col_key = id_info_df.iloc[0]['COLUMN_KEY']
        if 'auto_increment' not in str(extra).lower():
            if col_key == 'PRI':
                mysql_hook.run(f"ALTER TABLE {table_name} MODIFY id INT AUTO_INCREMENT")
            else:
                mysql_hook.run(f"ALTER TABLE {table_name} MODIFY id INT AUTO_INCREMENT PRIMARY KEY")

    # Get existing columns
    existing_cols_df = mysql_hook.get_pandas_df(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = DATABASE()")
    existing_cols = existing_cols_df['COLUMN_NAME'].tolist() if not existing_cols_df.empty else []

    # Add missing columns
    for col in df.columns:
        if col not in existing_cols:
            # A more robust solution would map pandas dtypes to SQL types
            mysql_hook.run(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")

def load_df_to_db(df: pd.DataFrame, table_name: str, conn_id: str):
    """
    Atomically loads a DataFrame into a MySQL table. It deletes old data for the
    same date and inserts new data within a single transaction.
    """
    if df.empty:
        return

    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    engine = mysql_hook.get_sqlalchemy_engine()
    
    # Ensure required columns exist
    if 'rate_date' not in df.columns:
        raise ValueError("DataFrame must contain 'rate_date' column.")

    dates = df["rate_date"].unique()

    # Use a transaction to ensure atomicity of delete and insert
    with engine.begin() as connection:
        from sqlalchemy.sql import text
        for d in dates:
            connection.execute(text(f"DELETE FROM {table_name} WHERE rate_date = :date"), {"date": d})
        
        # Insert new data in the same transaction
        df.to_sql(table_name, connection, if_exists='append', index=False)
