def sync_schema(cursor, table, df):

    cursor.execute(f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME='{table}'
    """)

    existing = [c[0] for c in cursor.fetchall()]

    for col in df.columns:

        if col not in existing:

            cursor.execute(
                f"ALTER TABLE {table} ADD COLUMN {col} TEXT"
            )