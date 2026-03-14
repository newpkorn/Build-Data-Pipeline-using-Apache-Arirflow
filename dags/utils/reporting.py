from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

def create_csv_report(output_path: str, table_name: str, conn_id: str):
    """
    Exports data from a MySQL table to a CSV file.
    """
    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    df = mysql_hook.get_pandas_df(f"SELECT * FROM {table_name} ORDER BY rate_date DESC")
    df.to_csv(output_path, index=False)
    return output_path

def generate_html_summary(table_name: str, conn_id: str, date: str) -> str:
    """
    Generates an HTML summary of exchange rates for major currencies.
    """
    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    sql = f"SELECT currency_code, rate FROM {table_name} WHERE rate_date = %s"
    df = mysql_hook.get_pandas_df(sql, parameters=(date,))

    if df.empty:
        return "<p>No data available for this date.</p>"

    df['rate'] = pd.to_numeric(df['rate'], errors='coerce')
    
    major_currencies = ['USD', 'EUR', 'JPY', 'GBP', 'CNY', 'SGD']
    preview_df = df[df['currency_code'].isin(major_currencies)].sort_values('currency_code')

    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; max-width: 500px; }}
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h2 style="color: #2c3e50;">📊 Global Exchange Rates Report</h2>
        <p><strong>Date:</strong> {date}</p>
        <p>Here are the rates for major currencies (Base: THB):</p>
        <table>
            <tr><th>Currency</th><th>Rate</th></tr>
    """
    for _, row in preview_df.iterrows():
        html_content += f"<tr><td>{row['currency_code']}</td><td>{row['rate']:.4f}</td></tr>"
    
    html_content += """
        </table>
        <p>Please find the complete data in the attached CSV file.</p>
    </body>
    </html>
    """
    return html_content

def generate_html_summary_from_df(df: pd.DataFrame, date: str) -> str:
    """
    Generates an HTML summary of exchange rates for major currencies from a DataFrame.
    """
    if df.empty:
        return "<p>No data available for this date.</p>"

    df['rate'] = pd.to_numeric(df['rate'], errors='coerce')
    
    major_currencies = ['USD', 'EUR', 'JPY', 'GBP', 'CNY', 'SGD']
    preview_df = df[df['currency_code'].isin(major_currencies)].sort_values('currency_code')

    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; max-width: 500px; }}
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h2 style="color: #2c3e50;">📊 Global Exchange Rates Report</h2>
        <p><strong>Date:</strong> {date}</p>
        <p>Here are the rates for major currencies (Base: THB):</p>
        <table>
            <tr><th>Currency</th><th>Rate</th></tr>
    """
    for _, row in preview_df.iterrows():
        html_content += f"<tr><td>{row['currency_code']}</td><td>{row['rate']:.4f}</td></tr>"
    
    html_content += """
        </table>
        <p>Please find the complete data in the attached CSV file.</p>
    </body>
    </html>
    """
    return html_content