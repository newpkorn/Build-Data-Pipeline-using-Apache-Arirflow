from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd
import random
import logging
import matplotlib.pyplot as plt
import io
import base64

# --- Configuration ---
MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "customer_sentiment_results"
S3_BUCKET_NAME = "my-data-lake-bucket" # Simulate an S3 bucket for "big data" storage
RAW_REVIEWS_PATH = "/tmp/raw_customer_reviews.csv"
SENTIMENT_DATA_PATH = "/tmp/customer_sentiment_data.parquet"
DASHBOARD_IMAGE_PATH = "/tmp/sentiment_dashboard_{{ ds }}.png" # Dynamic image path

def extract_reviews(**context):
    """
    Simulate extracting a large volume of customer reviews.
    In a real-world scenario, this would connect to a data source like S3, a data lake, or a database.
    For this example, we generate dummy data.
    """
    num_reviews = 100000 # Simulate a large number of reviews
    reviews = []
    products = ["Laptop", "Smartphone", "Headphones", "Smartwatch", "Monitor"]
    sentiments = ["positive", "negative", "neutral"]
    
    for i in range(num_reviews):
        product = random.choice(products)
        sentiment = random.choice(sentiments)
        
        # Generate review text based on sentiment
        if sentiment == "positive":
            review_text = f"This {product} is amazing! Highly recommend it. Great quality and features."
        elif sentiment == "negative":
            review_text = f"Very disappointed with this {product}. It broke quickly and had many issues."
        else:
            review_text = f"The {product} is okay. Nothing special, but it works as expected."
            
        reviews.append({
            "review_id": i + 1,
            "product_name": product,
            "review_text": review_text,
            "rating": random.randint(1, 5),
            "review_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
        })

    df_reviews = pd.DataFrame(reviews)
    df_reviews.to_csv(RAW_REVIEWS_PATH, index=False)
    logging.info(f"Extracted and simulated {num_reviews} reviews to {RAW_REVIEWS_PATH}")

def analyze_sentiment(**context):
    """
    Perform sentiment analysis on customer reviews.
    This is a simplified rule-based sentiment analysis for demonstration.
    In a real-world scenario, this would use more sophisticated NLP models (e.g., NLTK, spaCy, Hugging Face).
    """
    df_reviews = pd.read_csv(RAW_REVIEWS_PATH)
    
    def get_sentiment(text):
        text_lower = text.lower()
        if "great" in text_lower or "amazing" in text_lower or "excellent" in text_lower or "love" in text_lower:
            return "Positive"
        elif "disappointed" in text_lower or "bad" in text_lower or "broken" in text_lower or "hate" in text_lower:
            return "Negative"
        else:
            return "Neutral"

    df_reviews["sentiment"] = df_reviews["review_text"].apply(get_sentiment)
    
    # Select relevant columns for sentiment data
    df_sentiment = df_reviews[["review_id", "product_name", "review_date", "sentiment", "rating"]]
    
    df_sentiment.to_parquet(SENTIMENT_DATA_PATH, index=False)
    logging.info(f"Analyzed sentiment for {len(df_reviews)} reviews and saved to {SENTIMENT_DATA_PATH}")

def load_sentiment_data(**context):
    """
    Load: Save to MySQL with Dynamic Schema
    """
    data_df = pd.read_parquet(SENTIMENT_DATA_PATH)
    if data_df.empty:
        logging.info("No data to load into MySQL")
        return

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Self-Healing: Check if the table has a Primary Key
    cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
    if cursor.fetchone():
        cursor.execute(f"SHOW KEYS FROM {TABLE_NAME} WHERE Key_name = 'PRIMARY'")
        if not cursor.fetchone():
            logging.warning(f"Table {TABLE_NAME} exists but missing Primary Key. Dropping to recreate with correct schema.")
            cursor.execute(f"DROP TABLE {TABLE_NAME}")

    first_row = data_df.iloc[0]
    columns = data_df.columns.tolist()

    def get_sql_type(col_name, value):
        if col_name == 'review_date': return 'DATE'
        if col_name == 'rating': return 'INT'
        if col_name == 'review_id': return 'INT'
        return 'VARCHAR(255)' # Default for strings

    col_defs = [f"{col} {get_sql_type(col, first_row[col])}" for col in columns]
    pk_cols = [c for c in ['review_id'] if c in columns] # Assuming review_id is the primary key
    pk_sql = f", PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""

    create_sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({', '.join(col_defs)}{pk_sql});"
    cursor.execute(create_sql)

    cursor.execute(f"DESCRIBE {TABLE_NAME}")
    existing_columns = {row[0] for row in cursor.fetchall()}
    
    for col in columns:
        if col not in existing_columns:
            alter_sql = f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col} {get_sql_type(col, first_row[col])}"
            cursor.execute(alter_sql)

    cols_str = ", ".join(columns)
    vals_str = ", ".join(["%s"] * len(columns))
    update_clause = ", ".join([f"{col}=VALUES({col})" for col in columns if col not in pk_cols])
    
    sql = f"INSERT INTO {TABLE_NAME} ({cols_str}) VALUES ({vals_str}) ON DUPLICATE KEY UPDATE {update_clause}"
    
    values = data_df.values.tolist()
    cursor.executemany(sql, values)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Loaded {len(data_df)} rows into MySQL table {TABLE_NAME}")

def generate_sentiment_dashboard(**context):
    """
    Generate a bar chart of sentiment distribution and save it as a PNG image.
    """
    df_sentiment = pd.read_parquet(SENTIMENT_DATA_PATH)
    
    if df_sentiment.empty:
        logging.info("No sentiment data to generate dashboard.")
        # Create a dummy image or handle this case as needed for email attachment
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'No Data Available for Dashboard', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
        ax.axis('off')
        plt.savefig(context["ti"].xcom_pull(key="dashboard_image_path"), format="png")
        return

    # Calculate Stats for Email Report
    total_reviews = len(df_sentiment)
    positive_count = len(df_sentiment[df_sentiment['sentiment'] == 'Positive'])
    negative_count = len(df_sentiment[df_sentiment['sentiment'] == 'Negative'])
    neutral_count = len(df_sentiment[df_sentiment['sentiment'] == 'Neutral'])

    sentiment_counts = df_sentiment['sentiment'].value_counts().sort_index()

    fig, ax = plt.subplots(figsize=(10, 6))
    sentiment_counts.plot(kind='bar', ax=ax, color=['green', 'red', 'blue'])
    ax.set_title('Sentiment Distribution of Customer Reviews')
    ax.set_xlabel('Sentiment')
    ax.set_ylabel('Number of Reviews')
    plt.xticks(rotation=0)
    plt.tight_layout()

    # Save image to a temporary file
    image_path = DASHBOARD_IMAGE_PATH.replace("{{ ds }}", context['ds'])
    plt.savefig(image_path, format="png")
    plt.close(fig) # Close the plot to free memory
    logging.info(f"Generated sentiment dashboard image to {image_path}")
    
    # --- Prepare HTML Email Content with Embedded Image ---
    # Encode image to Base64 to display inline in email
    with open(image_path, "rb") as img_file:
        img_base64 = base64.b64encode(img_file.read()).decode('utf-8')

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f4f9; color: #333; }}
            .container {{ max-width: 700px; margin: 0 auto; background: #ffffff; padding: 0; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); overflow: hidden; }}
            .header {{ background-color: #2c3e50; color: #ffffff; padding: 20px; text-align: center; }}
            .header h2 {{ margin: 0; font-size: 24px; }}
            .content {{ padding: 20px; }}
            .stats-grid {{ display: flex; justify-content: space-between; margin-bottom: 20px; text-align: center; gap: 10px; }}
            .stat-box {{ flex: 1; padding: 15px; border-radius: 8px; background-color: #ecf0f1; }}
            .stat-value {{ font-size: 24px; font-weight: bold; display: block; }}
            .stat-label {{ font-size: 14px; color: #7f8c8d; }}
            .pos {{ color: #27ae60; }}
            .neg {{ color: #c0392b; }}
            .neu {{ color: #2980b9; }}
            .chart-container {{ text-align: center; margin-top: 20px; border-top: 1px solid #eee; padding-top: 20px; }}
            .footer {{ background-color: #f8f9fa; padding: 15px; text-align: center; font-size: 12px; color: #aaa; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>📢 Customer Sentiment Report</h2>
                <p style="margin: 5px 0 0 0; opacity: 0.8;">Date: {context['ds']}</p>
            </div>
            <div class="content">
                <p>Hello Team,</p>
                <p>Here is the daily summary of customer reviews analysis. The data has been processed and loaded into the database.</p>
                
                <div class="stats-grid">
                    <div class="stat-box"><span class="stat-value pos">{positive_count}</span><span class="stat-label">Positive 😄</span></div>
                    <div class="stat-box"><span class="stat-value neg">{negative_count}</span><span class="stat-label">Negative 😠</span></div>
                    <div class="stat-box"><span class="stat-value neu">{neutral_count}</span><span class="stat-label">Neutral 😐</span></div>
                </div>
                
                <p style="text-align: center; color: #555;"><strong>Total Reviews Processed: {total_reviews:,}</strong></p>

                <div class="chart-container">
                    <h3>📊 Sentiment Distribution</h3>
                    <img src="data:image/png;base64,{img_base64}" alt="Sentiment Chart" style="width: 100%; max-width: 600px; height: auto; border: 1px solid #ddd; border-radius: 4px;">
                </div>
            </div>
            <div class="footer">
                <p>Generated by Apache Airflow Pipeline • {datetime.now().year}</p>
            </div>
        </div>
    </body>
    </html>
    """

    context["ti"].xcom_push(key="dashboard_image_path", value=image_path)
    context["ti"].xcom_push(key="dashboard_html", value=html_content)

# --- DAG Definition ---
default_args = {
    "owner": "dataeng",
    "start_date": datetime(2023, 1, 1), # Start date in the past
    "email": ["non-reply@example.com"], # Change to your email
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer_review_sentiment_pipeline",
    schedule_interval="0 0 * * *", # Run once daily at midnight UTC
    catchup=False,
    default_args=default_args,
    tags=["big_data", "sentiment_analysis", "customer_reviews", "dashboard"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customer_reviews",
        python_callable=extract_reviews,
        provide_context=True
    )

    analyze_task = PythonOperator(
        task_id="analyze_review_sentiment",
        python_callable=analyze_sentiment,
        provide_context=True
    )

    load_to_db_task = PythonOperator(
        task_id="load_sentiment_results_to_mysql",
        python_callable=load_sentiment_data,
        provide_context=True
    )

    generate_dashboard_task = PythonOperator(
        task_id="generate_sentiment_dashboard",
        python_callable=generate_sentiment_dashboard,
        provide_context=True
    )

    send_email_report = EmailOperator(
        task_id="send_email_with_dashboard",
        to=["customer_sentiment_report@example.com"], # Change to your email
        subject="✅ Customer Review Sentiment Pipeline Report ({{ ds }})",
        html_content="{{ ti.xcom_pull(task_ids='generate_sentiment_dashboard', key='dashboard_html') }}",
        files=["{{ ti.xcom_pull(task_ids='generate_sentiment_dashboard', key='dashboard_image_path') }}"]
    )

    extract_task >> analyze_task >> load_to_db_task >> generate_dashboard_task >> send_email_report
