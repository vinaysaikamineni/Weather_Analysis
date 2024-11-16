from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json

# OpenWeather API credentials and endpoint
API_KEY = "f640156891b8f1e737b5973e68a22c80"
BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"

# PostgreSQL connection details
DB_CONNECTION = {
    "dbname": "weather",
    "user": "vinay",
    "password": "root",
    "host": "postgres",
    "port": "5432"  # Ensure this matches your container's internal port
}

def fetch_weather_data():
    params = {
        "lat": 39.099724,     # Example latitude for New York
        "lon": -94.578331,    # Example longitude for New York
        "appid": API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    # Connect to PostgreSQL and store raw JSON
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS raw_weather_data (id SERIAL PRIMARY KEY, data JSONB, ingestion_time TIMESTAMP);")
    cursor.execute(
    "INSERT INTO raw_weather_data (data, ingestion_time) VALUES (%s, %s);",
    (json.dumps(data), datetime.now())
    )
    conn.commit()
    cursor.close()
    conn.close()

# Define default args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Create the DAG
with DAG(
    "weather_data_ingestion",
    default_args=default_args,
    description="DAG for ingesting weather data from OpenWeather API",
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:

    # Define the data ingestion task
    ingest_data = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data
    )

    ingest_data
