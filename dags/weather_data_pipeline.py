from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import Json
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
    "port": "5432"
}

def fetch_weather_data():
    params = {
        "lat": 39.099724,     # Example latitude
        "lon": -94.578331,    # Example longitude
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

def transform_weather_data():
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()
    
    # Read raw data from the database
    cursor.execute("SELECT data FROM raw_weather_data;")
    raw_data = cursor.fetchall()

    # Initialize lists to store values
    temperatures = []
    humidities = []
    pressures = []
    aggregated_data = []

    for row in raw_data:
        data = row[0]
        
        # Access current weather data
        if 'current' in data:
            current_data = data['current']
            temperatures.append(current_data['temp'])
            humidities.append(current_data['humidity'])
            pressures.append(current_data['pressure'])
        
        # Access daily weather data
        if 'daily' in data:
            for daily_data in data['daily']:
                temperatures.append(daily_data['temp']['day'])
                humidities.append(daily_data['humidity'])
                pressures.append(daily_data['pressure'])
                aggregated_data.append(daily_data)  # Store each day’s data

    # Calculate averages
    avg_temperature = sum(temperatures) / len(temperatures) if temperatures else None
    avg_humidity = sum(humidities) / len(humidities) if humidities else None
    avg_pressure = sum(pressures) / len(pressures) if pressures else None
    
    # Insert transformed data into transformed_weather_data table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS transformed_weather_data (date DATE, avg_temperature FLOAT, avg_humidity FLOAT, avg_pressure FLOAT, aggregated_data JSONB);"
    )
    cursor.execute(
        "INSERT INTO transformed_weather_data (date, avg_temperature, avg_humidity, avg_pressure, aggregated_data) VALUES (%s, %s, %s, %s, %s);",
        (datetime.now().date(), avg_temperature, avg_humidity, avg_pressure, Json(aggregated_data))
    )
    
    conn.commit()
    cursor.close()
    conn.close()

def data_quality_checks():
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()

    # Check 1: Row Count
    cursor.execute("SELECT COUNT(*) FROM transformed_weather_data;")
    row_count = cursor.fetchone()[0]
    if row_count == 0:
        raise ValueError("Data quality check failed: No rows in transformed_weather_data table.")

    # Check 2: Null Value Check
    cursor.execute("""
        SELECT COUNT(*)
        FROM transformed_weather_data
        WHERE avg_temperature IS NULL OR avg_humidity IS NULL OR avg_pressure IS NULL;
    """)
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        raise ValueError(f"Data quality check failed: Found {null_count} rows with null values.")

    # Check 3: Sanity Check for Temperature, Humidity, and Pressure
    cursor.execute("""
        SELECT COUNT(*)
        FROM transformed_weather_data
        WHERE avg_temperature < -50 OR avg_temperature > 60
           OR avg_humidity < 0 OR avg_humidity > 100
           OR avg_pressure < 800 OR avg_pressure > 1100;
    """)
    sanity_issues = cursor.fetchone()[0]
    if sanity_issues > 0:
        raise ValueError(f"Data quality check failed: Found {sanity_issues} rows with unrealistic values.")

    cursor.close()
    conn.close()

def send_failure_email(context):
    subject = "Airflow Alert: Data Quality Check Failed"
    body = f"Task {context['task_instance_key_str']} in DAG {context['dag'].dag_id} failed."
    send_email("vinaysaikamineni@gmail.com", subject, body)

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
    "weather_data_pipeline",
    default_args=default_args,
    description="Integrated DAG for Weather Data Pipeline",
    schedule_interval='@daily',
    catchup=False,
) as dag:

    ingest_data = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data
    )

    transform_data = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data
    )

    quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=data_quality_checks,
        retries=0,  # No retries for this task
        on_failure_callback=send_failure_email  # Send email on failure
    )

    # Set task dependencies
    ingest_data >> transform_data >> quality_checks
