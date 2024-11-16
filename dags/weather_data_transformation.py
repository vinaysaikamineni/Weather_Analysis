from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import Json
from airflow.sensors.external_task import ExternalTaskSensor

def transform_weather_data():
    conn = psycopg2.connect(
        dbname="weather",
        user="vinay",
        password="root",
        host="postgres"
    )
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
        "INSERT INTO transformed_weather_data (date, avg_temperature, avg_humidity, avg_pressure, aggregated_data) VALUES (%s, %s, %s, %s, %s);",
        (datetime.now().date(), avg_temperature, avg_humidity, avg_pressure, Json(aggregated_data))
    )
    
    conn.commit()
    cursor.close()
    conn.close()

# Define the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    'weather_data_transformation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion',
        external_dag_id='weather_data_ingestion',  # Replace with the exact DAG ID
        external_task_id='fetch_weather_data',     # Replace with the exact task ID, if required
        mode='poke',                               # or 'reschedule' depending on your needs
        timeout=600,                               # Set an appropriate timeout
        retry_delay=timedelta(seconds=30),
        retries=3,
    )
    
    transform_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data
    )

    # Set dependency
    wait_for_ingestion >> transform_data
