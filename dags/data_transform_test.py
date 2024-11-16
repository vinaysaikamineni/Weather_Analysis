from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
from psycopg2.extras import Json

def transform_test_weather_data():
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
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'weather_data_transformation_test',  # Updated unique dag_id
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    transform_data = PythonOperator(
        task_id='transform_test_weather_data',
        python_callable=transform_test_weather_data
    )

transform_data
