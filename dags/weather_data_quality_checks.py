from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import psycopg2

def data_quality_checks():
    conn = psycopg2.connect(
        dbname="weather",
        user="vinay",
        password="root",
        host="postgres"
    )
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

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Airflow DAG for Data Quality Checks
with DAG(
    'weather_data_quality_checks',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # Wait for the transformation DAG to complete
    wait_for_transformation = ExternalTaskSensor(
        task_id='wait_for_transformation',
        external_dag_id='weather_data_transformation',  # Ensure this matches the transformation DAG's ID
        external_task_id='transform_weather_data',      # Ensure this matches the transformation task ID
        mode='poke',
        timeout=600,
        retry_delay=timedelta(seconds=30),
        retries=3,
    )
    
    # Data quality check task
    run_data_quality_checks = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=data_quality_checks
    )

    # Set task dependencies
    wait_for_transformation >> run_data_quality_checks
