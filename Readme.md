# Weather Data ETL Pipeline Project

This project is an end-to-end ETL (Extract, Transform, Load) pipeline to fetch, process, and store weather data. The pipeline integrates data ingestion from the OpenWeather API, processes it, and loads it into a PostgreSQL database for further analysis and visualization.

## **Project Overview**

The Weather Data ETL Pipeline consists of the following steps:
1. **Data Ingestion**: Fetches weather data (current, daily, and forecast) from the OpenWeather API.
2. **Data Transformation**: Processes and aggregates the data, including unit conversion (e.g., temperature to Fahrenheit) and calculation of average metrics.
3. **Data Quality Checks**: Ensures the integrity and accuracy of the data through validation rules.
4. **Data Loading**: Stores the raw and transformed data into a PostgreSQL database for future analysis.

---

## **Technologies Used**

### **Data Pipeline**
- **Apache Airflow**: Orchestrates the pipeline, manages DAGs, and automates task execution.
- **Python**: Primary programming language for building the pipeline and implementing ETL logic.

### **Database**
- **PostgreSQL**: Stores raw and transformed weather data.

### **Data Source**
- **OpenWeather API**: Provides real-time, historical, and forecast weather data.

### **Orchestration Environment**
- **Docker**: Containerized the Airflow, PostgreSQL, and supporting services to ensure consistent and portable execution.

---

## **Setup Instructions**

### **Pre-requisites**
1. **Python** (Version 3.7 or higher)
2. **Docker** (Installed and configured)
3. **OpenWeather API Key** (Signup required at [OpenWeather](https://openweathermap.org/))
4. PostgreSQL database credentials and a database created for the project.

---

### **Steps to Run the Pipeline**

1. **Clone the Repository**:
   ```bash
   git clone <repository_url>
   cd <repository_folder>
   ```

2. **Set Up Environment Variables**:
   Create a `.env` file with the following:
   ```bash
   API_KEY=<your_openweather_api_key>
   DB_NAME=weather
   DB_USER=vinay
   DB_PASSWORD=root
   DB_HOST=postgres
   DB_PORT=5432
   ```

3. **Start Docker Environment**:
   Build and start the Docker containers:
   ```bash
   docker-compose up --build
   ```

4. **Access Apache Airflow**:
   Open the Airflow web server at `http://localhost:8090`. Use the default credentials:
   - Username: `airflow`
   - Password: `airflow`

5. **Trigger the Pipeline**:
   Enable and trigger the DAG `weather_data_pipeline` in the Airflow UI.

6. **Check Data in PostgreSQL**:
   Connect to the PostgreSQL instance and validate data ingestion:
   ```sql
   SELECT * FROM raw_weather_data;
   SELECT * FROM transformed_weather_data;
   ```

---

## **Pipeline Components**

### **DAG Structure**
The pipeline is orchestrated using a single DAG, `weather_data_pipeline`, which consists of the following tasks:
1. **Data Ingestion (`fetch_weather_data`)**:
   - Fetches weather data using the OpenWeather API.
   - Stores the raw JSON data in the `raw_weather_data` table.

2. **Data Transformation (`transform_weather_data`)**:
   - Converts temperature from Kelvin to Fahrenheit.
   - Calculates average temperature, humidity, and pressure.
   - Stores transformed data in the `transformed_weather_data` table.

3. **Data Quality Checks (`run_data_quality_checks`)**:
   - Validates data for:
     - Non-empty rows.
     - No null values in key metrics.
     - Sanity checks for temperature, humidity, and pressure ranges.

---

## **Features Implemented**

- **ETL Pipeline**:
  - Extraction using REST API.
  - Transformation using Python with PostgreSQL integration.
  - Loading into PostgreSQL database.

- **Data Validation**:
  - Ensures integrity and accuracy through automated quality checks.

- **Alerts**:
  - Sends email notifications on data quality check failures.

---

## **Next Steps**

### **Phase 2 Goals**
1. Enhance the dataset by increasing the number of API requests (e.g., fetching data for multiple locations).
2. Integrate with visualization tools like **Power BI** for advanced analytics and reporting.
3. Implement data archival and automated backups for PostgreSQL.
4. Explore real-time data ingestion using Apache Kafka.

---

## **Folder Structure**

```
project_root/
│
├── dags/                   # Contains Airflow DAGs
│   ├── weather_data_pipeline.py   # Integrated DAG for ETL pipeline
│
├── docker-compose.yml      # Docker environment configuration
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation
```

---

## **Contact**

For any questions or suggestions, feel free to contact **Vinay Sai Kamineni** at `vinaysaikamineni@gmail.com`.
