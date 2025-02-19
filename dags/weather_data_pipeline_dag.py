from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import json
import os
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# ==================================================================================================

API_URL = "http://api.openweathermap.org/data/2.5/weather?q=London&appid=8f99d8bcfaf61db2c4b7d0eaf46ac6c1"

# ==================================================================================================

# Пути к файлам
HOME_DIR = os.path.expanduser("~")
WEATHER_DATA_DIR = os.path.join(HOME_DIR, "weather_data")

RAW_FILE_PATH = os.path.join(WEATHER_DATA_DIR, 'weather_data.json')
PROCESSED_FILE_PATH = os.path.join(WEATHER_DATA_DIR, 'processed_weather_data.csv')
PARQUET_FILE_PATH = os.path.join(WEATHER_DATA_DIR, 'weather.parquet')

# ==================================================================================================

def download_weather_data():
    try:
        session = requests.Session()
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Для решения ошибки requests.exceptions.ConnectionError
        headers = requests.utils.default_headers()
        headers.update(
            {
                'User-Agent': 'My User Agent 1.0',
            }
        )

        response = session.get(API_URL, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        
        # Log the response data for debugging
        logging.info(f"Data downloaded: {data}")
        
        # Log the current working directory
        logging.info(f"Current working directory: {os.getcwd()}")
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(RAW_FILE_PATH), exist_ok=True)
        
        # Log the full path where the file will be saved
        logging.info(f"Saving data to {RAW_FILE_PATH}")
        
        with open(RAW_FILE_PATH, 'w') as f:
            json.dump(data, f)
        
        # Log the file save operation
        logging.info(f"Data saved to {RAW_FILE_PATH}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading data: {e}")
        raise

    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

# ==================================================================================================

def process_weather_data():
    try:
        with open(RAW_FILE_PATH, 'r') as f:
            data = json.load(f)
        
        main = data.get('main', {})
        weather_data = {
            'temp_celsius': main.get('temp') - 273.15 if main.get('temp') is not None else None,
            'pressure': main.get('pressure'),
            'humidity': main.get('humidity')
        }
        
        # Log the processed data for debugging
        logging.info(f"Processed weather data: {weather_data}")
        
        df = pd.DataFrame(weather_data, index=[0])
        logging.info(f"Processed weather data: {df}")

        df.to_csv(PROCESSED_FILE_PATH, index=False)
        
        # Log the file save operation
        logging.info(f"Processed data saved to {PROCESSED_FILE_PATH}")

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

# ==================================================================================================

def save_to_parquet():
    try:
        df = pd.read_csv(PROCESSED_FILE_PATH)
        df.to_parquet(PARQUET_FILE_PATH, index=False)

        # Log the file save operation
        logging.info(f"Data saved to {PARQUET_FILE_PATH}")
    except Exception as e:
        logging.error(f"Error saving data to Parquet: {e}")
        raise

# ==================================================================================================

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# ==================================================================================================

with DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval='0 0 * * *', # Этот DAG будет выполняться ежедневно в полночь
) as dag:

    download_task = PythonOperator(
        task_id='download_weather_data',
        python_callable=download_weather_data
    )

    process_task = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data
    )

    save_parquet_task = PythonOperator(
        task_id='save_to_parquet',
        python_callable=save_to_parquet
    )

    download_task >> process_task >> save_parquet_task

# ==================================================================================================
