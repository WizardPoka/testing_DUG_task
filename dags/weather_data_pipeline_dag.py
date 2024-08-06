# weather_data_pipeline_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import json
import os
import logging

# Ваш API-ключ
API_KEY = '8f99d8bcfaf61db2c4b7d0eaf46ac6c1'

# Координаты города (например, Лондон)
LAT = '51.5074'
LON = '-0.1278'

# URL для API One Call
API_URL = f'http://api.openweathermap.org/data/2.5/weather?q=London&appid={API_KEY}'

# Пути к файлам
RAW_FILE_PATH = '/tmp/weather_data.json'
PROCESSED_FILE_PATH = '/tmp/processed_weather_data.csv'
PARQUET_FILE_PATH = '/tmp/weather.parquet'

def download_weather_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        
        # Log the response data for debugging
        logging.info(f"Data downloaded: {data}")
        
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
        
        df = pd.DataFrame([weather_data])
        df.to_csv(PROCESSED_FILE_PATH, index=False)
        
        # Log the file save operation
        logging.info(f"Processed data saved to {PROCESSED_FILE_PATH}")
        
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

def save_to_parquet():
    try:
        df = pd.read_csv(PROCESSED_FILE_PATH)
        df.to_parquet(PARQUET_FILE_PATH, index=False)
        
        # Log the file save operation
        logging.info(f"Data saved to {PARQUET_FILE_PATH}")
        
    except Exception as e:
        logging.error(f"Error saving data to Parquet: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval='0 0 * * *',
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
