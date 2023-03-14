import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Define the path to the raw and processed datasets.
RAW_DATA_PATH = "../Datasets/raw/flipkart_com-ecommerce_sample.csv"
PROCESSED_DATA_PATH = "../Datasets/processed/processed_dataset.csv"

# Define the expected columns.
EXPECTED_COLUMNS = ['uniq_id', 'crawl_timestamp', 'product_url', 'product_name', 'product_category_tree', 'pid', 'retail_price', 'discounted_price', 'image', 'is_FK_Advantage_product', 'description', 'product_rating', 'overall_rating', 'brand', 'product_specifications']

# Define the default arguments for the DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG object.
dag = DAG(
    'csv_data_pipeline_daily',
    default_args=default_args,
    description='A DAG that retrieves data from a CSV dataset, normalizes and cleans it, and saves it to another CSV dataset daily.',
    schedule_interval=timedelta(days=1),
)

# Define the functions to retrieve, normalize, and clean the data.
def retrieve_data():
    logging.info("Retrieving data from: %s", RAW_DATA_PATH)
    data = pd.read_csv(RAW_DATA_PATH)
    return data

def normalize_data(data):
    logging.info("Normalizing data")
    # Perform any data normalization here
    normalized_data = data.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    return normalized_data

def clean_data(data):
    logging.info("Cleaning data")
    # Perform any data cleaning here
    cleaned_data = data.dropna()
    return cleaned_data

def save_data(data):
    logging.info("Saving data to: %s", PROCESSED_DATA_PATH)
    data.to_csv(PROCESSED_DATA_PATH, index=False)

# Define the tasks.
task1 = PythonOperator(
    task_id='retrieve_data',
    python_callable=retrieve_data,
    dag=dag,
)

task2 = PythonOperator(
    task_id='normalize_data',
    python_callable=normalize_data,
    dag=dag,
)

task3 = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

task4 = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    dag=dag,
)

# Define the dependencies.
task1 >> task2 >> task3 >> task4
