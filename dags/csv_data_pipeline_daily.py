import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Define the path to the raw and processed datasets.
RAW_DATA_PATH = "../Datasets/raw/flipkart_com-ecommerce_sample.csv"
PROCESSED_DATA_PATH = "../Datasets/processed/processed_dataset.csv"

# Define the expected columns.
EXPECTED_COLUMNS = ['uniq_id', 'crawl_timestamp', 'product_url', 'product_name', 'product_category_tree', 'pid',
                    'retail_price', 'discounted_price', 'image', 'is_FK_Advantage_product', 'description',
                    'product_rating', 'overall_rating', 'brand', 'product_specifications']

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
    description=('A DAG that retrieves data from a CSV dataset, normalizes '
                 'and cleans it, and saves it to another CSV dataset daily.'),
    schedule_interval=timedelta(days=1),
)


# Define the functions to retrieve,normalize, and clean the data.
def retrieve_data():
    data = pd.read_csv(RAW_DATA_PATH)
    return data


def normalize_data(data):
    # Perform any data normalization here
    normalized_data = data.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    return normalized_data


def clean_data(data):
    # Perform any data cleaning here
    cleaned_data = data.dropna()
    return cleaned_data


def save_data(data):
    validate_processed_data(data)
    data.to_csv(PROCESSED_DATA_PATH, index=False)


def validate_processed_data(data):
    # Check that all expected columns are present
    columns = set(data.columns)
    if set(EXPECTED_COLUMNS) != columns:
        missing_columns = set(EXPECTED_COLUMNS) - columns
        extra_columns = columns - set(EXPECTED_COLUMNS)
        error_msg = (
            f"Processed dataset is missing expected columns {missing_columns} "
            f"and has extra columns {extra_columns}"
        )
        logging.error(error_msg)
        raise ValueError(error_msg)

    # Check that there are no null values in the expected columns
    null_values = data[EXPECTED_COLUMNS].isnull().sum().sum()
    if null_values > 0:
        error_msg = f"Processed dataset has {null_values} null values in expected columns"
        logging.error(error_msg)
        raise ValueError(error_msg)

    # Check that the data types of the expected columns are correct
    expected_types = {'uniq_id': str, 'crawl_timestamp': str, 'product_url': str, 'product_name': str,
                      'product_category_tree': str, 'pid': str, 'retail_price': float, 'discounted_price': float,
                      'image': str, 'is_FK_Advantage_product': int, 'description': str, 'product_rating': float,
                      'overall_rating': float, 'brand': str, 'product_specifications': str}
    for col, expected_type in expected_types.items():
        if data[col].dtype != expected_type:
            error_msg = f"Processed dataset has incorrect data type for column {col}"
            logging.error(error_msg)
            raise ValueError(error_msg)

    logging.info("Processed dataset validation successful.")


def validate_raw_data(data):
    """
    Check that the raw data contains all expected columns.
    """
    missing_columns = set(EXPECTED_COLUMNS) - set(data.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in raw data: {missing_columns}")


def drop_duplicates(data):
    """
    Drop any duplicate rows.
    """
    data.drop_duplicates(inplace=True)
    return data


# Define the tasks.
task1 = PythonOperator(
    task_id='retrieve_data',
    python_callable=retrieve_data,
    dag=dag,
)

task2 = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data,
    op_kwargs={'data': retrieve_data()},
    dag=dag,
)

task3 = PythonOperator(
    task_id='normalize_data',
    python_callable=normalize_data,
    op_kwargs={'data': retrieve_data()},
    dag=dag,
)

task4 = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    op_kwargs={'data': normalize_data(retrieve_data())},
    dag=dag,
)

task5 = PythonOperator(
    task_id='drop_duplicates',
    python_callable=drop_duplicates,
    op_kwargs={'data': clean_data(retrieve_data())},
    dag=dag,
)

task6 = PythonOperator(
    task_id='validate_processed_data',
    python_callable=validate_processed_data,
    op_kwargs={'data': clean_data(retrieve_data())},
    dag=dag,
)

task7 = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    op_kwargs={'data': clean_data(retrieve_data())},
    dag=dag,
)

# Define the dependencies.
task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7
