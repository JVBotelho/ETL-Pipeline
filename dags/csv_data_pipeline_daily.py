import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os

# Define the path to the raw and processed datasets.
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
RAW_DATA_PATH = AIRFLOW_HOME + "/Datasets/raw/flipkart_com-ecommerce_sample.csv"
PROCESSED_DATA_PATH = AIRFLOW_HOME + "/Datasets/processed/processedDataset.csv"

# Define the expected columns.
EXPECTED_COLUMNS = ['uniq_id', 'crawl_timestamp', 'product_url', 'product_name', 'product_category_tree', 'pid',
                    'retail_price', 'discounted_price', 'image', 'is_FK_Advantage_product', 'description',
                    'product_rating', 'overall_rating', 'brand', 'product_specifications']


def on_failure_callback(context):
    """
    A function to handle errors in the DAG.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    error_message = context['exception'].getTraceback()

    # Log the error message to the DAG audit log.
    logging.error(f"DAG {dag_id}, task {task_id}: {error_message}")


# Define the default arguments for the DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,
}

# Create the DAG object.
dag = DAG(
    'csv_data_pipeline_daily_v2.2.1',
    default_args=default_args,
    description=('A DAG that retrieves data from a CSV dataset, normalizes '
                 'and cleans it, and saves it to another CSV dataset daily.'),
    schedule_interval=timedelta(days=1),
)


def retrieve_data(**context):
    try:
        workingData = pd.read_csv(RAW_DATA_PATH, header=0, encoding='unicode_escape', engine='python')
        logging.info('Data retrieved successfully.')
        context['ti'].xcom_push(key='working_data', value=workingData.to_json())
    except Exception as e:
        logging.error(f'Error retrieving data: {str(e)}')
        raise e


def validate_raw_data(**context):
    """
    Check that the raw data contains all expected columns.
    """
    ti = context['ti']
    workingData = pd.read_json(ti.xcom_pull(key='working_data'))
    missing_columns = set(EXPECTED_COLUMNS) - set(workingData.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in raw data: {missing_columns}")


def normalize_data(**context):
    # Perform any data normalization here
    ti = context['ti']
    workingData = pd.read_json(ti.xcom_pull(key='working_data'))
    data = workingData.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    ti.xcom_push(key='working_data', value=data.to_json())


def clean_data(**context):
    # Perform any data cleaning here
    ti = context['ti']
    workingData = pd.read_json(ti.xcom_pull(key='working_data'))
    data = workingData.dropna()
    ti.xcom_push(key='working_data', value=data.to_json())


def drop_duplicates(**context):
    """
    Drop any duplicate rows.
    """
    ti = context['ti']
    workingData = pd.read_json(ti.xcom_pull(key='working_data'))
    workingData.drop_duplicates(inplace=True)
    ti.xcom_push(key='working_data', value=workingData.to_json())


def save_data(**context):
    ti = context['ti']
    workingData = pd.read_json(ti.xcom_pull(key='working_data'),
                               dtype={'uniq_id': str, 'crawl_timestamp': str, 'product_url': str,
                                      'product_name': str, 'product_category_tree': str, 'pid': str, 'image': str,
                                      'description': str, 'brand': str, 'product_specifications': str
                                      })
    validate_processed_data(workingData)  # pass workingData as an argument
    workingData.to_csv(PROCESSED_DATA_PATH, index=False)


def validate_processed_data(data):
    # Check that all expected columns are present
    columns = set(data.columns)
    missing_columns = set()
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
        actual_type = data[col].dtype
        is_object = isinstance(actual_type, object)

        if not is_object:
            if actual_type != expected_type:
                error_msg = (f"Processed dataset has incorrect data type for column {col},"
                             f"Expected {expected_type}, found {actual_type} : {is_object}")
                logging.error(error_msg)
                raise ValueError(error_msg)

    logging.info("Processed dataset validation successful.")


# Define the tasks.
task1 = PythonOperator(
    task_id='retrieve_data',
    python_callable=retrieve_data,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='normalize_data',
    python_callable=normalize_data,
    provide_context=True,
    dag=dag,
)

task4 = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

task5 = PythonOperator(
    task_id='drop_duplicates',
    python_callable=drop_duplicates,
    provide_context=True,
    dag=dag,
)

task6 = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    provide_context=True,
    dag=dag,
)

# Define the dependencies.
task1 >> task2 >> task3 >> task4 >> task5 >> task6
