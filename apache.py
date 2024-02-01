import zipfile
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Task 1.1: Define DAG arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1.2: Define the DAG
dag = DAG(
    'toll_plaza_data_pipeline',
    default_args=default_args,
    description='A DAG to collect and process toll plaza data',
    schedule_interval='@daily',  # You can adjust this based on your requirements
)


# Task 1.3: Create a task to download data
def unzip_data(**kwargs):
    zip_file_path = '/Users/chimdikeironkwe/Desktop/pythonProject/apache/tolldata.tgz'
    output_folder = 'output_folder'

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # Extract all the contents into the output folder
        zip_ref.extractall(output_folder)

    return output_folder


# Task 1.4: Create a task to extract data from CSV file
def extract_data_from_csv(**kwargs):
    input_csv_path = 'vehicle-data.csv'
    output_csv_path = 'csv_data.csv'

    # Read the CSV file and extract the required columns
    df = pd.read_csv(input_csv_path, usecols=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'])

    # Save the extracted data to a new CSV file
    df.to_csv(output_csv_path, index=False)

    return output_csv_path


# Task 1.5: Create a task to extract data from TSV file
def extract_data_from_tsv(**kwargs):
    input_tsv_path = 'tollplaza-data.tsv'
    output_csv_path = 'tsv_data.csv'

    df = pd.read_csv(input_tsv_path, delimiter='\t', usecols=['Number of axles', 'Tollplaza id', 'Tollplaza code'])

    df.to_csv(output_csv_path, index=False)

    return output_csv_path


extract_data_from_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    provide_context=True,
    dag=dag,
)
