import zipfile
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
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
    schedule='@daily',
)


# Task 1.3: Create a task to download data
def unzip_data(**kwargs):
    zip_file_path = '/Users/chimdikeironkwe/Desktop/pythonProject/apache/tolldata.tgz'
    output_folder = 'output_folder'

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # Extract all the contents into the output folder
        zip_ref.extractall(output_folder)

    return output_folder


unzip_data_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    provide_context=True,
    dag=dag,
)


# Task 1.4: Create a task to extract data from CSV file
def extract_data_from_csv(**kwargs):
    input_csv_path = '/Users/chimdikeironkwe/Desktop/pythonProject/apache/vehicle-data.csv'
    output_csv_path = 'csv_data.csv'

    df = pd.read_csv(input_csv_path, usecols=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'])

    df.to_csv(output_csv_path, index=False)

    return output_csv_path


extract_data_from_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    provide_context=True,
    dag=dag,
)


# Task 1.5: Create a task to extract data from TSV file
def extract_data_from_tsv(**kwargs):
    input_tsv_path = '/Users/chimdikeironkwe/Desktop/pythonProject/apache/tollplaza-data.tsv'
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


# Task 1.6: Create a task to extract data from fixed-width file
def extract_data_from_fixed_width(**kwargs):
    input_fixed_width_path = '/Users/chimdikeironkwe/Desktop/pythonProject/apache/payment-data.txt'
    output_csv_path = 'fixed_width_data.csv'

    col_specs = [(41, 45), (45, 54)]  # Adjust these positions based on your actual fixed-width format

    # Read the fixed-width file and extract the required columns
    df = pd.read_fwf(input_fixed_width_path, colspecs=col_specs, header=None,
                     names=['Type of Payment code', 'Vehicle Code'])

    df.to_csv(output_csv_path, index=False)

    return output_csv_path


extract_data_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    provide_context=True,
    dag=dag,
)


# Task 1.7: Create a task to consolidate data
def consolidate_data(**kwargs):
    csv_data_path = 'csv_data.csv'
    tsv_data_path = 'tsv_data.csv'
    fixed_width_data_path = 'fixed_width_data.csv'

    csv_data = pd.read_csv(csv_data_path)
    tsv_data = pd.read_csv(tsv_data_path)
    fixed_width_data = pd.read_csv(fixed_width_data_path)

    # Consolidate data based on common columns
    consolidated_data = pd.concat([csv_data, tsv_data, fixed_width_data], axis=1)

    # Define the order of fields in the final CSV file
    field_order = [
        'Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type',
        'Number of axles', 'Tollplaza id', 'Tollplaza code',
        'Type of Payment code', 'Vehicle Code'
    ]

    consolidated_data = consolidated_data[field_order]

    # Save consolidated data to a new CSV file
    output_csv_path = 'extracted_data.csv'
    consolidated_data.to_csv(output_csv_path, index=False)

    # You can perform further processing or return information for downstream tasks
    return output_csv_path


consolidate_data_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    provide_context=True,
    dag=dag,
)


# task 1.8
def transform_and_load_data(**kwargs):
    consolidated_data_path = kwargs['ti'].xcom_pull(task_ids='consolidate_data')
    consolidated_data = pd.read_csv(consolidated_data_path)

    consolidated_data['Vehicle type'] = consolidated_data['Vehicle type'].str.upper()

    # Save transformed data to a new CSV file
    transformed_data_path = 'transformed_data.csv'
    consolidated_data.to_csv(transformed_data_path, index=False)

    return transformed_data_path


transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_and_load_data,
    provide_context=True,
    dag=dag,
)

unzip_data_task >> extract_data_from_csv_task
unzip_data_task >> extract_data_from_tsv_task
unzip_data_task >> extract_data_from_fixed_width_task
extract_data_from_csv_task >> consolidate_data_task
extract_data_from_tsv_task >> consolidate_data_task
extract_data_from_fixed_width_task >> consolidate_data_task
consolidate_data_task >> transform_data_task
