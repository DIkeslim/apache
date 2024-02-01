from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


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
download_task = BashOperator(
    task_id='download_data_task',
    bash_command='your_download_script.sh',  # Replace with your actual download script or command
    dag=dag,
)

# Task 1.4: Create a task to extract data from CSV file
extract_csv_task = BashOperator(
    task_id='extract_csv_task',
    bash_command='your_extract_csv_script.sh',  # Replace with your actual extract CSV script or command
    dag=dag,
)

# Task 1.5: Create a task to extract data from TSV file
extract_tsv_task = BashOperator(
    task_id='extract_tsv_task',
    bash_command='your_extract_tsv_script.sh',  # Replace with your actual extract TSV script or command
    dag=dag,
)

# Define the order of tasks
download_task >> extract_csv_task
download_task >> extract_tsv_task
