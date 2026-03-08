import os
import re
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task, task_group
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset

# Define the Asset that links the two DAGs
PROCESSED_ASSET = Asset("file:///tmp/processed_data.csv")

# Constants for file paths
INPUT_FILE_PATH = "/tmp/input_data.csv"
PROCESSED_FILE_PATH = "/tmp/processed_data.csv"
INTERMEDIATE_NULLS_PATH = "/tmp/stage_nulls_replaced.csv"
INTERMEDIATE_SORTED_PATH = "/tmp/stage_sorted.csv"

@dag(
    start_date=datetime(2024, 3, 8),
    schedule=None,
    catchup=False,
    tags=["etl", "producer"]
)
def file_sensor_processor_dag():

    # 1. Sensor: Waits for the file to appear
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_FILE_PATH,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=600
    )

    # 2. Branching: Check if file is empty
    @task.branch(task_id="check_file_empty")
    def check_file_empty():
        # Check if file exists and has data (greater than just headers)
        if os.path.exists(INPUT_FILE_PATH) and os.path.getsize(INPUT_FILE_PATH) > 0:
            df = pd.read_csv(INPUT_FILE_PATH)
            if not df.empty:
                return "processing_tasks.replace_nulls"
        return "empty_file_alert"

    # 3. Bash Task: If file is empty
    empty_file_alert = BashOperator(
        task_id="empty_file_alert",
        bash_command=f"echo 'The file at {INPUT_FILE_PATH} is empty!'"
    )

    # 4. TaskGroup: Data Processing
    @task_group(group_id="processing_tasks")
    def data_processing_group():
        
        @task(task_id="replace_nulls")
        def replace_nulls():
            df = pd.read_csv(INPUT_FILE_PATH)
            # Replace literal "null" strings or actual NaNs with "-"
            df.replace(["null", pd.NA], "-", inplace=True)
            df.fillna("-", inplace=True)
            df.to_csv(INTERMEDIATE_NULLS_PATH, index=False)

        @task(task_id="sort_data")
        def sort_data():
            df = pd.read_csv(INTERMEDIATE_NULLS_PATH)
            df['at'] = pd.to_datetime(df['at'])
            df.sort_values(by="at", inplace=True)
            df.to_csv(INTERMEDIATE_SORTED_PATH, index=False)

        # The outlets parameter makes this task update the Asset when it succeeds
        @task(task_id="clean_content", outlets=[PROCESSED_ASSET])
        def clean_content():
            df = pd.read_csv(INTERMEDIATE_SORTED_PATH)
            # Regex to keep only alphanumeric characters, spaces, and punctuation
            # This strips out emojis and special symbols
            clean_regex = r'[^\w\s.,;:!?\'"()-]'
            df['content'] = df['content'].apply(lambda x: re.sub(clean_regex, '', str(x)))
            
            # Save the final dataset
            df.to_csv(PROCESSED_FILE_PATH, index=False)
            print(f"Data saved to {PROCESSED_FILE_PATH}")

        # Define dependencies within the TaskGroup
        replace_nulls() >> sort_data() >> clean_content()

    # Define DAG-level dependencies
    branch_task = check_file_empty()
    wait_for_file >> branch_task
    branch_task >> empty_file_alert
    branch_task >> data_processing_group()

# Instantiate the DAG
file_sensor_processor_dag()