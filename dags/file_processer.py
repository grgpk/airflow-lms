import os
import re
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task, task_group
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset

PROCESSED_ASSET = Asset("file:///tmp/processed_data.csv")

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

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_FILE_PATH,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=600
    )

    @task.branch(task_id="check_file_empty")
    def check_file_empty():
        if os.path.exists(INPUT_FILE_PATH) and os.path.getsize(INPUT_FILE_PATH) > 0:
            df = pd.read_csv(INPUT_FILE_PATH)
            if not df.empty:
                return "processing_tasks.replace_nulls"
        return "empty_file_alert"

    empty_file_alert = BashOperator(
        task_id="empty_file_alert",
        bash_command=f"echo 'The file at {INPUT_FILE_PATH} is empty!'"
    )

    @task_group(group_id="processing_tasks")
    def data_processing_group():
        
        @task(task_id="replace_nulls")
        def replace_nulls():
            df = pd.read_csv(INPUT_FILE_PATH)
            df.replace(["null", pd.NA], "-", inplace=True)
            df.fillna("-", inplace=True)
            df.to_csv(INTERMEDIATE_NULLS_PATH, index=False)

        @task(task_id="sort_data")
        def sort_data():
            df = pd.read_csv(INTERMEDIATE_NULLS_PATH)
            df['at'] = pd.to_datetime(df['at'])
            df.sort_values(by="at", inplace=True)
            df.to_csv(INTERMEDIATE_SORTED_PATH, index=False)

        @task(task_id="clean_content", outlets=[PROCESSED_ASSET])
        def clean_content():
            df = pd.read_csv(INTERMEDIATE_SORTED_PATH)
            clean_regex = r'[^\w\s.,;:!?\'"()-]'
            df['content'] = df['content'].apply(lambda x: re.sub(clean_regex, '', str(x)))
            
            df.to_csv(PROCESSED_FILE_PATH, index=False)
            print(f"Data saved to {PROCESSED_FILE_PATH}")

        replace_nulls() >> sort_data() >> clean_content()

    branch_task = check_file_empty()
    wait_for_file >> branch_task
    branch_task >> empty_file_alert
    branch_task >> data_processing_group()

file_sensor_processor_dag()