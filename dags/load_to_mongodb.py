import json
import pandas as pd
from datetime import datetime
from airflow.sdk import dag, task, Asset
from airflow.providers.mongo.hooks.mongo import MongoHook

PROCESSED_ASSET = Asset("file:///tmp/processed_data.csv")
RECORDS_PATH = "/tmp/processed_records.json"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[PROCESSED_ASSET],
    catchup=False,
    tags=["etl", "consumer"]
)
def mongodb_loader_dag():

    @task(task_id="csv_to_records")
    def csv_to_records():
        df = pd.read_csv("/tmp/processed_data.csv")
        records = df.to_dict('records')
        with open(RECORDS_PATH, 'w') as f:
            json.dump(records, f)
        print(f"Converted {len(records)} records from CSV.")

    @task(task_id="load_to_mongo")
    def load_to_mongo():
        with open(RECORDS_PATH, 'r') as f:
            records = json.load(f)

        hook = MongoHook(mongo_conn_id="mongo_default")
        
        client = hook.get_conn()
        db = client.get_database("sensor_db")
        collection = db.get_collection("sensor_data")
        
        if records:
            collection.insert_many(records)
            print(f"Inserted {len(records)} records into MongoDB.")
        else:
            print("No records to insert.")

    csv_to_records() >> load_to_mongo()

mongodb_loader_dag()