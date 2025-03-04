import sys
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DAGS_FOLDER)
sys.path.append(os.path.join(DAGS_FOLDER, "libraries"))

from libraries.data_extraction import extract_data
from libraries.data_transforming import data_transform
from libraries.data_loading import load_data


@dag(schedule="0 8 * * *", start_date=days_ago(1), catchup=False, tags=["PsychoBunny", "S3", "Airflow"])
def psychobunny_dag():

    @task
    def extract_data_task(file_type: str):
        df = extract_data(s3_prefix="airflow/psychobunny/", file_type=file_type)
        return df

    @task
    def transform_data_task(df, file_type: str):
        df_transformed = data_transform(df, file_type=file_type)
        return df_transformed

    @task
    def load_data_task(df, table_name: str):
        load_data(df, table_name=table_name)

    file_types = ["customers", "transactions"]

    for file_type in file_types:
        extract_task = extract_data_task.override(task_id=f"extract_{file_type}_data")(file_type=file_type)
        transform_task = transform_data_task.override(task_id=f"transform_{file_type}_data")(df=extract_task, file_type=file_type)
        load_task = load_data_task.override(task_id=f"load_{file_type}_data")(df=transform_task, table_name=file_type)

dag = psychobunny_dag()
