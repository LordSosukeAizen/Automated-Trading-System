from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator


def consume_from_kafka():
    ""

def clean_and_transform():
    ""

def store_to_postgres():
    ""
    
def archive_to_s3():
    ""




with DAG("options_data_pipeline",
         start_date=datetime(2024, 1, 1),
         schedule_interval="@hourly",
         catchup=False) as dag:

    consume = PythonOperator(task_id="consume_from_kafka", python_callable=consume_from_kafka)
    clean = PythonOperator(task_id="clean_and_transform", python_callable=clean_and_transform)
    to_postgres = PythonOperator(task_id="store_to_postgres", python_callable=store_to_postgres)
    to_s3 = PythonOperator(task_id="archive_to_s3", python_callable=archive_to_s3)

    consume >> clean >> [to_postgres, to_s3]




