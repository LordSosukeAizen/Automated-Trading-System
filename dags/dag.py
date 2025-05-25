from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
# from confluent_kafka import Consumer
import json
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator



TOPIC = "options_data_stream"
POSTGRES_CONN_ID = "postgres"


def process_kafka_message(message, **context):
    decoded_msg = message.value().decode('utf-8')
    print("Processed:", decoded_msg)
    context['ti'].xcom_push(key='raw_message', value=decoded_msg)
    
    
def clean_and_transform(**context):
    raw = context['ti'].xcom_pull(key='raw_message', task_ids='consume_options_data')
    if not raw:
        return None

    data = json.loads(raw)
    
    transformed = (
        data['open'],
        data['close'],
        data['low'],
        data['volume'],
        data['vwap'],
        data['timestamp'],
        data['transactions']
    )
    
    context['ti'].xcom_push(key='cleaned_data', value=transformed)

def store_to_postgres(**context):
    data = context['ti'].xcom_pull(key='cleaned_data', task_ids='clean_and_transform')
    if not data:
        print("No data to store.")
        return

    query = """
    INSERT INTO options_data (open, close, low, volume, vwap, timestamp, transactions)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query, data)
    conn.commit()
    print("Inserted to PostgreSQL.")

def archive_to_s3():
    # You can integrate boto3 here to push to S3
    print("Archived to S3 (placeholder).")

with DAG("options_data_pipeline",
         start_date=datetime(2024, 1, 1),
         schedule="* * * * *",
         catchup=False) as dag:

    query = """
    CREATE TABLE IF NOT EXISTS options_data (
        id VARCHAR(255) PRIMARY KEY,
        open DECIMAL,
        close DECIMAL,
        low DECIMAL,
        volume DECIMAL,
        vwap DECIMAL,
        timestamp DECIMAL,
        transactions DECIMAL
    );
    """
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_stream_data_table",
        conn_id=POSTGRES_CONN_ID,
        sql=query
    )

    consume = ConsumeFromTopicOperator(
    task_id="consume_options_data",
    kafka_config_id="option_stream",
    topics=[TOPIC],
    apply_function=process_kafka_message,
    poll_timeout=5,

)

    clean = PythonOperator(
        task_id="clean_and_transform",
        python_callable=clean_and_transform,        
    )

    to_postgres = PythonOperator(
        task_id="store_to_postgres",
        python_callable=store_to_postgres,
    )

    to_s3 = PythonOperator(
        task_id="archive_to_s3",
        python_callable=archive_to_s3
    )

    create_table >> consume >> clean >> [to_postgres, to_s3]