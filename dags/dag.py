import logging
import json
import functools
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

consumer_logger = logging.getLogger("airflow")

KAFKA_TOPIC = "stock-data"
KAFKA_CONFIG_ID = "TEST"
POSTGRES_CONN_ID = "postgres"

# Modified consumer function to handle both message and context
def consumer_function(message, ti):
    """
    This function consumes data from the producer.
    """
    decoded_msg = message.value().decode("utf-8")
    consumer_logger.info("Kafka Msg: %s - %s", message.key(), decoded_msg)
    ti.xcom_push(key="key", value=decoded_msg)
    

# Clean and transform
def clean_and_transform(**context):
    """
    Transforms decoded JSON string to tuple.
    """
    messages = context['ti'].xcom_pull(task_ids="consume_from_topic", key="key")
    print("Decoded Message ---", decoded_msg)
    
    if not messages:
        raise ValueError("No messages found from Kafka.")

    decoded_msg = messages[0]  

    try:
        msg = json.loads(decoded_msg)

        transformed = (
            msg["open"],
            msg["close"],
            msg["low"],
            msg["high"],
            msg["volume"],
            msg["vwap"],
            msg["timestamp"],
            msg["transactions"],
        )
        
        return transformed

    except Exception as e:
        raise ValueError(f"Invalid message: {decoded_msg} — {e}")


# store to postgres
def store_to_postgres(**context):
    """
    Stores cleaned data into PostgreSQL.
    """
    transformed_data = context['ti'].xcom_pull(task_ids="clean_and_transform")
    if not transformed_data:
        print("No data to store.")
        return

    query = """
    INSERT INTO stocks_data (open, close, low, high, volume, vwap, timestamp, transactions)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query, transformed_data)
    conn.commit()
    print("Inserted to PostgreSQL.")

with DAG("consume_kafka_msgs",
         schedule="@hourly") as dag:
    
    # Create table if not exist
    query = """
    CREATE TABLE IF NOT EXISTS stocks_data (
        id SERIAL PRIMARY KEY,
        open DECIMAL,
        close DECIMAL,
        low DECIMAL,
        high DECIMAL,
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
    
    consumer = ConsumeFromTopicOperator(
        kafka_config_id=KAFKA_CONFIG_ID,
        task_id="consume_from_topic",
        topics=[KAFKA_TOPIC],
        apply_function=consumer_function,
        apply_function_kwargs={'ti' : '{{ti}}' },
        max_messages=1,
        max_batch_size=1
    )

    clean = PythonOperator(
        task_id="clean_and_transform",
        python_callable=clean_and_transform,    
  
    )
    
    to_postgres = PythonOperator(
        task_id="store_to_postgres",
        python_callable=store_to_postgres,

    )
    
    create_table >> consumer >> clean >> to_postgres