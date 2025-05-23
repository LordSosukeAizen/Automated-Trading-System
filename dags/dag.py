from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from confluent_kafka import Consumer

TOPIC = "options_data_stream"
POSTGRES_CONN_ID = "postgres"



c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-options-group',
    'auto.offset.reset': 'earliest'
})

c.subscribe([TOPIC])

def consume_from_kafka():
    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))
            return msg.value().decode('utf-8')
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        c.close()
        print("Consumer closed.")
    
    
    

def clean_and_transform():
    ""



def store_to_postgres():
    
    
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
    )
    """
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        return 0
    except Exception as e:
        print("Error creating table:", e)
        return 1
    
    
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




