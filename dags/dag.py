import json
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.sdk import task, dag
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from research.agent import Agent, Finance
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Input
from tensorflow.keras.optimizers import Adam
import numpy as np


model_file_path = "model.keras"

opt = Adam(learning_rate=0.001)
NUM_FEATURES = 10
NUM_ACTIONS = 2


KAFKA_TOPIC = ["stock-data"]
KAFKA_CONFIG_ID = "TEST"
POSTGRES_CONN_ID = "postgres"


def build_model(num_features, num_actions):
        # Model
    model = Sequential()
    model.add(Input(shape=(num_features,)))
    model.add(Dense(24, activation="relu"))
    model.add(Dense(24, activation="relu"))
    model.add(Dense(num_actions, activation="linear"))
    model.compile(loss="mse", optimizer=opt)
    return model
@dag(
 schedule="@hourly"   
)
def consume_kafka_msgs():
    
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
    
    
    # Consumer Using Consumer Hook
    @task
    def consumer_from_topic(BATCH_SIZE=10, MAX_MESSAGES=100):
        """
        This Task Consumes from Kafka Topic
        """
        consumer = KafkaConsumerHook(
            topics=KAFKA_TOPIC,
            kafka_config_id=KAFKA_CONFIG_ID
        ).get_consumer()

        if not consumer:
            raise AirflowException("Kafka connection failed. Check config and topic.")

        try:
            msg_collector = []
            remaining = MAX_MESSAGES

            while remaining > 0:
                msgs = consumer.consume(num_messages=BATCH_SIZE)
                print("Messages -- ", msgs)
                for msg in msgs:
                    val = msg.value().decode("utf-8")
                    print(f"Received: {msg.key()} --- {val}")
                    msg_collector.append(val)
                remaining -= min(len(msgs), remaining)

            consumer.close()
            print("Consumer closed.")

            return msg_collector 

        except Exception as e:
            raise AirflowException(f"Kafka consumption error: {e}")

            
    @task
    def clean_and_transform(messages):
            """
            Transforms decoded JSON string to tuple.
            """

            transformed_messages = []
            for message in messages:
                print("Decoded Message ---", message)
                if not message:
                    raise ValueError("No messages found from Kafka.")

                decoded_msg = message
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
                    
                    transformed_messages.append(transformed)

            
                except Exception as e:
                    raise ValueError(f"Invalid message: {decoded_msg} — {e}")

            return transformed_messages
            
    @task
    def signal_generation(transformed_data):
        """
    Generates buy/sell signals using pre-trained model.
        """

        model = build_model(num_features=NUM_FEATURES, num_actions=NUM_ACTIONS)
        try:
            model.load_weights(model_file_path)
        except Exception as e:
            raise AirflowException(f"Failed to load model weights: {e}")
  
        if len(transformed_data) < NUM_FEATURES:
            raise ValueError("Not enough data to form state")

        # Only take closing prices
        data_ = transformed_data[-NUM_FEATURES:]
        current_state = [closing_price for _, closing_price, *_ in data_]
        current_state = np.array(current_state).reshape(1, -1)
        current_state = (current_state - np.mean(current_state)) / np.std(current_state)

        best_action = np.argmax(model.predict(current_state, verbose=0)[0])
        
        print(f'{best_action} is best action for current state')
        
        if best_action == 1:
            print("🔼 Buy Signal Generated")
        else:
            print("🔽 Sell Signal Generated")
    
    
    @task
    def store_to_postgres(transformed_data):
        """
        Stores cleaned data into PostgreSQL.
        """
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
        try:
            cursor.executemany(query, transformed_data)
            conn.commit()
            print(f"Inserted {len(transformed_data)} rows into PostgreSQL.")
        except Exception as e:
            conn.rollback()
            print("Failed to insert data:", e)
            raise
        finally:
            cursor.close()
            conn.close()


    consume = consumer_from_topic()
    clean = clean_and_transform(consume)
    load_data_to_db = store_to_postgres(clean)
    signal = signal_generation(clean)
    create_table >> consume >> clean >> load_data_to_db >> signal
    
    
dag = consume_kafka_msgs()