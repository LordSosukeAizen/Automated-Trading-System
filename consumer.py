from confluent_kafka import Consumer
import json


TOPIC =  "stock-data"

c = Consumer({
     "bootstrap.servers": "localhost:9092",
        'group.id': 'options-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'isolation.level': 'read_committed'
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

            data = json.loads(msg.value().decode('utf-8'))
            print(f"Consumed: {data}")
            c.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        c.close()
        print("Consumer closed.")

if __name__ == "__main__":
    consume_from_kafka()
