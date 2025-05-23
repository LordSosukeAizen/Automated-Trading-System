from confluent_kafka import Consumer

TOPIC = "options_data_stream"

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
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        c.close()
        print("Consumer closed.")

if __name__ == "__main__":
    consume_from_kafka()
