import asyncio
from websockets.asyncio.server import serve
import json
import websockets
from confluent_kafka import SerializingProducer

KAFKA_TOPIC = "stock-data"
KAFKA_BROKER = "localhost:9092"

# Initialize Kafka Producer
producer = SerializingProducer({
    'bootstrap.servers': KAFKA_BROKER
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

async def handle_websocket(websocket):
    print("WebSocket client connected.")
    try:
        async for message in websocket:
            data = json.loads(message)
            print("Received:", data)

            # Produce to Kafka
            producer.produce(
                KAFKA_TOPIC,
                key=str(data.get("timestamp", "")),
                value=json.dumps(data)
            )
            producer.flush()  # Serve delivery callbacks

    except websockets.ConnectionClosed:
        print("WebSocket client disconnected.")

async def main():
    async with serve(handle_websocket, "0.0.0.0", 8675):
        print("WebSocket server running on ws://0.0.0.0:8675")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
