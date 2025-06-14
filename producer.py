# from confluent_kafka import Producer
# import json
# import time
# from faker import Faker
# from polygon import RESTClient
# from secrets import API_KEY

# import asyncio
# import websockets
# import json

# api_key = API_KEY
# client = RESTClient(api_key=api_key)

# fake = Faker()

# topic = "options_data_stream"
# producer = SerializingProducer({
#      'bootstrap.servers': 'localhost:9092'
     
# }
# )


# def get_options_data(ticker, from_date, to_date):
    
#     try:
#         aggs = []
#         for a in client.list_aggs(ticker=ticker, 
#                                   multiplier=1,
#                                   timespan="minute",
#                                   from_=from_date,
#                                   to=to_date,
#                                 limit=50000):
#             aggs.append(a)
#         print("Successfully fetched")

#     except Exception as e:
#         print("Error occured", e)
    
#     return aggs



# def convert_agg_to_dict(data):
#     return {
#         "open": data.open,
#         "close": data.close,
#         "low": data.low,
#         "volume": data.volume,
#         "vwap": data.vwap,
#         "timestamp": data.timestamp,
#         "transactions": data.transactions
#     }
    
#     """"""
# data = get_options_data(ticker="AAPL", from_date="2023-01-01", to_date="2024-01-01")

# dataLEN = len(data)

# i = 1
# while i <= dataLEN:
#     options_data = convert_agg_to_dict(data[i-1])

#     producer.produce(
#         topic=topic,
#         key=str(options_data['timestamp']),
#         value=json.dumps(options_data)
#     )
#     producer.flush()
#     print(options_data)
#     i+=1
#     time.sleep(5)

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
