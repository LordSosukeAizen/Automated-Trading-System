from confluent_kafka import SerializingProducer
import json
import time
from faker import Faker
from polygon import RESTClient
from secrets import API_KEY


api_key = API_KEY
client = RESTClient(api_key=api_key)

fake = Faker()

topic = "options_data_stream"
producer = SerializingProducer({
     'bootstrap.servers': 'localhost:9092'
     
}
)


def get_options_data(ticker, from_date, to_date):
    
    try:
        aggs = []
        for a in client.list_aggs(ticker=ticker, 
                                  multiplier=1,
                                  timespan="minute",
                                  from_=from_date,
                                  to=to_date,
                                limit=50000):
            aggs.append(a)
        print("Successfully fetched")

    except Exception as e:
        print("Error occured", e)
    
    return aggs



def convert_agg_to_dict(data):
    return {
        "id": fake.uuid4(),
        "open": data.open,
        "close": data.close,
        "low": data.low,
        "volume": data.volume,
        "vwap": data.vwap,
        "timestamp": data.timestamp,
        "transactions": data.transactions
    }
    
    """"""
data = get_options_data(ticker="AAPL", from_date="2023-01-01", to_date="2024-01-01")

dataLEN = len(data)

i = 1
while i <= dataLEN:
    options_data = convert_agg_to_dict(data[i-1])

    producer.produce(
        topic=topic,
        key=options_data['id'],
        value=json.dumps(options_data)
    )
    producer.flush()
    print(options_data)
    i+=1
    time.sleep(5)

    