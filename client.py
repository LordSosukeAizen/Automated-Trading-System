from websockets.sync.client import connect
from secret import API_KEY
from polygon import RESTClient
import time
import json

api_key = API_KEY
client = RESTClient(api_key=api_key)

URI = "ws://localhost:8675"  

def get_options_data(ticker, from_date, to_date):
    try:
        aggs = list(client.list_aggs(ticker=ticker, 
                                     multiplier=1,
                                     timespan="day",
                                     from_=from_date,
                                     to=to_date,
                                     limit=50000))
        print(f"Fetched {len(aggs)} data points for {ticker}")
        return aggs
    except Exception as e:
        print("Error occurred:", e)
        return []

def convert_agg_to_dict(data):
    return {
        "open": data.open,
        "close": data.close,
        "low": data.low,
        "high": data.high,
        "volume": data.volume,
        "vwap": data.vwap,
        "timestamp": data.timestamp,
        "transactions": data.transactions
    }

def stream_data(data_list):
    with connect(URI) as websocket:
        for d in data_list:
            try:
                packet = convert_agg_to_dict(d)
                websocket.send(json.dumps(packet))
                print("Sent:", packet)
                time.sleep(5) 
            except Exception as e:
                print("Error sending data:", e)


if __name__ == "__main__":
    data = get_options_data("AAPL", "2023-01-01", "2024-01-02")  
    if data:
        stream_data(data)
