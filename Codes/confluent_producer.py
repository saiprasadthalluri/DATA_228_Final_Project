import json
from confluent_kafka import Producer
import requests
import time

# Confluent Kafka configuration with credentials
conf = {
    'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'VA6DXSX6K225JI2U',  # Replace with your Confluent Kafka API key
    'sasl.password': 'RoRa3tYHOMTFJuj3gUI8V49FNKyk68y/MZSpxXsbHW8oMPQ0RxrhMe54t9MCE8PS'  # Replace with your Confluent Kafka secret
}

producer = Producer(conf)

# Function to fetch data from CoinGecko API
def fetch_coin_data():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,
        'page': 1,
        'sparkline': 'false',
        'x_cg_demo_api_key': 'CG-UQwdLdQFYQ3P4JBapi6sPTsZ'
    }
    response = requests.get(url, params=params)
    
    # Ensure response is valid JSON
    try:
        return response.json()  # Parse JSON response
    except json.JSONDecodeError as e:
        print("Failed to parse API response as JSON:", e)
        print("Response content:", response.text)
        return []

# Producer loop to stream data to Kafka
while True:
    coin_data = fetch_coin_data()
    print("Fetched Coin Data:", coin_data)  # Debug: Print the API response

    # Iterate over the data and produce messages
    for coin in coin_data:
        try:
            # Ensure 'coin' is a dictionary and has the expected keys
            if isinstance(coin, dict) and 'id' in coin:
                producer.produce('coingecko', key=str(coin['id']), value=json.dumps(coin))
                print(f"Produced record to topic 'coingecko': {coin['id']}")
            else:
                print(f"Skipping invalid coin data: {coin}")
        except Exception as e:
            print(f"Error producing record for coin: {coin}. Error: {e}")
    producer.flush()
    time.sleep(60)  # Fetch data every 60 seconds
