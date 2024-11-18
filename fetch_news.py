from confluent_kafka import Producer
from newsapi import NewsApiClient
import json
import time

# initializing NewsAPI client
newsapi = NewsApiClient(api_key="76f71110459744dcaa5944ec59cc91d8")

# kafka Producer configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch_and_send_news():
    try:
        # getting top headlines
        articles = newsapi.get_top_headlines(language="en")
        for article in articles["articles"]:
            if article["title"]:  # Ensure title exists
                message = {"title": article["title"]}
                # sending to Kafka
                producer.produce("topic1", key=None, value=json.dumps(message), callback=delivery_report)
        producer.flush()
    except Exception as e:
        print(f"Error fetching or sending data: {e}")

# indefinite data streaming to Kafka
if __name__ == "__main__":
    while True:
        fetch_and_send_news()
        # fetching news every 30 sec
        time.sleep(30)
