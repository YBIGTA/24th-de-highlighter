import argparse
import logging
import os

import pafy
import pytchat
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging config
logger = logging.getLogger(__name__)
logging.basicConfig(
    encoding='utf-8',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y/%m/%d %H:%M:%S'
)


def delivery_report(err, msg):
    if err is not None:
        logger.warning(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def get_chat_from_live(video_id: str, producer: Producer):
    api_key = os.environ.get('GCP_KEY')
    pafy.set_api_key(api_key)

    chat = pytchat.create(video_id=video_id)
    while chat.is_alive():
        data = chat.get()
        for c in data.items:
            # send timestamp to kafka
            producer.produce(
                'stream_filter',
                value=c.datetime.encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Streamlink Kafka Producer')
    parser.add_argument('-u', '--url', type=str, required=True, help='URL link to YouTube live')
    args = parser.parse_args()

    # Kafka config
    broker_public_ip = os.getenv('BROKER_IP', 'localhost')
    conf = {
        'bootstrap.servers': f'{broker_public_ip}:9092',
        'security.protocol': 'PLAINTEXT'
    }
    producer = Producer(**conf)

    get_chat_from_live(
        video_id=args.url,
        producer=producer
    )
