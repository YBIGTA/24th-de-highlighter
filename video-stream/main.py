"""
Read stream, send from EC2 to SQS.
"""
import argparse
import logging
import os
import time

import streamlink
from confluent_kafka import Producer


# Logging config
logger = logging.getLogger(__name__)
logging.basicConfig(
    encoding='utf-8',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y/%m/%d %H:%M:%S'
)

# Kafka config
broker_public_ip = os.getenv('BROKER_IP', 'localhost')
conf = {
    'bootstrap.servers': f'{broker_public_ip}:9092',
    'security.protocol': 'PLAINTEXT'
}

producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        logger.warning(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def get_stream(url: str, duration: int, buf_size: int):
    streams = streamlink.streams(url)
    logger.info(f'Streams found: {streams.keys()}')
    fd = streams['best'].open()

    now = time.time()
    while time.time() - now < duration:
        data = fd.read(buf_size)
        producer.produce(
            'mytopic',
            data,
            callback=delivery_report
        )
        producer.poll(0)
    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Streamlink Kafka Producer')
    parser.add_argument('-u', '--url', type=str, required=True, help='URL link to YouTube live')
    parser.add_argument('-t', '--time', type=int, required=True, help='Time duration to capture streams')
    parser.add_argument('-b', '--bufsize', type=int, required=True, help='Buffer size')
    args = parser.parse_args()

    url = args.url
    duration = args.time
    bufsize = args.bufsize

    get_stream(
        url=url,
        duration=duration,
        buf_size=bufsize
    )
