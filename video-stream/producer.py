import logging
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
conf = {
    'bootstrap.servers': "127.0.0.1:9092",
    'security.protocol': 'PLAINTEXT'
}

producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        logger.warning(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

for i in range(1000):
    producer.poll(0)
    producer.produce('mytopic', f'data {i}'.encode('utf-8'), callback=delivery_report)
producer.flush()
