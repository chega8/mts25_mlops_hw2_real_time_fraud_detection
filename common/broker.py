import json
import logging
import os
import signal
from confluent_kafka import Consumer, Producer, KafkaError

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

def producer():
    return Producer({'bootstrap.servers': BOOTSTRAP, 'enable.idempotence': True,
                     'acks': 'all', 'delivery.timeout.ms': 30000})


def publish(client, topic, value, key=None):
    delivered = []
    metadata = []
    client.produce(topic, key=key, value=json.dumps(value, allow_nan=False).encode(),
                   on_delivery=lambda error, message: (delivered.append(error), metadata.append(message)))
    pending = client.flush(35)
    if pending or not delivered or delivered[0] is not None:
        raise RuntimeError(f'Kafka delivery failed: {delivered}')
    return metadata[0]


def consume(topic, group, handler):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    client = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': group,
                       'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
                       'enable.auto.offset.store': False})
    dlq = producer()
    running = True
    def stop(*args):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    client.subscribe([topic])
    try:
        while running:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())
            try:
                def invalid_constant(value):
                    raise ValueError(f'Invalid JSON constant: {value}')
                value = json.loads(message.value(), parse_constant=invalid_constant)
                handler(value)
            except (ValueError, TypeError, KeyError, OverflowError) as error:
                logging.warning('Invalid message at %s/%s/%s: %s', topic, message.partition(), message.offset(), error)
                publish(dlq, topic + '_dlq', {'source_topic': topic,
                    'source_partition': message.partition(), 'source_offset': message.offset(),
                    'error': str(error)})
            client.commit(message=message, asynchronous=False)
    finally:
        client.close()
