import json
import logging
import os
from typing import Callable, List, Union

from kafka import KafkaConsumer

KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "2.5.0")


def create_kafka_consumer(topics: Union[str, List[str]]) -> KafkaConsumer:
    logging.info("Creating Kafka Consumer...")
    consumer = KafkaConsumer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        group_id="datalake",
        reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
        # API Version is needed in order to prevent api version guessing leading to an error
        # on startup if Kafka Broker isn't ready yet
        api_version=tuple(
            [int(value) for value in KAFKA_API_VERSION.split(".")]
        ),
    )
    consumer.subscribe(topics)
    logging.info("Kafka Consumer created")
    return consumer


def consume_kafka(
    topics: Union[str, List[str]],
    message_processing_func: Callable[[dict], None],
) -> None:
    consumer = create_kafka_consumer(topics)
    logging.info("Ready to consume messages")
    for message in consumer:
        val_utf8 = message.value.decode("utf-8").replace("NaN", "null")
        data = json.loads(val_utf8)
        message_processing_func(data)
