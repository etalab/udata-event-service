import json
import logging
import os

from kafka import KafkaProducer
from kafka.errors import KafkaError


class KafkaProducerSingleton:
    __instance = None

    @staticmethod
    def get_instance(kafka_uri: str) -> KafkaProducer:
        if KafkaProducerSingleton.__instance is None:
            kafka_api_version = os.environ.get("KAFKA_API_VERSION", "2.5.0")
            KafkaProducerSingleton.__instance = KafkaProducer(
                bootstrap_servers=kafka_uri,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=tuple(
                    [int(value) for value in kafka_api_version.split(".")]
                ),
            )
        return KafkaProducerSingleton.__instance


def produce(
    kafka_uri: str,
    topic: str,
    service: str,
    key_id: str,
    document: dict = None,
    meta: dict = None,
) -> None:
    if kafka_uri:
        print("---start---")
        producer = KafkaProducerSingleton.get_instance(kafka_uri)
        print("---1---")
        key = key_id.encode("utf-8")
        print("---2---")
        value = {"service": service, "value": document, "meta": meta}
        print("---3---")
        r = producer.send(topic=topic, value=value, key=key)
        print("---4---")
        producer.flush()
        print("---stop---")
        if not r.succeeded():
            raise KafkaError("Message couldn't be produced")
    else:
        logging.warning("No kafka_uri provided")
