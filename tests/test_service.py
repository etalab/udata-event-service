import time
import uuid

from udata_event_service.consumer import create_kafka_consumer, consume_kafka
from udata_event_service.producer import produce

KAFKA_URI = "localhost:9092"


def test_service():
    # consumer = create_kafka_consumer(
    #     kafka_uri=KAFKA_URI,
    #     group_id=None,
    #     topics=["test.event_service"]
    # )
    start = time.time()
    produce(
        kafka_uri=KAFKA_URI,
        topic="test.event_service",
        service="event_service",
        key_id=uuid.uuid4().__str__(),
        document={
            "doc_int": 1,
            "doc_str": "a",
        }, meta={
            "meta_int": 1,
            "meta_str": "a",
        }
    )
    print("Took {}s".format(time.time() - start))
