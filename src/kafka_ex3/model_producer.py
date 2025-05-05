from datetime import datetime
from decimal import Decimal

from confluent_kafka import Producer  # type: ignore[import]

from messages.models import User, Location, Update
from messages.serialization import serialize_model_to_json


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main(topic: str) -> None:
    models = [
        User(
            name="Rob",
            date_of_birth=datetime(2000, 1, 1),
            height=1.86
        ),
        Location(
            name="London",
            latitude=Decimal('51.5074'),
            longitude=Decimal('-0.1278'),
        ),
        Update(
            model=User(
                name="Ann-Marie",
                date_of_birth=datetime(1973, 8, 5),
                height=1.64
            )
        ),
    ]

    headers: list[tuple[str, bytes]] = [("content-type", b"application/json")]

    print("Connecting to Kafka...")

    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    for model in models:
        message = serialize_model_to_json(model)
        print(f"Sending to {topic!r}: {message!r}")

        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        producer.produce(
            topic,
            message.encode('utf-8'),
            callback=delivery_report,
            headers=headers
        )

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()


if __name__ == '__main__':
    main('foobar')
