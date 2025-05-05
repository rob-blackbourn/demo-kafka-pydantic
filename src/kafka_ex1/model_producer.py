from datetime import datetime
from decimal import Decimal

from kafka import KafkaProducer  # type: ignore[import]

from messages.models import User, Location, Update
from messages.serialization import serialize_model_to_json


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
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    for model in models:
        message = serialize_model_to_json(model)
        print(f"Sending to {topic!r}: {message!r}")
        producer.send(topic, message.encode(), headers=headers)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main('foobar')
