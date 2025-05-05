import asyncio
from datetime import datetime
from decimal import Decimal

from aiokafka import AIOKafkaProducer  # type: ignore[import]

from messages.models import User, Location, Update
from messages.serialization import serialize_model_to_json


async def main_async(topic: str):
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
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')

    # Get cluster layout and initial topic/partition leadership information
    await producer.start()

    try:
        for model in models:
            message = serialize_model_to_json(model)
            print(f"Sending to {topic!r}: {message!r}")
            await producer.send_and_wait(topic, message.encode('utf-8'), headers=headers)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main_async('foobar'))
