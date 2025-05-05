import asyncio
from typing import Callable, Sequence

from aiokafka import AIOKafkaConsumer  # type: ignore[import]

from messages.serialization import deserialize_model_from_json


def get_header_value[T](
        headers: Sequence[tuple[str, bytes]],
        key: str,
        decode: Callable[[bytes], T],
        default: T
) -> T:
    for k, v in headers:
        if k == key:
            return decode(v)
    return default


async def main_async(topic: str):
    print("Connecting to Kafka...")
    consumer = AIOKafkaConsumer(topic, bootstrap_servers='localhost:9092')

    # Get cluster layout
    await consumer.start()

    try:
        # Consume messages
        async for msg in consumer:
            print(msg)

            content_type = get_header_value(
                msg.headers,
                'content-type',
                lambda b: b.decode('utf-8'),
                "application/octet-stream"
            )
            if content_type == "application/json":
                model = deserialize_model_from_json(msg.value)
                print(f"Received {content_type!r}: {model!r}")
            else:
                print(f"Received {content_type!r}: {msg.value!r}")

    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(main_async('foobar'))
