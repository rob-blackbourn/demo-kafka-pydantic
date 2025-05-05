import asyncio

from aiokafka import AIOKafkaConsumer  # type: ignore[import]


async def main_async(topic: str):

    print("Connecting to Kafka...")

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id='my-group'
    )

    await consumer.start()

    try:

        print("Waiting for messages...")

        async for msg in consumer:
            print(msg)

            if msg.value == b'3':
                break

    finally:

        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(main_async('foobar'))
