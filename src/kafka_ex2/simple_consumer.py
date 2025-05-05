import asyncio

from aiokafka import AIOKafkaConsumer  # type: ignore[import]


async def main_async(topic: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id='my-group'
    )

    # Get cluster layout
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(main_async('foobar'))
