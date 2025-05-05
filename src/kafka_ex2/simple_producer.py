import asyncio

from aiokafka import AIOKafkaProducer  # type: ignore[import]


async def main_async(topic: str):
    print("Connecting to Kafka...")
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')

    # Get cluster layout and initial topic/partition leadership information
    await producer.start()

    try:
        for message in ['one', 'two', 'three']:
            print(f"Sending to {topic!r}: {message!r}")
            await producer.send_and_wait(topic, message.encode('utf-8'))
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main_async('foobar'))
