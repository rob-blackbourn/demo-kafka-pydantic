import asyncio

from aiokafka import AIOKafkaProducer  # type: ignore[import]


async def main_async(topic: str):
    data = {
        'one': '1',
        'two': '2',
        'three': '3',
    }

    print("Connecting to Kafka...")
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')

    # Get cluster layout and initial topic/partition leadership information
    await producer.start()

    try:
        for key, value in data.items():
            print(f"Sending to {topic!r}: {key!r}/{value!r}")
            await producer.send_and_wait(topic, value.encode('utf-8'), key.encode('utf-8'))
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main_async('foobar'))
