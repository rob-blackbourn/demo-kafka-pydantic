from kafka import KafkaProducer  # type: ignore[import]


def main(topic: str) -> None:
    data = {
        'one': '1',
        'two': '2',
        'three': '3',
    }
    print("Connecting to Kafka...")
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    for key, value in data.items():
        print(f"Sending to {topic!r}: {key!r}/{value!r}")
        producer.send(topic, value.encode('utf-8'), key.encode('utf-8'))


if __name__ == "__main__":
    main('foobar')
