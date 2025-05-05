from kafka import KafkaConsumer  # type: ignore[import]


def main(topic: str) -> None:

    print("Connecting to Kafka...")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id="my-group"
    )

    print("Waiting for messages...")

    for msg in consumer:
        print(msg)

        if msg.value == b'3':
            break

    consumer.close()


if __name__ == "__main__":
    main('foobar')
