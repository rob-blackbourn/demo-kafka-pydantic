from confluent_kafka import Consumer  # type: ignore[import]


def main(topic: str) -> None:
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print(f'Received message: {msg.value()!r}')

    consumer.close()


if __name__ == '__main__':
    main('foobar')
