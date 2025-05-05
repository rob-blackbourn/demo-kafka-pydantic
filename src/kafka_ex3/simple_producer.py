from confluent_kafka import Producer  # type: ignore[import]


def main(topic: str) -> None:
    data = {
        'one': '1',
        'two': '2',
        'three': '3',
    }

    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    for key, value in data.items():
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        producer.produce(
            topic,
            value.encode('utf-8'),
            key.encode(),
        )

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()


if __name__ == '__main__':
    main('foobar')
