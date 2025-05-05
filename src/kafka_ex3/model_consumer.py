from typing import Callable, Sequence

from confluent_kafka import Consumer  # type: ignore[import]

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


def main(topic: str) -> None:
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
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

        content_type = get_header_value(
            msg.headers(),
            'content-type',
            lambda b: b.decode('utf-8'),
            "application/octet-stream"
        )
        if content_type == "application/json":
            model = deserialize_model_from_json(msg.value())
            print(f"Received {content_type!r}: {model!r}")
        else:
            print(f"Received {content_type!r}: {msg.value()!r}")

    consumer.close()


if __name__ == '__main__':
    main('foobar')
