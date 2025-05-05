from typing import Callable

from kafka import KafkaConsumer  # type: ignore[import]

from messages.serialization import deserialize_model_from_json


def get_header_value[T](
        headers: list[tuple[str, bytes]],
        key: str,
        decode: Callable[[bytes], T],
        default: T
) -> T:
    for k, v in headers:
        if k == key:
            return decode(v)
    return default


def main() -> None:
    print("Connecting to Kafka...")
    consumer = KafkaConsumer(
        'foobar',
        bootstrap_servers='localhost:9092',
        group_id='my-group'
    )

    print("Waiting for messages...")
    for msg in consumer:
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


if __name__ == "__main__":
    main()
