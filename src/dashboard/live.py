import json
import os

from confluent_kafka import Consumer


def build_consumer(group_suffix: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            "group.id": f"dashboard-{group_suffix}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )


def poll_latest(consumer: Consumer, topic: str, timeout: float = 2.0) -> dict[str, dict]:
    """Drain the topic and keep only the most recent candle per symbol."""
    consumer.subscribe([topic])
    latest: dict[str, dict] = {}
    deadline_polls = 40

    for _ in range(deadline_polls):
        msg = consumer.poll(timeout / deadline_polls)
        if msg is None or msg.error():
            continue
        payload = json.loads(msg.value().decode())
        symbol = payload["symbol"]
        current = latest.get(symbol)
        if current is None or payload["window_start"] >= current["window_start"]:
            latest[symbol] = payload

    return latest
