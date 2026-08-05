import asyncio
import json
import logging
import random
import signal
import time

import websockets
from confluent_kafka import Producer

from producer.config import settings
from producer.schema import normalize_trade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("producer")

shutdown = asyncio.Event()


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": "binance-trade-producer",
            "linger.ms": 50,
            "batch.num.messages": 1000,
            "compression.type": "lz4",
            "acks": "all",
            "enable.idempotence": True,
        }
    )


def on_delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery failed: %s", err)


async def stream_trades(producer: Producer) -> None:
    delivered = 0
    backoff = 1.0

    while not shutdown.is_set():
        try:
            log.info("connecting to %s", settings.stream_url)
            async with websockets.connect(
                settings.stream_url, ping_interval=20, ping_timeout=20
            ) as ws:
                log.info("connected, streaming %s", ",".join(settings.symbols))
                backoff = 1.0

                while not shutdown.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    envelope = json.loads(raw)
                    event = normalize_trade(envelope["data"])
                    event["ingested_at_ms"] = int(time.time() * 1000)

                    producer.produce(
                        topic=settings.raw_topic,
                        key=event["symbol"].encode(),
                        value=json.dumps(event).encode(),
                        callback=on_delivery,
                    )
                    producer.poll(0)

                    delivered += 1
                    if delivered % settings.log_every_n == 0:
                        lag = event["ingested_at_ms"] - event["trade_time_ms"]
                        log.info("produced %s trades, last lag %s ms", delivered, lag)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if shutdown.is_set():
                break
            sleep_for = min(backoff, 30) * (1 + random.random() * 0.3)
            log.warning("stream error (%s), reconnecting in %.1fs", exc, sleep_for)
            await asyncio.sleep(sleep_for)
            backoff = min(backoff * 2, 30)


async def main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: shutdown.set())

    producer = build_producer()
    try:
        await stream_trades(producer)
    finally:
        log.info("flushing producer")
        producer.flush(10)
        log.info("stopped")


if __name__ == "__main__":
    asyncio.run(main())