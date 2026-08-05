import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    ws_base: str = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443")
    raw_topic: str = os.getenv("RAW_TRADES_TOPIC", "trades.raw")
    log_every_n: int = int(os.getenv("LOG_EVERY_N", "500"))
    symbols: list[str] = field(
        default_factory=lambda: [
            s.strip().lower()
            for s in os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")
            if s.strip()
        ]
    )

    @property
    def stream_url(self) -> str:
        streams = "/".join(f"{s}@trade" for s in self.symbols)
        return f"{self.ws_base}/stream?streams={streams}"


settings = Settings()