from typing import Any


def normalize_trade(payload: dict[str, Any]) -> dict[str, Any]:
    """Map a Binance @trade event to the canonical bronze schema."""
    return {
        "symbol": payload["s"],
        "trade_id": payload["t"],
        "price": payload["p"],
        "quantity": payload["q"],
        "trade_time_ms": payload["T"],
        "event_time_ms": payload["E"],
        "is_buyer_maker": payload["m"],
        "source": "binance",
    }