import os
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.lake import load_history
from dashboard.live import build_consumer, poll_latest

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
AGG_TOPIC = os.getenv("AGG_OHLC_TOPIC", "ohlc.1m")

st.set_page_config(page_title="Crypto realtime pipeline", layout="wide")


@st.cache_resource
def consumer():
    return build_consumer(str(int(time.time())))


@st.cache_data(ttl=60)
def history(symbol: str, resolution: str) -> pd.DataFrame:
    return load_history(symbol, resolution)


def format_window(raw: str) -> str:
    return raw.replace("T", " ").replace("Z", "").split(".")[0]


st.title("Crypto realtime pipeline")
st.caption("Binance websocket to Redpanda to Spark Structured Streaming to S3")

col_a, col_b, col_c = st.columns([2, 2, 1])
symbol = col_a.selectbox("Symbol", SYMBOLS)
resolution = col_b.selectbox("History resolution", ["1m", "5m", "15m", "1h"])
auto = col_c.toggle("Live", value=True)

live = poll_latest(consumer(), AGG_TOPIC)
current = live.get(symbol)

st.subheader("Current candle")

if current:
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Open", f"{float(current['open']):,.2f}")
    m2.metric("High", f"{float(current['high']):,.2f}")
    m3.metric("Low", f"{float(current['low']):,.2f}")
    m4.metric("Close", f"{float(current['close']):,.2f}")
    m5.metric("Trades", f"{current['trade_count']:,}")
    st.caption(
        f"Window starting {format_window(current['window_start'])} UTC, "
        f"streamed live from Kafka topic {AGG_TOPIC}"
    )
else:
    st.info("Waiting for the first aggregate. Is the Spark job running?")

st.subheader(f"History, {resolution} candles")

df = history(symbol, resolution)

if df.empty:
    st.warning("No data in the lake yet for this symbol and resolution.")
else:
    fig = go.Figure(
        go.Candlestick(
            x=df["window_start"],
            open=df["open"].astype(float),
            high=df["high"].astype(float),
            low=df["low"].astype(float),
            close=df["close"].astype(float),
        )
    )
    fig.update_layout(
        height=460,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis_rangeslider_visible=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns(2)
    left.metric("Candles loaded", len(df))
    right.metric("Total trades", f"{int(df['trade_count'].sum()):,}")
    st.caption("Read from S3 Parquet via DuckDB, cached for 60 seconds")

if auto:
    time.sleep(2)
    st.rerun()
