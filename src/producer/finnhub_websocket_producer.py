import json
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
import websocket


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "market_ticks"
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FINNHUB_SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA", "AMZN"]
FINNHUB_ENABLE_TRACE = False

producer = None


def event_time_from_millis(value):
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def normalize_trade(trade):
    return {
        "symbol": trade["s"],
        "event_time": event_time_from_millis(trade["t"]),
        "price": float(trade["p"]),
        "volume": int(float(trade.get("v", 0))),
        "source": "finnhub",
    }


def on_open(ws):
    for symbol in FINNHUB_SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(f"Subscribed to Finnhub trades for {symbol}")


def on_message(_ws, message):
    payload = json.loads(message)

    if payload.get("type") != "trade":
        print(payload)
        return

    for trade in payload.get("data", []):
        tick = normalize_trade(trade)
        producer.send(KAFKA_TOPIC, key=tick["symbol"], value=tick)
        print(tick)

    producer.flush()


def on_error(_ws, error):
    print(f"Finnhub WebSocket error: {error}")


def on_close(_ws, close_status_code, close_msg):
    print(f"Finnhub WebSocket closed: {close_status_code} {close_msg}")


def main():
    global producer

    if not FINNHUB_API_KEY:
        raise ValueError("FINNHUB_API_KEY is required. Add it to .env first.")
    if not FINNHUB_SYMBOLS:
        raise ValueError("FINNHUB_SYMBOLS must contain at least one symbol.")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )

    print(f"Producing Finnhub ticks to {KAFKA_TOPIC} on {KAFKA_BOOTSTRAP_SERVERS}")
    websocket.enableTrace(FINNHUB_ENABLE_TRACE)

    while True:
        socket_app = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        socket_app.run_forever(ping_interval=20, ping_timeout=10)

        print(f"Finnhub disconnected. Reconnecting in 10 seconds...")
        time.sleep(10)


if __name__ == "__main__":
    main()
