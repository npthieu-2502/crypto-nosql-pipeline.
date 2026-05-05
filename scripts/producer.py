import json
import time
import websocket
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load cấu hình từ file .env
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'crypto_trades'

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        print(f"Connected to Kafka broker at {KAFKA_BROKER} successfully.")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return None

producer = create_kafka_producer()
if not producer:
    exit(1)

def on_message(ws, message):
    try:
        data = json.loads(message)
        if data.get('e') == 'trade':
            trade_event = {
                "event_time": data['E'],
                "symbol": data['s'],
                "price": float(data['p']),
                "quantity": float(data['q'])
            }
            producer.send(TOPIC_NAME, key=trade_event["symbol"], value=trade_event)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Sent -> {trade_event['symbol']}: ${trade_event['price']} (Qty: {trade_event['quantity']})")
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket Closed")

def on_open(ws):
    print("WebSocket Connection Opened. Subscribing to BTC and ETH trades...")
    subscribe_msg = {
        "method": "SUBSCRIBE",
        "params": ["btcusdt@trade", "ethusdt@trade"],
        "id": 1
    }
    ws.send(json.dumps(subscribe_msg))

def run_websocket():
    socket_url = "wss://stream.binance.com:9443/ws"
    # Vòng lặp vô hạn giúp tự động Reconnect nếu bị đứt mạng
    while True:
        try:
            print("Attempting to connect to Binance WebSocket...")
            ws = websocket.WebSocketApp(
                socket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever()
        except Exception as e:
            print(f"Fatal error in WebSocket: {e}")
        
        print("Connection dropped. Reconnecting in 5 seconds...")
        time.sleep(5)

if __name__ == "__main__":
    run_websocket()
