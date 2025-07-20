"""
Redis Stream Test Consumer - Enhanced

This script reads the Binance order book stream and calculates key metrics like
Best Bid, Best Ask, and Mid-Price from the data.

Usage:
python test_redis-data.py --symbol BTCUSDT
"""
import argparse
import json
import redis

REDIS_HOST = "localhost"
REDIS_PORT = 31111

def parse_arguments():
    parser = argparse.ArgumentParser(description="An enhanced test consumer for Redis streams.")
    parser.add_argument("--symbol", type=str, required=True, help="The trading symbol (e.g., BTCUSDT) to read.")
    return parser.parse_args()

def redis_stream_consumer(symbol: str):
    stream_name = f"binance:orderbook:{symbol.lower()}"
    
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError as e:
        print(f"Error: Could not connect to Redis. {e}")
        return

    last_id = '$'
    print(f"Listening for new messages on Redis stream: '{stream_name}'...")
    print("Press Ctrl+C to stop.")

    while True:
        try:
            response = r.xread({stream_name: last_id}, count=1, block=0)
            if response:
                for stream, messages in response:
                    for message_id, data in messages:
                        print(f"\n--- New Message (ID: {message_id}) ---")
                        bids = json.loads(data.get('bids', '[]'))
                        asks = json.loads(data.get('asks', '[]'))

                        # --- METRIC CALCULATION ---
                        if bids and asks:
                            best_bid = float(bids[0][0])
                            best_ask = float(asks[0][0])
                            mid_price = (best_bid + best_ask) / 2
                            spread = best_ask - best_bid
                            
                            print(f"  Best Bid:  {best_bid:.2f}")
                            print(f"  Best Ask:  {best_ask:.2f}")
                            print(f"  Mid-Price: {mid_price:.2f}")
                            print(f"  Spread:    {spread:.2f}")
                        else:
                            print("  (Order book side is empty, cannot calculate price)")
                        
                        last_id = message_id
        except Exception as e:
            print(f"An error occurred: {e}")
            break

def main():
    args = parse_arguments()
    try:
        redis_stream_consumer(args.symbol.upper())
    except KeyboardInterrupt:
        print("\nConsumer stopped.")

if __name__ == "__main__":
    main()
