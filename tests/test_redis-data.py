"""
Redis Stream Test Consumer

This script connects to a Redis instance and continuously reads the latest messages
from the Binance order book stream created by the binance-wrapper.py script.

Usage:
python test_redis-data.py --symbol BTCUSDT
"""
import argparse
import json
import redis

# --- Configuration ---
# Your Redis connection details
REDIS_HOST = "localhost"
REDIS_PORT = 31111 # As specified in your request

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="A test consumer for Redis streams.")
    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="The trading symbol (e.g., BTCUSDT) whose stream you want to read."
    )
    return parser.parse_args()

def redis_stream_consumer(symbol: str):
    """
    Connects to Redis and listens for new messages on a specific stream.
    """
    stream_name = f"binance:orderbook:{symbol.lower()}"
    
    try:
        # Connect to Redis, decode_responses=True makes the output human-readable
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError as e:
        print(f"Error: Could not connect to Redis. Please ensure it is running and accessible.")
        print(f"Details: {e}")
        return

    # The special '$' ID means we only want to receive messages that arrive
    # after we start listening.
    last_id = '$'
    
    print(f"Listening for new messages on Redis stream: '{stream_name}'...")
    print("Press Ctrl+C to stop.")

    while True:
        try:
            # The block=0 argument tells Redis to wait forever until a message arrives.
            response = r.xread(
                {stream_name: last_id},
                count=1,
                block=0
            )

            if response:
                # The response is a list containing the stream and its messages
                for stream, messages in response:
                    for message_id, data in messages:
                        print(f"\n--- New Message (ID: {message_id}) ---")
                        
                        # Nicely format and print the received data
                        # The 'bids' and 'asks' are stored as JSON strings, so we load them.
                        print(f"  Last Update ID: {data.get('lastUpdateId')}")
                        bids = json.loads(data.get('bids', '[]'))
                        asks = json.loads(data.get('asks', '[]'))
                        
                        print(f"  Top 3 Bids: {bids[:3]}")
                        print(f"  Top 3 Asks: {asks[:3]}")

                        # Update the last_id to ensure we only get newer messages
                        last_id = message_id

        except Exception as e:
            print(f"An error occurred: {e}")
            break

def main():
    """Main function to run the consumer."""
    args = parse_arguments()
    symbol_to_read = args.symbol.upper()
    
    try:
        redis_stream_consumer(symbol_to_read)
    except KeyboardInterrupt:
        print("\nConsumer stopped. Exiting gracefully.")

if __name__ == "__main__":
    main()

