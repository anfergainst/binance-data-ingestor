"""
Redis Ticker Stream Test Consumer

This script connects to a Redis instance and continuously reads the latest 24hr
statistics from the Binance ticker stream.

Usage:
python test_redis-ticker.py --symbol BTCUSDT
"""
import argparse
import redis

# --- Configuration ---
REDIS_HOST = "localhost"
REDIS_PORT = 31111 # As specified in your setup

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="A test consumer for Redis ticker streams.")
    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="The trading symbol (e.g., BTCUSDT) whose stream you want to read."
    )
    return parser.parse_args()

def redis_stream_consumer(symbol: str):
    """
    Connects to Redis and listens for new messages on the ticker stream.
    """
    stream_name = f"binance:ticker:{symbol.lower()}"
    
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
                        print(f"\n--- New Ticker Update (ID: {message_id}) ---")
                        
                        # Print all available metrics from the payload
                        for key, value in data.items():
                            print(f"  {key.replace('_', ' ').title():<22}: {value}")

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
