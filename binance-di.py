#!/usr/bin/env python3
"""
Unified Binance Data Ingestor (binance-di)

This module ingests real-time data from multiple symbols and streams
concurrently from the Binance API and can:
1. Write data to Redis Streams.
2. Write data to files (JSONL, CSV, Parquet, ORC) with batching and rotation.
3. Print clean JSON data to stdout for debugging or piping to other tools.
4. Exit automatically after collecting a specific number of samples.
5. Log all status messages to a file with the --log flag.
Includes graceful shutdown to flush all file buffers.
"""
import argparse
import asyncio
import csv
import json
import os
import sys
import logging
import pandas as pd
import redis.asyncio as redis
import websockets
from pathlib import Path
from dotenv import load_dotenv

# --- Configuration ---
PROD_BASE_URL = "wss://stream.binance.com:9443/ws"
TESTNET_BASE_URL = "wss://stream.testnet.binance.vision/ws"

BATCH_SIZE_FOR_COLUMNAR = 10000
FILE_ROTATION_LINE_COUNT = 100000

# --- Payload Processing Functions ---
def process_ticker_payload(data: dict) -> dict:
    return {
        "price_change": data.get("p"), "price_change_percent": data.get("P"),
        "last_price": data.get("c"), "high_price": data.get("h"),
        "low_price": data.get("l"), "total_volume_asset": data.get("v"),
        "total_volume_quote": data.get("q"), "event_time": data.get("E"),
    }

def process_order_book_payload(data: dict) -> dict:
    return {
        "lastUpdateId": data.get("u"), "bids": json.dumps(data.get("b", [])),
        "asks": json.dumps(data.get("a", [])),
    }

def process_trade_payload(data: dict) -> dict:
    return {
        'event_time': data.get('E'), 'price': data.get('p'),
        'quantity': data.get('q'), 'trade_time': data.get('T'),
        'is_buyer_maker': str(data.get('m'))
    }

def process_kline_payload(data: dict) -> dict:
    kline_data = data.get('k', {})
    return {
        'event_time': data.get('E'), 'kline_start_time': kline_data.get('t'),
        'kline_close_time': kline_data.get('T'), 'symbol': kline_data.get('s'),
        'interval': kline_data.get('i'), 'open_price': kline_data.get('o'),
        'close_price': kline_data.get('c'), 'high_price': kline_data.get('h'),
        'low_price': kline_data.get('l'), 'base_asset_volume': kline_data.get('v'),
        'number_of_trades': kline_data.get('n'),
        'is_kline_closed': str(kline_data.get('x')),
        'quote_asset_volume': kline_data.get('q'),
    }

# --- Main Logic ---
def setup_logging(args: argparse.Namespace):
    logger = logging.getLogger()
    log_level = logging.CRITICAL + 1 if args.silent else logging.INFO
    logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(stream_handler)

    if args.log:
        file_handler = logging.FileHandler(args.log, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        logger.addHandler(file_handler)

async def get_redis_connection(redis_host, redis_port):
    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        await r.ping()
        logging.info(f"Successfully connected to Redis at {redis_host}:{redis_port}")
        return r
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Error: Could not connect to Redis. {e}")
        return None

async def stream_producer(symbol: str, stream_type: str, base_url: str, process_func, data_queue: asyncio.Queue, stream_key: str, args: argparse.Namespace):
    websocket_url = f"{base_url}/{symbol.lower()}{stream_type}"
    tag = f"{stream_key.upper()}/{symbol}"
    logging.info(f"[{tag}] Starting producer for stream: {websocket_url}")

    sample_count = 0
    while True:
        try:
            async with websockets.connect(websocket_url) as websocket:
                logging.info(f"[{tag}] Successfully connected.")
                async for message in websocket:
                    data = json.loads(message)
                    payload = process_func(data)
                    await data_queue.put((stream_key, symbol, payload))

                    if args.samples:
                        sample_count += 1
                        if sample_count >= args.samples:
                            logging.info(f"[{tag}] Sample limit of {args.samples} reached. Producer finishing.")
                            return
        except (websockets.exceptions.ConnectionClosed, Exception) as e:
            if isinstance(e, asyncio.CancelledError):
                break
            logging.error(f"\n[{tag}] Producer error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

async def data_consumer(data_queue: asyncio.Queue, redis_client: redis.Redis, args: argparse.Namespace, file_writers: dict):
    while True:
        try:
            stream_key, symbol, payload = await data_queue.get()

            if args.print or args.print_only:
                try:
                    if args.silent:
                        output_payload = {"stream": stream_key, "symbol": symbol, "data": payload}
                        # FIX: Force flush to ensure data goes through pipes immediately
                        print(json.dumps(output_payload), flush=True)
                    else:
                        # Also flush here for good measure
                        print(f"\n--- [{stream_key.upper()}/{symbol}] Data Received ---", flush=True)
                        print(json.dumps(payload, indent=2), flush=True)
                except BrokenPipeError:
                    logging.info("[INFO] Pipe closed by consumer. Initiating shutdown.")
                    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
                    [task.cancel() for task in tasks]
                    break

            if redis_client:
                redis_stream_name = f"binance:{stream_key}:{symbol.lower()}"
                await redis_client.xadd(redis_stream_name, payload)

            if args.output:
                for fmt in args.output:
                    await write_to_file(stream_key, symbol, payload, fmt, file_writers, args)

            data_queue.task_done()
        except asyncio.CancelledError:
            break

async def write_to_file(stream_key, symbol, payload, fmt, writers, args):
    writer_key = f"{stream_key}_{symbol.lower()}_{fmt}"
    state = writers.get(writer_key, {})

    if fmt in ['parquet', 'orc']:
        if 'buffer' not in state:
            state['buffer'] = []
            state['part'] = 0
        state['buffer'].append(payload)
        if len(state['buffer']) >= BATCH_SIZE_FOR_COLUMNAR:
            await flush_buffer(state, stream_key, symbol, fmt, args)

    elif fmt in ['json', 'csv']:
        if not state.get('handle'):
            state['part'] = state.get('part', 0) + 1
            filename = f"{args.output_dir}/{stream_key}_{symbol.lower()}_{state['part']}.{fmt if fmt == 'csv' else 'jsonl'}"
            state['handle'] = open(filename, 'a', newline='', encoding='utf-8')
            state['lines'] = 0
            if fmt == 'csv':
                state['writer'] = csv.writer(state['handle'])
                state['header'] = list(payload.keys())
                state['writer'].writerow(state['header'])
                state['lines'] += 1
        handle = state['handle']
        if fmt == 'json':
            handle.write(json.dumps(payload) + '\n')
        elif fmt == 'csv':
            writer = state['writer']
            writer.writerow([payload.get(h) for h in state['header']])
        handle.flush()
        state['lines'] += 1
        if state['lines'] >= FILE_ROTATION_LINE_COUNT:
            handle.close()
            state['handle'] = None
            state['writer'] = None
            logging.info(f"\n[FILE_WRITER] Rotated file for {writer_key}")
    writers[writer_key] = state

async def flush_buffer(state, stream_key, symbol, fmt, args):
    if not state.get('buffer'):
        return
    df = pd.DataFrame(state['buffer'])
    state['buffer'] = []
    state['part'] += 1
    filename = f"{args.output_dir}/{stream_key}_{symbol.lower()}_{state['part']}.{fmt}"
    try:
        if fmt == 'parquet':
            df.to_parquet(filename, index=False, engine='pyarrow')
        elif fmt == 'orc':
            df.to_orc(filename, index=False)
        logging.info(f"\n[FILE_WRITER] Wrote batch to {filename}")
    except Exception as e:
        logging.error(f"\n[FILE_WRITER] Error writing to {filename}: {e}")

async def flush_all_buffers(file_writers, args):
    logging.info("\n[CLEANUP] Flushing all remaining file buffers...")
    for writer_key, state in file_writers.items():
        if writer_key.endswith(('parquet', 'orc')):
            stream_key, symbol, fmt = writer_key.rsplit('_', 2)
            await flush_buffer(state, stream_key, symbol, fmt, args)
    logging.info("[CLEANUP] Buffer flushing complete.")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Unified Binance Data Ingestor")
    parser.add_argument("--symbol", metavar="SYMBOL", type=str, required=True, help="Comma-separated list of symbols.")
    parser.add_argument("--load", type=str, default="ticker,trades,order-book", help="Comma-separated list of streams.")
    parser.add_argument("--interval", type=str, default="1m", help="Kline interval. Defaults to '1m'.")
    parser.add_argument("--testnet", action="store_true", help="Use TestNet and load .env_testnet.")
    parser.add_argument("--output-dir", type=str, default="data", help="Directory for output files.")
    parser.add_argument("--output", type=str, help="Comma-separated list of output formats: json,csv,parquet,orc.")
    parser.add_argument("--samples", type=int, help="Exit after receiving N samples per stream.")
    parser.add_argument("--log", nargs='?', const='binance-di.log', default=None, help="Enable file logging. Optionally specify a path. Defaults to binance-di.log.")
    parser.add_argument("--silent", action="store_true", help="Suppress status logs and format print output for piping.")

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--print", action="store_true", help="Print data to stdout.")
    output_group.add_argument("--print-only", action="store_true", help="Print data to stdout only (no Redis/file output).")

    args = parser.parse_args()
    args.load = set(args.load.lower().split(','))
    if args.output:
        args.output = set(args.output.lower().split(','))
    if args.print_only:
        args.output = None

    return args

async def main():
    args = parse_arguments()
    setup_logging(args)

    if args.output:
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)
        logging.info(f"Output directory set to '{args.output_dir}'")

    env_file = ".env_testnet" if args.testnet else ".env"
    if not os.path.exists(env_file):
        logging.error(f"Error: Environment file '{env_file}' not found.")
        return
    load_dotenv(dotenv_path=env_file)
    logging.info(f"Loaded configuration from '{env_file}'")

    redis_client = None
    if not args.print_only:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 31111))
        redis_client = await get_redis_connection(redis_host, redis_port)
        if not redis_client and not args.output:
             logging.critical("Failed to establish Redis connection and no file output is configured. Exiting.")
             return

    data_queue = asyncio.Queue()
    producer_tasks = []
    file_writers = {}

    consumer_task = asyncio.create_task(data_consumer(data_queue, redis_client, args, file_writers))

    base_url = TESTNET_BASE_URL if args.testnet else PROD_BASE_URL
    symbols = [s.strip().upper() for s in args.symbol.split(',')]
    load_types = args.load

    logging.info(f"Preparing to load streams {list(load_types)} for symbols {symbols}")

    for symbol in symbols:
        if "ticker" in load_types:
            producer_tasks.append(asyncio.create_task(stream_producer(symbol, "@ticker", base_url, process_ticker_payload, data_queue, "ticker", args)))
        if "order-book" in load_types:
            producer_tasks.append(asyncio.create_task(stream_producer(symbol, "@depth", base_url, process_order_book_payload, data_queue, "order-book", args)))
        if "trades" in load_types:
            producer_tasks.append(asyncio.create_task(stream_producer(symbol, "@aggTrade", base_url, process_trade_payload, data_queue, "trades", args)))
        if "klines" in load_types:
            producer_tasks.append(asyncio.create_task(stream_producer(symbol, f"@kline_{args.interval}", base_url, process_kline_payload, data_queue, "klines", args)))

    if not producer_tasks:
        logging.error("No valid load option specified. Exiting.")
        consumer_task.cancel()
        return

    try:
        await asyncio.gather(*producer_tasks)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        logging.info("\n[SHUTDOWN] Initiating shutdown sequence...")
        # Cancel running producers if they haven't finished
        for task in producer_tasks:
            task.cancel()

        await data_queue.join()

        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

        if args.output:
            await flush_all_buffers(file_writers, args)

        logging.info("[SHUTDOWN] Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n[MAIN] Keyboard interrupt received. Exiting.")
