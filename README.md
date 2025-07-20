# Binance WebSocket Redis Data Ingestor

## Overview

The `binance-di.py` script is a high-performance, concurrent data ingestion engine for the Binance exchange. It connects to multiple real-time WebSocket streams for multiple trading symbols simultaneously, making it a flexible tool for various applications including real-time analytics, trading bot development, and historical dataset creation.

## Key Features

- **Concurrent Ingestion**: Manages hundreds of WebSocket connections efficiently using asyncio
- **Multi-Symbol & Multi-Stream**: Subscribe to any combination of symbols and data streams in a single command
- **Flexible Output Targets**:
  - Redis Streams for low-latency, real-time consumption
  - File storage in multiple formats (JSON, CSV, Parquet, ORC)
  - Console output for live monitoring or piping to other tools
- **Sample Limiting**: Automatically stop after collecting a specified number of messages
- **Advanced Logging**: Control status message visibility with `--silent` flag and file logging with `--log`
- **Configurable Environment**: Switch between production and testnet environments
- **Robust & Resilient**: Automatic reconnection and graceful shutdown
- **Efficient File Handling**: Smart batching and rotation for optimal performance

## Setup and Installation

### Prerequisites

- Python 3.8+
- Redis instance (optional, not required with `--print-only`) [Simple Kubernetes Redis server on `k8s-redis.yaml`]

### Step 1: Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

This will install:

- redis
- websockets
- python-dotenv
- pandas
- pyarrow

### Step 2: Configure Environment Files

#### Production Environment (`.env`)

For your live API keys:

```env
BINANCE_API_KEY="your_api_key_here"
BINANCE_API_SECRET="your_secret_here"
REDIS_HOST=localhost
REDIS_PORT=31111
```

#### TestNet Environment (`.env_testnet`)

For testnet API keys:

```env
BINANCE_API_KEY="your_testnet_api_key"
BINANCE_API_SECRET="your_testnet_secret"
REDIS_HOST=localhost
REDIS_PORT=31111
```

## Usage

### Command-Line Arguments

| Argument | Description | Required | Default |
|----------|-------------|----------|----------|
| `--symbol` | Comma-separated list of trading symbols | Yes | - |
| `--load` | Streams to load: ticker, order-book, trades, klines | No | ticker,trades,order-book |
| `--interval` | Kline interval (e.g., 1m, 5m, 1h) | No | 1m |
| `--samples` | Exit after N samples per stream | No | Run indefinitely |
| `--testnet` | Use TestNet environment | No | Disabled |
| `--output-dir` | Directory for output files | No | data/ |
| `--output` | Output formats: json, csv, parquet, orc | No | None |
| `--log` | Enable file logging (optionally specify path) | No | binance-di.log |
| `--print` | Print data to stdout + write to outputs | No | Disabled |
| `--print-only` | Print data to stdout only | No | Disabled |
| `--silent` | Suppress logs, format for machine parsing | No | Disabled |

## Examples

### 1. Standard Data Ingestion

Stream ticker and trade data to Parquet files and Redis:

```bash
python binance-di.py --symbol BTCUSDT,ETHUSDT --load ticker,trades --output parquet
```

### 2. Create Sample Dataset

Get 5 ticker samples and save in multiple formats:

```bash
python binance-di.py --symbol SOLUSDT --load ticker --samples 5 --output json,csv,parquet --print
```

### 3. Silent Mode for Scripting

Pipe clean JSON to jq for processing:

```bash
python binance-di.py --symbol BTCUSDT --load trades --samples 10 --print-only --silent | jq '.data.price'
```

### 4. Long-Running with Logging

Run indefinitely with logging:

```bash
python binance-di.py --symbol BTCUSDT,ETHUSDT,BNBUSDT --output parquet --log
```

### 5. Silent Mode for Bots

Pipe clean JSON to jq for processing:

```bash
python binance-di.py --symbol BTCUSDT --load trades --samples 1 --print-only --silent | jq '.data.last_price'
```

### 6. Filtering when multiple load sources are active

Selecting `ticker` only data:

```bash
python binance-di.py --symbol BTCUSDT --load trades,ticker --samples 1 --print-only --silent | jq 'select(.stream == "ticker") | .data.last_price'
```

## Output Details

### Console Output Modes

#### Human-Readable (Default)

```bash
--- [TICKER/BTCUSDT] Data Received ---
{
  "last_price": "118144.16000000",
  ...
}
```

#### Machine-Readable (`--silent`)

```bash
{"stream":"ticker","symbol":"BTCUSDT","data":{"last_price":"118144.16..."}}
```

### File Output

- **Directory**: `--output-dir` (default: `data/`)
- **Naming**: `<stream_type>_<symbol>_<part>.<format>` (e.g., `trades_btcusdt_1.parquet`)

#### File Writing Strategies

| Format | Behavior | Batch Size | Rotation |
|--------|----------|------------|----------|
| JSON/CSV | Line-by-line, immediate flush | N/A | 100,000 lines |
| Parquet/ORC | Batched writes | 10,000 records | When batch is full |

## Data Format Examples

### Ticker Data

**Redis Stream Name**: `binance:ticker:btcusdt`

**Payload Example**:

```json
{
    "price_change": "150.50",
    "price_change_percent": "0.127",
    "last_price": "118050.00",
    "high_price": "119000.00",
    "low_price": "117500.00",
    "total_volume_asset": "12345.67",
    "total_volume_quote": "1459876543.21",
    "event_time": "1678886400000"
}
```

### Order Book Data

**Redis Stream Name**: `binance:order-book:btcusdt`

**Payload Example**:

```json
{
    "lastUpdateId": "123456789",
    "bids": "[[\"118050.00\",\"0.5\"],[\"118049.90\",\"1.2\"]]",
    "asks": "[[\"118050.10\",\"0.8\"],[\"118050.20\",\"2.1\"]]"
}
```

### Aggregate Trades Data

**Redis Stream Name**: `binance:trades:btcusdt`

**Payload Example**:

```json
{
    "event_time": "1678886400123",
    "price": "118050.10",
    "quantity": "0.005",
    "trade_time": "1678886400120",
    "is_buyer_maker": "true"
}
```

### Klines (Candlesticks) Data

**Redis Stream Name**: `binance:klines:btcusdt`

**Payload Example (1m interval)**:

```json
{
    "event_time": "1678886400234",
    "kline_start_time": "1678886340000",
    "kline_close_time": "1678886399999",
    "symbol": "BTCUSDT",
    "interval": "1m",
    "open_price": "118040.00",
    "close_price": "118050.00",
    "high_price": "118055.00",
    "low_price": "118035.00",
    "base_asset_volume": "50.123",
    "number_of_trades": "542",
    "is_kline_closed": "false",
    "quote_asset_volume": "5917890.12"
}
```

## Graceful Shutdown

Press `Ctrl+C` to initiate a graceful shutdown that will:

1. Close all WebSocket connections
2. Process remaining data in the queue
3. Flush all file buffers to disk
4. Ensure no data is lost
