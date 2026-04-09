# Crypto ETL Pipeline

A production-patterned daily ETL pipeline that extracts live cryptocurrency market data from the CoinGecko API, validates and transforms it using Pydantic and pandas, and loads it into a local PostgreSQL database via SQLAlchemy bulk upserts.

Built as a portfolio project to demonstrate core Data Engineering concepts — schema validation, idempotent loading, secrets management, structured logging, and scheduled orchestration.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Database Schema](#database-schema)
- [Engineering Decisions](#engineering-decisions)
- [Scheduling](#scheduling)
- [Extending the Project](#extending-the-project)

---

## Architecture

```
CoinGecko API
     │
     │  HTTP GET /coins/markets
     ▼
┌─────────────┐     raw JSON      ┌──────────────────────┐     clean DataFrame     ┌──────────────────┐
│  extract.py │ ───────────────►  │    transform.py       │ ──────────────────────► │     load.py      │
│             │                   │                        │                         │                  │
│  requests   │                   │  Pydantic validation   │                         │  SQLAlchemy      │
│  + timeout  │                   │  pandas transform      │                         │  bulk upsert     │
│  + retries  │                   │  UTC normalization     │                         │  PostgreSQL      │
└─────────────┘                   └──────────────────────┘                         └──────────────────┘
                                                                                              │
                                                                                              ▼
                                                                                   coin_market_data table
                                                                                   (idempotent, timestamped)

Orchestrated by: main.py  ──►  schedule library  OR  GitHub Actions cron
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Extract | `requests` | HTTP client for REST API calls |
| Validate | `pydantic` v2 | Schema enforcement and type coercion at ingestion boundary |
| Transform | `pandas` | DataFrame manipulation and normalization |
| Load | `SQLAlchemy` | ORM table definition, connection pooling, bulk upserts |
| Database | PostgreSQL | Persistent storage with exact-decimal `NUMERIC` columns |
| Config | `python-dotenv` | Secret management via `.env` file |
| Scheduling | `schedule` / GitHub Actions | Daily pipeline orchestration |

---

## Project Structure

```
crypto_etl_project/
├── src/
│   ├── extract.py        # CoinGecko API client — HTTP, error handling, logging
│   ├── transform.py      # Pydantic model, validation, pandas transformations
│   ├── load.py           # SQLAlchemy ORM model, table creation, bulk upsert
│   └── database.py       # Engine factory, connection verification
├── main.py               # Orchestrator — runs the full ETL cycle
├── requirements.txt      # Pinned dependencies
├── .env.example          # Template for required environment variables
├── .gitignore
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL 14+ running locally (or a remote instance)
- A free [CoinGecko](https://www.coingecko.com/en/api) account (no API key required for the public endpoints used here)

### 1. Clone the repository

```bash
git clone https://github.com/your-username/crypto-etl-pipeline.git
cd crypto-etl-pipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv

# macOS / Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create the database

Connect to your PostgreSQL instance and run:

```sql
CREATE DATABASE crypto_etl;
```

### 5. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials (see [Configuration](#configuration) below).

---

## Configuration

All configuration is managed via a `.env` file. **Never commit this file.** It is excluded by `.gitignore`.

```bash
# .env.example  — copy to .env and fill in your values

COINGECKO_BASE_URL=https://api.coingecko.com/api/v3

DB_USER=postgres
DB_PASSWORD=your_password_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crypto_etl
```

| Variable | Description | Default |
|---|---|---|
| `COINGECKO_BASE_URL` | CoinGecko API base URL | `https://api.coingecko.com/api/v3` |
| `DB_USER` | PostgreSQL username | — |
| `DB_PASSWORD` | PostgreSQL password | — |
| `DB_HOST` | Database host | `localhost` |
| `DB_PORT` | Database port | `5432` |
| `DB_NAME` | Target database name | — |

---

## Running the Pipeline

### Single run

```bash
python main.py
```

Expected output:

```
2026-04-09 11:40:12,313 [INFO] main — ═══════════════════════════════════════════════════
2026-04-09 11:40:12,313 [INFO] main — ETL pipeline starting at 2026-04-09T11:40:12+00:00
2026-04-09 11:40:12,313 [INFO] main — ═══════════════════════════════════════════════════
2026-04-09 11:40:12,350 [INFO] main — [0/3] Running startup checks...
2026-04-09 11:40:12,380 [INFO] main — [1/3] Starting extraction...
2026-04-09 11:40:12,800 [INFO] main — [2/3] Starting transformation & validation...
2026-04-09 11:40:12,410 [INFO] main — [3/3] Starting load...
2026-04-09 11:40:12,424 [INFO] main — Pipeline complete in 1.36s
2026-04-09 11:40:12,425 [INFO] main —   Records extracted : 5
2026-04-09 11:40:12,425 [INFO] main —   Records validated : 5
2026-04-09 11:40:12,426 [INFO] main —   Rows inserted     : 5
```

### Verify the data

Connect to PostgreSQL and run:

```sql
SELECT
    coin_id,
    symbol,
    current_price,
    market_cap_rank,
    last_updated,
    ingested_at
FROM coin_market_data
ORDER BY market_cap_rank;
```

### Test idempotency

Run the pipeline a second time immediately. The output should show:

```
Rows inserted     : 0
```

This confirms the `ON CONFLICT DO NOTHING` upsert strategy is working correctly. The pipeline is safe to re-run without producing duplicate rows.

### Run individual modules in isolation

Each module exposes a `__main__` block for isolated testing:

```bash
# Test extraction only
python src/extract.py

# Test transformation only (calls extract internally)
python src/transform.py

# Test the full stack except orchestration
python src/load.py
```

---

## Database Schema

**Table:** `coin_market_data`

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `id` | `INTEGER` | No | Auto-increment surrogate primary key |
| `coin_id` | `VARCHAR(50)` | No | CoinGecko coin identifier (e.g. `bitcoin`) |
| `symbol` | `VARCHAR(10)` | No | Normalized to uppercase (e.g. `BTC`) |
| `name` | `VARCHAR(100)` | No | Display name |
| `current_price` | `NUMERIC(30, 10)` | No | Price in USD |
| `high_24h` | `NUMERIC(30, 10)` | No | 24h high in USD |
| `low_24h` | `NUMERIC(30, 10)` | No | 24h low in USD |
| `price_change_24h` | `NUMERIC(30, 10)` | Yes | Absolute price change |
| `price_change_percentage_24h` | `NUMERIC(10, 6)` | Yes | Percentage price change |
| `market_cap` | `NUMERIC(30, 10)` | No | Market capitalisation in USD |
| `market_cap_rank` | `INTEGER` | Yes | Global market cap rank |
| `market_cap_change_24h` | `NUMERIC(30, 10)` | Yes | Absolute market cap change |
| `market_cap_change_percentage_24h` | `NUMERIC(10, 6)` | Yes | Percentage market cap change |
| `total_volume` | `NUMERIC(30, 10)` | No | 24h trading volume in USD |
| `circulating_supply` | `NUMERIC(30, 10)` | Yes | Coins in circulation |
| `total_supply` | `NUMERIC(30, 10)` | Yes | Total minted supply |
| `max_supply` | `NUMERIC(30, 10)` | Yes | Hard cap (null for uncapped coins) |
| `ath` | `NUMERIC(30, 10)` | Yes | All-time high price in USD |
| `ath_change_percentage` | `NUMERIC(10, 6)` | Yes | % change from all-time high |
| `ath_date` | `TIMESTAMPTZ` | Yes | All-time high date (UTC) |
| `last_updated` | `TIMESTAMPTZ` | No | CoinGecko's last update timestamp (UTC) |
| `ingested_at` | `TIMESTAMPTZ` | No | Pipeline ingestion timestamp (UTC) |

**Unique constraint:** `(coin_id, last_updated)` — enforces one record per coin per API update cycle, enabling idempotent upserts.

---

## Engineering Decisions

### Why Pydantic for validation instead of relying on pandas?

Pandas will silently load a DataFrame with mixed types, coerce nulls to `NaN`, or accept a string where a float is expected. Pydantic validates each record at the ingestion boundary and raises an explicit error before bad data enters the pipeline. This enforces a strict data contract with the external API — if CoinGecko changes its response shape, the pipeline fails loudly on the first bad record rather than silently corrupting the database.

### Why `NUMERIC(30, 10)` instead of `FLOAT`?

PostgreSQL's `FLOAT` uses IEEE 754 binary floating-point arithmetic, which trades precision for speed. For financial data (prices, market caps, volumes), silent rounding errors are unacceptable. `NUMERIC` stores exact decimal values — `71391.0000000000` is stored and retrieved exactly as written.

### Why separate `database.py` from `load.py`?

Single-responsibility principle. `database.py` knows how to connect; `load.py` knows how to write data. If a future `query.py` or `health_check.py` module also needs a database connection, it imports from `database.py` without pulling in any loading logic. This also makes mocking the database connection in unit tests straightforward.

### Why `ON CONFLICT DO NOTHING` (upsert) instead of plain `INSERT`?

Plain `INSERT` raises an `IntegrityError` when the unique constraint `(coin_id, last_updated)` is violated by a duplicate row. The upsert strategy skips duplicates silently, making the pipeline idempotent — running it 10 times produces the same database state as running it once. This is critical for scheduled pipelines where retries and re-runs must be safe.

### Why store both `last_updated` and `ingested_at`?

`last_updated` is CoinGecko's timestamp — it tells you how fresh the market data is. `ingested_at` is the pipeline's own timestamp — it tells you when your system processed it. These are different facts. The gap between them is a useful diagnostic signal (e.g. a large gap may indicate API lag or a pipeline delay).

### Why `pool_pre_ping=True` on the SQLAlchemy engine?

SQLAlchemy reuses database connections from a pool. If a connection goes stale (database restarted, network timeout), the pool will hand it out anyway, causing a cryptic mid-pipeline failure. `pool_pre_ping=True` sends a lightweight `SELECT 1` before reusing any connection, recycling it automatically if it's dead.

---

## License

MIT License. See `LICENSE` for details.
