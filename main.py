import os
import sys
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "src")

from extract import fetch_market_data
from transform import transform
from load import bulk_load, initialize_database
from database import get_engine, verify_connection


# ─────────────────────────────────────────────
# Logging Configuration
# ─────────────────────────────────────────────

LOG_FILE = Path(__file__).parent / "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ]
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Core Pipeline Function
# ─────────────────────────────────────────────

def run_pipeline() -> None:
    """
    Executes one full ETL cycle: Extract → Transform → Load.

    WHY wrap everything in one function instead of module-level code?
    A bare script (code at the top level of main.py) cannot be imported,
    tested, or scheduled without executing immediately. A function gives
    you a callable unit — the scheduler calls run_pipeline(), a test
    can mock run_pipeline(), and you can run it manually just as easily.
    """
    run_start = datetime.now(timezone.utc)
    logger.info("=" * 55)
    logger.info(f"ETL pipeline starting at {run_start.isoformat()}")
    logger.info("=" * 55)

    try:
        # ── 0. Startup: DB connection check ──────────────────
        logger.info("[0/3] Running startup checks...")
        engine = get_engine()

        if not verify_connection(engine):
            # Fail immediately — no point extracting data we can't load
            raise ConnectionError("Database unreachable. Aborting pipeline run.")

        initialize_database(engine)  # No-op if tables already exist

        # ── 1. Extract ────────────────────────────────────────
        logger.info("[1/3] Starting extraction...")
        raw_data = fetch_market_data()

        if not raw_data:
            raise ValueError("Extraction returned empty data. API may be down.")

        logger.info(f"      Extracted {len(raw_data)} raw records.")

        # ── 2. Transform ──────────────────────────────────────
        logger.info("[2/3] Starting transformation & validation...")
        df = transform(raw_data)
        logger.info(f"      {len(df)} records passed validation.")

        # ── 3. Load ───────────────────────────────────────────
        logger.info("[3/3] Starting load...")
        rows_inserted = bulk_load(df, engine)

        # ── Summary ───────────────────────────────────────────
        duration = (datetime.now(timezone.utc) - run_start).total_seconds()
        logger.info("=" * 55)
        logger.info(f"Pipeline complete in {duration:.2f}s")
        logger.info(f"  Records extracted : {len(raw_data)}")
        logger.info(f"  Records validated : {len(df)}")
        logger.info(f"  Rows inserted     : {rows_inserted}")
        logger.info("=" * 55)

    except Exception as e:
        # 1. Log the full error to your local file/terminal
        logger.exception(f"UNEXPECTED FAILURE — {e}")

        # 2. Grab the webhook URL from your environment
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

        # 3. If the URL exists, format and send the alert
        if webhook_url:
            slack_payload = {
                "text": f"🚨 *CRITICAL ALERT: Crypto ETL Pipeline Failed!* 🚨\n\n*Error Details:*\n`{e}`\n\n_Please check the GitHub Actions logs for the full traceback._"
            }

            try:
                # Send the POST request to Slack
                requests.post(webhook_url, json=slack_payload)
                logger.info("Slack alert dispatched successfully.")
            except Exception as slack_error:
                logger.error(f"Failed to send Slack alert: {slack_error}")

        # 4. Exit with a failure code so GitHub Actions knows it crashed
        sys.exit(1)

    except ValueError as e:
        logger.error(f"PIPELINE FAILURE — {e}")
        sys.exit(1)

    except Exception as e:
        # Catch-all for unexpected errors — log the full traceback
        logger.exception(f"UNEXPECTED FAILURE — {e}")
        sys.exit(1)


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    run_pipeline()
