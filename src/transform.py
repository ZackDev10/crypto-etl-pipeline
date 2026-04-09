import logging
import pandas as pd
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SECTION 1: Pydantic Model (Schema + Validation)
# ─────────────────────────────────────────────

class CoinMarketRecord(BaseModel):
    """
    Validates and coerces a single raw CoinGecko market record.

    WHY Pydantic here, not just pandas?
    Pandas will happily load a DataFrame with a column of mixed types or
    silently convert None to NaN. Pydantic rejects a bad record loudly and
    immediately, before it can corrupt your database. Think of it as a
    contract: if the API starts sending unexpected data, your pipeline
    fails fast with a clear error instead of silently writing garbage.
    """

    # --- Identity ---
    id: str
    symbol: str
    name: str

    # --- Pricing ---
    current_price: float
    high_24h: float
    low_24h: float
    price_change_24h: float
    price_change_percentage_24h: float

    # --- Market Size ---
    market_cap: float
    market_cap_rank: int
    market_cap_change_24h: float
    market_cap_change_percentage_24h: float
    total_volume: float

    # --- Supply ---
    circulating_supply: float
    total_supply: Optional[float] = None   # Some coins have no fixed supply
    max_supply: Optional[float] = None

    # --- All-Time High ---
    ath: float
    ath_change_percentage: float
    ath_date: datetime                     # Pydantic auto-parses ISO strings → datetime

    # --- Timestamps ---
    last_updated: datetime                 # Same — automatic ISO 8601 parsing

    # model_config Pydantic v2 requires this to allow extra fields in the
    # raw JSON (like `image`, `roi`, `atl_*`) to be silently ignored instead
    # of raising a validation error.
    model_config = {"extra": "ignore"}

    @field_validator("current_price", "market_cap", "total_volume", mode="before")
    @classmethod
    def must_be_positive(cls, v: float) -> float:
        """
        Core business rule: price, market cap, and volume cannot be zero or
        negative. A zero price would indicate a bad API response or a data
        pipeline bug — we want to catch this explicitly, not load it.
        """
        if v is not None and v <= 0:
            raise ValueError(f"Expected a positive value, got {v}")
        return v


# ─────────────────────────────────────────────
# SECTION 2: Transformation Logic
# ─────────────────────────────────────────────

# Columns keep in the final DataFrame (in order)
FINAL_COLUMNS = [
    "id", "symbol", "name",
    "current_price", "high_24h", "low_24h",
    "price_change_24h", "price_change_percentage_24h",
    "market_cap", "market_cap_rank",
    "market_cap_change_24h", "market_cap_change_percentage_24h",
    "total_volume",
    "circulating_supply", "total_supply", "max_supply",
    "ath", "ath_change_percentage", "ath_date",
    "last_updated",
    "ingested_at",      # Our own audit column added during transform
]


def validate_records(raw_records: list[dict]) -> tuple[list[CoinMarketRecord], list[dict]]:
    """
    Runs each raw record through the Pydantic model.

    WHY return both valid AND failed records?
    In production ETL, you never silently drop bad data. You route it to a
    'dead letter' store for investigation. Here we return failed records so
    the orchestrator can log and handle them explicitly.

    Returns:
        A tuple of (valid_records, failed_records)
    """
    valid, failed = [], []

    for record in raw_records:
        try:
            valid.append(CoinMarketRecord(**record))
        except ValidationError as e:
            logger.warning(f"Validation failed for record '{record.get('id', 'unknown')}': {e}")
            failed.append(record)

    logger.info(f"Validation complete — {len(valid)} passed, {len(failed)} failed.")
    return valid, failed


def transform(raw_records: list[dict]) -> pd.DataFrame:
    """
    Full transformation pipeline: validate → build DataFrame → clean → audit.

    Args:
        raw_records: List of raw dicts from the CoinGecko API.

    Returns:
        A clean, typed DataFrame ready for loading.

    Raises:
        ValueError: If every record fails validation (nothing to load).
    """
    # --- Step 1: Validate ---
    valid_records, failed_records = validate_records(raw_records)

    if not valid_records:
        raise ValueError("No valid records survived validation. Aborting transform.")

    if failed_records:

        logger.warning(f"{len(failed_records)} record(s) were dropped before loading.")

    # --- Step 2: Build DataFrame from validated Pydantic models ---
    #.model_dump() Pydantic models aren't DataFrames. model_dump()
    # serializes the validated, coerced model back to a dict — now with correct
    # Python types (datetime objects, floats, etc.) instead of raw strings.
    df = pd.DataFrame([r.model_dump() for r in valid_records])

    # --- Step 3: Add ingestion audit column ---
    #timezone.utc always store timestamps in UTC in database.
    # Never store local time — it breaks when servers move regions or DST shifts.
    df["ingested_at"] = datetime.now(timezone.utc)

    # --- Step 4: Enforce column selection and order ---
    df = df[FINAL_COLUMNS]

    # --- Step 5: Final dtype enforcement ---
    # Pydantic already coerced types, but we be explicit for pandas' benefit
    df["market_cap_rank"] = df["market_cap_rank"].astype(int)
    df["symbol"] = df["symbol"].str.upper()   

    logger.info(f"Transform complete. Output shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")

    return df


if __name__ == "__main__":
    # Test transform.py in isolation using live data from extract.py
    from extract import fetch_market_data

    raw = fetch_market_data()
    df = transform(raw)

    print("\n--- DataFrame Info ---")
    print(df.dtypes)
    print("\n--- Sample Row ---")
    print(df.iloc[0].to_dict())
