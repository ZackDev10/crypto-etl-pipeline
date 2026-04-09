import logging
import pandas as pd
from sqlalchemy import (
    Column, String, Integer, Numeric, DateTime, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database import Base, get_engine, verify_connection

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SECTION 1: Table Definition (ORM Model)
# ─────────────────────────────────────────────

class CoinMarketData(Base):
    """
    ORM model mapping to the `coin_market_data` PostgreSQL table.

    WHY define the schema in Python instead of raw SQL?
    The ORM model is version-controllable, portable, and self-documenting.
    Your table schema lives in the same repo as your pipeline code.
    If a teammate clones the project, `create_all()` builds the DB for them
    automatically — no separate SQL setup script required.
    """
    __tablename__ = "coin_market_data"

    # --- Surrogate primary key ---
    # surrogate key (id column) a simple auto-increment integer PK is unambiguous and fast.
    id = Column(Integer, primary_key=True, autoincrement=True)

    coin_id        = Column(String(50),        nullable=False)
    symbol         = Column(String(10),        nullable=False)
    name           = Column(String(100),       nullable=False)

    current_price  = Column(Numeric(30, 10),   nullable=False)
    high_24h       = Column(Numeric(30, 10),   nullable=False)
    low_24h        = Column(Numeric(30, 10),   nullable=False)

    price_change_24h             = Column(Numeric(30, 10))
    price_change_percentage_24h  = Column(Numeric(10, 6))

    market_cap                        = Column(Numeric(30, 10), nullable=False)
    market_cap_rank                   = Column(Integer)
    market_cap_change_24h             = Column(Numeric(30, 10))
    market_cap_change_percentage_24h  = Column(Numeric(10, 6))

    total_volume       = Column(Numeric(30, 10), nullable=False)
    circulating_supply = Column(Numeric(30, 10))
    total_supply       = Column(Numeric(30, 10))
    max_supply         = Column(Numeric(30, 10))

    ath                    = Column(Numeric(30, 10))
    ath_change_percentage  = Column(Numeric(10, 6))
    ath_date               = Column(DateTime(timezone=True))

    last_updated  = Column(DateTime(timezone=True), nullable=False)
    ingested_at   = Column(DateTime(timezone=True), nullable=False)


    __table_args__ = (
        UniqueConstraint(
            "coin_id",
            "last_updated",
            name="uq_coin_last_updated"
        ),
    )


# ─────────────────────────────────────────────
# SECTION 2: Loading Logic
# ─────────────────────────────────────────────

def initialize_database(engine) -> None:
    """
    Creates all tables defined under Base if they don't already exist.
    Safe to call on every pipeline run — `checkfirst=True` is the default.
    """
    Base.metadata.create_all(engine)
    logger.info("Database schema verified/created.")


def bulk_load(df: pd.DataFrame, engine) -> int:
    """
    Loads a transformed DataFrame into PostgreSQL using an upsert strategy.

    WHY upsert (INSERT ... ON CONFLICT DO NOTHING) over plain INSERT?
    Plain INSERT raises an IntegrityError if a duplicate hits the unique
    constraint. ON CONFLICT DO NOTHING skips the duplicate silently and
    keeps going — making your pipeline idempotent. Run it 10 times, get
    the same result as running it once.

    WHY not df.to_sql()?
    pandas' to_sql() is convenient but gives you no control over conflict
    handling. It also doesn't support ON CONFLICT natively. For any
    production load, you want explicit control over what happens on duplicates.

    Returns:
        Number of rows actually inserted (excluding skipped duplicates).
    """
    # Renaming `id` from DataFrame → `coin_id` to match ORM column name
    records = df.rename(columns={"id": "coin_id"}).to_dict(orient="records")

    stmt = pg_insert(CoinMarketData).values(records)

    # ON CONFLICT: if coin_id + last_updated already exists, skip the row
    upsert_stmt = stmt.on_conflict_do_nothing(
        index_elements=["coin_id", "last_updated"]
    )

    with engine.begin() as conn:   # engine.begin() auto-commits on success,
                                   # auto-rolls back on any exception — atomic.
        result = conn.execute(upsert_stmt)
        rows_inserted = result.rowcount

    logger.info(f"Bulk load complete. Rows inserted: {rows_inserted} / {len(records)} attempted.")
    return rows_inserted


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    from extract import fetch_market_data
    from transform import transform

    engine = get_engine()

    if not verify_connection(engine):
        raise SystemExit("Cannot reach database. Check your .env credentials.")

    initialize_database(engine)

    raw   = fetch_market_data()
    df    = transform(raw)
    count = bulk_load(df, engine)

    print(f"\n✓ Pipeline test complete. {count} rows inserted into coin_market_data.")
