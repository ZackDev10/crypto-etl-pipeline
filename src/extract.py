import os
import requests
import logging
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# --- Constants ---
BASE_URL = os.getenv("COINGECKO_BASE_URL")

COINS_TO_TRACK = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "ripple",
]


def fetch_market_data(coins: list[str] = COINS_TO_TRACK) -> list[dict]:
    """
    Fetches current market data for specified coins from CoinGecko.

    Args:
        coins: List of CoinGecko coin IDs to fetch.

    Returns:
        A list of raw market data dicts, one per coin.

    Raises:
        requests.HTTPError: If the API returns a non-2xx status.
        requests.ConnectionError: If the network is unreachable.
    """
    endpoint = f"{BASE_URL}/coins/markets"

    params = {
        "vs_currency": "usd",
        "ids": ",".join(coins),
        "order": "market_cap_desc",
        "per_page": len(coins),
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "24h",
    }

    logger.info(f"Fetching market data for {len(coins)} coins: {coins}")

    try:
        # Without timeout, script can hang forever on a slow API.
        response = requests.get(endpoint, params=params, timeout=10)

        # raise_for_status() converts any 4xx/5xx HTTP response into a
        response.raise_for_status()

        data = response.json()

        logger.info(f"Successfully fetched {len(data)} records.")
        return data

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error from CoinGecko API: {e}")
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Network connection error: {e}")
        raise
    except requests.exceptions.Timeout:
        logger.error("Request to CoinGecko timed out after 10 seconds.")
        raise


if __name__ == "__main__":

    raw_data = fetch_market_data()

    import json
    print(json.dumps(raw_data[0], indent=2))
