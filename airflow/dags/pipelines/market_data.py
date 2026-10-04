import glob
import json
import os
import time
from functools import lru_cache

import requests

from pipelines.config import MARKET_DATA_SOURCE, SAMPLE_DIR
from pipelines.landing import land_json

API_KEY_NAMES = ("EXCHANGE_RATES_API_KEY", "METAL_PRICE_API_KEY", "ALPHA_VANTAGE_API_KEY")


# Read on first use by a task, not at import: a module-level call would run on
# every DAG parse and drop the DAG if the secret store were unreachable.
@lru_cache(maxsize=1)
def get_secrets():
    # Environment variables first (.env locally); otherwise AWS Secrets Manager
    # (the original deployment ran on EC2 with an IAM role).
    if all(os.environ.get(k) for k in API_KEY_NAMES):
        return {k: os.environ[k] for k in API_KEY_NAMES}

    secret_name = os.getenv("AWS_SECRET_NAME")
    if not secret_name:
        raise RuntimeError(
            f"Set {', '.join(API_KEY_NAMES)} (or AWS_SECRET_NAME), "
            "or use MARKET_DATA_SOURCE=sample"
        )

    import boto3
    from botocore.exceptions import ClientError

    client = boto3.session.Session().client(
        service_name="secretsmanager", region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve secrets from AWS: {e}")
    return json.loads(response["SecretString"])


# The free plan of this API only serves plain HTTP
EXCHANGE_BASE_URL = "http://api.exchangeratesapi.io/v1/latest"
EXCHANGE_SYMBOLS = ["USD", "SAR", "GBP", "CHF"]
METAL_BASE_URL = "https://api.metalpriceapi.com/v1/latest"
METALS = {"XAU": "Gold", "XAG": "Silver", "XPT": "Platinum", "XPD": "Palladium"}
EQUITY_BASE_URL = "https://www.alphavantage.co/query"
TICKERS = ["AAPL", "MSFT", "SPY", "GLD", "GOOGL", "IBM", "SAP", "VOD"]

API_ENDPOINTS = {
    "exchange_rates": EXCHANGE_BASE_URL,
    "metal_prices": METAL_BASE_URL,
    "equity_prices": EQUITY_BASE_URL,
}


def _load_samples(source):
    """Sample mode: the fake API responses in data/sample/<source>/."""
    paths = sorted(glob.glob(os.path.join(SAMPLE_DIR, source, "*.json")))
    if not paths:
        raise FileNotFoundError(f"No sample files in {SAMPLE_DIR}/{source}")
    for path in paths:
        with open(path, encoding="utf-8") as f:
            yield os.path.basename(path), json.load(f)


def check_api_availability(**kwargs):
    """Fail fast if any upstream market data API is unreachable."""
    if MARKET_DATA_SOURCE == "sample":
        print("MARKET_DATA_SOURCE=sample: skipping API checks")
        return

    unavailable = []
    for name, url in API_ENDPOINTS.items():
        try:
            response = requests.get(url, timeout=10)
            print(f"{name} ({url}) responded with status {response.status_code}")
        except requests.exceptions.RequestException as exc:
            print(f"{name} ({url}) is unavailable: {exc}")
            unavailable.append(name)

    if unavailable:
        raise RuntimeError(f"Unavailable APIs: {', '.join(unavailable)}")


def validate_exchange_rates(data, symbols):
    if not data.get("success"):
        raise ValueError(f"Exchange rates API returned an error: {data}")

    rates = data.get("rates", {})
    invalid = [
        s for s in symbols
        if not isinstance(rates.get(s), (int, float)) or rates.get(s) <= 0
    ]
    if invalid:
        raise ValueError(f"Missing or invalid exchange rates for {invalid}: {rates}")


def validate_metal_prices(data, metals):
    if not data.get("success"):
        raise ValueError(f"Metal prices API returned an error: {data}")

    rates = data.get("rates", {})
    invalid = [
        m for m in metals
        if not isinstance(rates.get(m), (int, float)) or rates.get(m) <= 0
    ]
    if invalid:
        raise ValueError(f"Missing or invalid metal rates for {invalid}: {rates}")


def validate_equity_prices(raw_results, tickers):
    missing = [t for t in tickers if t not in raw_results]
    if missing:
        raise ValueError(f"Missing tickers in equity response: {missing}")

    for symbol, payload in raw_results.items():
        series = payload.get("Time Series (Daily)")
        if not series:
            raise ValueError(f"No daily time series for {symbol}: {payload}")

        for date, values in series.items():
            missing_fields = [
                f for f in ("1. open", "2. high", "3. low", "4. close", "5. volume")
                if f not in values
            ]
            if missing_fields:
                raise ValueError(f"Missing {missing_fields} for {symbol} on {date}")


def rebase_to_usd(data):
    """The free FX plan only returns EUR-based rates; triangulate through
    EUR->USD so every rate is USD-based, like the other sources.
    """
    eur_rates = data["rates"]
    usd_rate = eur_rates["USD"]
    return {
        **data,
        "base": "USD",
        "rates": {symbol: round(rate / usd_rate, 6) for symbol, rate in eur_rates.items()},
    }


# Each fetch returns the list of landed paths for the load task (via XCom).

def fetch_exchange_rates(**kwargs):
    if MARKET_DATA_SOURCE == "sample":
        landed = []
        for name, data in _load_samples("exchange_rates"):
            validate_exchange_rates(data, EXCHANGE_SYMBOLS)
            landed.append(land_json(data, f"exchange_rates/{name}"))
        return landed

    params = {
        "access_key": get_secrets()["EXCHANGE_RATES_API_KEY"],
        "symbols": ",".join(EXCHANGE_SYMBOLS),
    }
    response = requests.get(EXCHANGE_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()
    print("Exchange rates fetched successfully.")

    validate_exchange_rates(raw_data, EXCHANGE_SYMBOLS)
    usd_based = rebase_to_usd(raw_data)
    return [land_json(usd_based, f"exchange_rates/{kwargs['ds']}.json")]


def fetch_metal_prices(**kwargs):
    if MARKET_DATA_SOURCE == "sample":
        landed = []
        for name, data in _load_samples("metal_prices"):
            validate_metal_prices(data, METALS.keys())
            landed.append(land_json(data, f"metal_prices/{name}"))
        return landed

    params = {
        "api_key": get_secrets()["METAL_PRICE_API_KEY"],
        "base": "USD",
        "currencies": ",".join(METALS.keys()),
    }
    response = requests.get(METAL_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()

    validate_metal_prices(raw_data, METALS.keys())
    return [land_json(raw_data, f"metal_prices/{kwargs['ds']}.json")]


def fetch_daily_equity_prices(**kwargs):
    if MARKET_DATA_SOURCE == "sample":
        landed = []
        for name, data in _load_samples("equity_prices"):
            validate_equity_prices(data, TICKERS)
            landed.append(land_json(data, f"equity_prices/{name}"))
        return landed

    raw_results = {}
    for symbol in TICKERS:
        print(f"Fetching {symbol}...")
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": get_secrets()["ALPHA_VANTAGE_API_KEY"],
        }
        response = requests.get(EQUITY_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        raw_results[symbol] = response.json()

        time.sleep(15)  # free tier: 5 requests per minute

    validate_equity_prices(raw_results, TICKERS)
    return [land_json(raw_results, f"equity_prices/{kwargs['ds']}.json")]
