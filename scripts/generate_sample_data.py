"""Regenerate data/sample/: small, fake, deterministic data with the exact
shapes the real sources deliver.

    python scripts/generate_sample_data.py

- exchange_rates/  one exchangeratesapi.io-style response per day (USD-based,
                   plus one legacy EUR-based day to exercise the rebasing model)
- metal_prices/    one metalpriceapi.com-style response per day
- equity_prices/   one Alpha Vantage TIME_SERIES_DAILY payload for all tickers
                   (each real pull repeats the whole history, so one file)
- internal_ledger/ a few generated ledger batches, one per day

All numbers come from a seeded random walk; none of it is market data.
"""
import json
import os
import random
import shutil
import sys
from datetime import date, datetime, time, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "airflow", "dags"))

from pipelines.ledger import build_ledger_rows, validate_ledger, write_ledger_csv  # noqa: E402
from pipelines.market_data import TICKERS  # noqa: E402

OUT = os.path.join(ROOT, "data", "sample")
START = date(2026, 9, 1)
DAYS = 14
LEDGER_DAYS = 5
LEDGER_ROWS_PER_BATCH = 200

rng = random.Random(42)


def trading_days():
    d = START
    for _ in range(DAYS):
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def all_days():
    return [START + timedelta(days=i) for i in range(DAYS)]


def walk(start, n, vol):
    values, v = [], start
    for _ in range(n):
        v *= 1 + rng.gauss(0, vol)
        values.append(v)
    return values


def epoch(d):
    return int(datetime.combine(d, time(23, 59, 59), timezone.utc).timestamp())


def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1)


def exchange_rates():
    days = all_days()
    sar = walk(3.75, len(days), 0.0002)
    gbp = walk(0.75, len(days), 0.003)
    chf = walk(0.83, len(days), 0.003)
    eur = walk(0.86, len(days), 0.003)
    for i, d in enumerate(days):
        rates = {"CHF": round(chf[i], 6), "GBP": round(gbp[i], 6), "SAR": round(sar[i], 6), "USD": 1}
        data = {"success": True, "timestamp": epoch(d), "base": "USD", "date": d.isoformat(), "rates": rates}
        if i == 0:
            # Legacy shape: the API's own EUR base, before rebasing moved into the fetch task
            usd_per_eur = 1 / eur[i]
            data["base"] = "EUR"
            data["rates"] = {k: round(v * usd_per_eur, 6) for k, v in rates.items()}
        write_json(os.path.join(OUT, "exchange_rates", f"{d.isoformat()}.json"), data)


def metal_prices():
    days = all_days()
    series = {
        "XAU": walk(4250, len(days), 0.008),
        "XAG": walk(63, len(days), 0.012),
        "XPT": walk(1760, len(days), 0.01),
        "XPD": walk(1290, len(days), 0.012),
    }
    for i, d in enumerate(days):
        rates = {}
        for metal, prices in series.items():
            rates[f"USD{metal}"] = round(prices[i], 10)  # USD per troy ounce
            rates[metal] = round(1 / prices[i], 10)  # ounces per USD
        data = {"success": True, "base": "USD", "timestamp": epoch(d), "rates": dict(sorted(rates.items()))}
        write_json(os.path.join(OUT, "metal_prices", f"{d.isoformat()}.json"), data)


def equity_prices():
    days = list(trading_days())
    start_prices = {"AAPL": 280, "MSFT": 510, "SPY": 770, "GLD": 395, "GOOGL": 340, "IBM": 225, "SAP": 210, "VOD": 16}
    payload = {}
    for ticker in TICKERS:
        closes = walk(start_prices[ticker], len(days), 0.012)
        series = {}
        for d, close in zip(days, closes):
            open_ = close * (1 + rng.gauss(0, 0.004))
            series[d.isoformat()] = {
                "1. open": f"{open_:.4f}",
                "2. high": f"{max(open_, close) * (1 + abs(rng.gauss(0, 0.004))):.4f}",
                "3. low": f"{min(open_, close) * (1 - abs(rng.gauss(0, 0.004))):.4f}",
                "4. close": f"{close:.4f}",
                "5. volume": str(rng.randint(2_000_000, 60_000_000)),
            }
        payload[ticker] = {
            "Meta Data": {
                "1. Information": "Daily Prices (open, high, low, close) and Volumes",
                "2. Symbol": ticker,
                "3. Last Refreshed": days[-1].isoformat(),
                "4. Output Size": "Compact",
                "5. Time Zone": "US/Eastern",
            },
            # newest first, like the real API
            "Time Series (Daily)": dict(sorted(series.items(), reverse=True)),
        }
    write_json(os.path.join(OUT, "equity_prices", f"{days[-1].isoformat()}.json"), payload)


def internal_ledger():
    for i in range(LEDGER_DAYS):
        start = datetime.combine(START + timedelta(days=i * 3), time(9, 0), timezone.utc)
        ts_nodash = start.strftime("%Y%m%dT%H%M%S")
        rows = build_ledger_rows(start, ts_nodash, LEDGER_ROWS_PER_BATCH)
        validate_ledger(rows, LEDGER_ROWS_PER_BATCH)
        write_ledger_csv(rows, os.path.join(OUT, "internal_ledger", f"{ts_nodash}.csv"))


if __name__ == "__main__":
    shutil.rmtree(OUT, ignore_errors=True)
    exchange_rates()
    metal_prices()
    equity_prices()
    internal_ledger()
    print(f"Sample data written to {OUT}")
