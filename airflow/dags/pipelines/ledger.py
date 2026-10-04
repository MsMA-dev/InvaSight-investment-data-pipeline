import csv
import glob
import os
import random
import tempfile
from datetime import datetime, timezone

from pipelines.config import LEDGER_ROWS, MARKET_DATA_SOURCE, SAMPLE_DIR
from pipelines.landing import land_file


# Synthetic clients: there is no real ledger, so the "source system" is this generator
CLIENTS = {f"C{i:03d}": f"Client {chr(64 + i)}" for i in range(1, 21)}  # C001 -> "Client A"
CLIENT_IDS = list(CLIENTS)
PORTFOLIO_IDS = [f"P{i:03d}" for i in range(1, 11)]

TICKER_CURRENCY = {
    "VOD": "USD",
    "SAP": "USD",
    "IBM": "USD",
    "XAU": "USD",
    "XAG": "USD",
    "XPT": "USD",
    "GOOGL": "USD",
    "MSFT": "USD",
}

TRANSACTION_TYPES = ["BUY", "SELL"]

LEDGER_COLUMNS = [
    "transaction_id",
    "client_id",
    "client_name",
    "portfolio_id",
    "ticker",
    "transaction_type",
    "quantity",
    "price",
    "currency",
    "fee_amount",
    "transaction_date",
    "transaction_ts",
    "total",
]


def validate_ledger(rows, expected_rows):
    """Fail fast if the generated ledger is incomplete or internally inconsistent."""
    if len(rows) != expected_rows:
        raise ValueError(f"Expected {expected_rows} rows, got {len(rows)}")

    for row in rows:
        for col in LEDGER_COLUMNS:
            if row.get(col) in (None, ""):
                raise ValueError(f"Missing {col} in row {row.get('transaction_id')}")

        if row["client_id"] not in CLIENT_IDS:
            raise ValueError(f"Unknown client_id {row['client_id']} in row {row['transaction_id']}")
        if row["client_name"] != CLIENTS[row["client_id"]]:
            raise ValueError(
                f"client_name {row['client_name']} does not match client_id {row['client_id']} "
                f"in row {row['transaction_id']}"
            )
        if row["portfolio_id"] not in PORTFOLIO_IDS:
            raise ValueError(f"Unknown portfolio_id {row['portfolio_id']} in row {row['transaction_id']}")
        if row["ticker"] not in TICKER_CURRENCY:
            raise ValueError(f"Unknown ticker {row['ticker']} in row {row['transaction_id']}")
        if row["currency"] != TICKER_CURRENCY[row["ticker"]]:
            raise ValueError(f"Currency mismatch for ticker {row['ticker']} in row {row['transaction_id']}")
        if row["transaction_type"] not in TRANSACTION_TYPES:
            raise ValueError(f"Unknown transaction_type {row['transaction_type']} in row {row['transaction_id']}")
        if row["quantity"] <= 0 or row["price"] <= 0 or row["fee_amount"] <= 0:
            raise ValueError(f"Non-positive quantity/price/fee_amount in row {row['transaction_id']}")

        gross_amount = row["quantity"] * row["price"]
        if row["transaction_type"] == "BUY":
            expected_total = gross_amount + row["fee_amount"]
        else:
            expected_total = -(gross_amount - row["fee_amount"])

        if round(expected_total, 2) != row["total"]:
            raise ValueError(
                f"Total mismatch in row {row['transaction_id']}: "
                f"expected {round(expected_total, 2)}, got {row['total']}"
            )

    print(f"Validated {len(rows)} rows.")


def build_ledger_rows(interval_start, ts_nodash, n_rows):
    """Deterministic rows for one interval: same seed -> same rows."""
    rng = random.Random(ts_nodash)

    transaction_ts = interval_start.isoformat()
    transaction_date = interval_start.strftime("%Y-%m-%d")

    rows = []
    for i in range(1, n_rows + 1):
        ticker = rng.choice(list(TICKER_CURRENCY.keys()))
        transaction_type = rng.choice(TRANSACTION_TYPES)

        quantity = rng.randint(9, 484)
        price = round(rng.uniform(66.06, 493.64), 2)
        fee_amount = round(rng.uniform(7.03, 636.55), 2)

        gross_amount = quantity * price
        if transaction_type == "BUY":
            total = gross_amount + fee_amount
        else:
            total = -(gross_amount - fee_amount)

        client_id = rng.choice(CLIENT_IDS)

        rows.append({
            "transaction_id": f"T{ts_nodash}{i:05d}",
            "client_id": client_id,
            "client_name": CLIENTS[client_id],
            "portfolio_id": rng.choice(PORTFOLIO_IDS),
            "ticker": ticker,
            "transaction_type": transaction_type,
            "quantity": quantity,
            "price": price,
            "currency": TICKER_CURRENCY[ticker],
            "fee_amount": fee_amount,
            "transaction_date": transaction_date,
            "transaction_ts": transaction_ts,
            "total": round(total, 2),
        })
    return rows


def write_ledger_csv(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEDGER_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def generate_internal_ledger(**kwargs):
    """Synthetic internal ledger for one 45-minute interval.

    Seeded from the interval start so a run is reproducible on retry or
    backfill, while each interval produces different rows. Falls back to the
    current time when triggered manually without a logical date.

    Returns the landed paths; the load task pulls them from XCom so it loads
    exactly these files.
    """
    interval_start = kwargs.get("data_interval_start") or datetime.now(timezone.utc)
    ts_nodash = kwargs.get("ts_nodash") or interval_start.strftime("%Y%m%dT%H%M%S")

    rows = build_ledger_rows(interval_start, ts_nodash, LEDGER_ROWS)
    validate_ledger(rows, LEDGER_ROWS)

    output_path = os.path.join(tempfile.gettempdir(), "internal_ledger", f"{ts_nodash}.csv")
    write_ledger_csv(rows, output_path)
    print(f"Created {len(rows)} transactions at {output_path}")

    landed = []
    # Sample mode also lands the historical sample batches, so the dashboard
    # tables have a few days of history to match the sample market data.
    # Already-loaded files are skipped by the warehouse load step.
    if MARKET_DATA_SOURCE == "sample":
        for path in sorted(glob.glob(os.path.join(SAMPLE_DIR, "internal_ledger", "*.csv"))):
            landed.append(land_file(path, f"internal_ledger/{os.path.basename(path)}"))

    landed.append(land_file(output_path, f"internal_ledger/{ts_nodash}.csv"))
    return landed
