#!/usr/bin/env python3
"""
Import funding trades from a JSON snapshot (produced by fetch_funding_trades.py)
into the local funding_trades SQLite database.
"""

import argparse
import json
import sqlite3
from decimal import Decimal
from pathlib import Path


def load_snapshot(path):
    with Path(path).open('r', encoding='utf-8') as handle:
        return json.load(handle)


def import_trades(snapshot_path, db_path, currency):
    trades = load_snapshot(snapshot_path)
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        for trade in trades:
            trade_id = str(trade["trade_id"])
            timestamp = int(int(trade["timestamp"]) / 1000)
            amount = abs(Decimal(str(trade["amount"])))
            rate = Decimal(str(trade["rate"]))  # already daily rate in snapshot
            duration = int(trade["duration"])

            cursor.execute(
                "INSERT OR IGNORE INTO funding_trades "
                "(loan_id, currency, rate, amount, duration, timestamp) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (trade_id, currency.upper(), float(rate), float(amount), duration, timestamp)
            )
        conn.commit()
    finally:
        conn.close()
    print(f"Imported {len(trades)} trades into {db_path}")


def main():
    parser = argparse.ArgumentParser(description="Import funding snapshot JSON into local DB.")
    parser.add_argument("--snapshot", default="market_data/funding_snapshot.json",
                        help="Path to JSON snapshot file")
    parser.add_argument("--db", default="market_data/BITFINEX-USD-funding.db",
                        help="Path to funding trades sqlite DB")
    parser.add_argument("--currency", default="USD", help="Currency symbol to insert (default USD)")
    args = parser.parse_args()

    import_trades(args.snapshot, args.db, args.currency)


if __name__ == "__main__":
    main()
