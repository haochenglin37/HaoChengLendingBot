#!/usr/bin/env python3
"""
Fetch Bitfinex public funding trades for a given symbol over the past N seconds
and store the result as JSON for manual inspection.
"""
import argparse
import json
import sys
import time
from pathlib import Path

import requests

API_URL = "https://api-pub.bitfinex.com/v2/trades/{symbol}/hist"


def fetch_trades(symbol, window_seconds, limit):
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - (window_seconds * 1000)
    all_trades = []

    while start_ms < now_ms:
        params = {
            "start": start_ms,
            "end": now_ms,
            "limit": limit,
            "sort": 1
        }
        url = API_URL.format(symbol=symbol)
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        trades = response.json()
        if not isinstance(trades, list) or len(trades) == 0:
            break
        all_trades.extend(trades)
        if len(trades) < limit:
            break
        # Continue from the last timestamp returned (inclusive)
        start_ms = trades[-1][1]
        time.sleep(0.2)  # be gentle to the API
    return all_trades


def main():
    parser = argparse.ArgumentParser(description="Fetch Bitfinex funding trades snapshot.")
    parser.add_argument("--symbol", default="fUSD", help="Funding symbol to download, e.g. fUSD")
    parser.add_argument("--window", type=int, default=4 * 60 * 60,
                        help="Window size in seconds (default 4h)")
    parser.add_argument("--limit", type=int, default=1000, help="REST page size")
    parser.add_argument("--output", default="market_data/funding_snapshot.json",
                        help="Where to store the JSON output")
    args = parser.parse_args()

    try:
        trades = fetch_trades(args.symbol, args.window, args.limit)
    except requests.RequestException as exc:
        print(f"Failed to fetch trades: {exc}", file=sys.stderr)
        sys.exit(1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "trade_id": t[0],
            "timestamp": t[1],
            "amount": t[2],
            "rate": t[3],
            "duration": t[4],
        }
        for t in trades
    ]
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    print(f"Saved {len(payload)} trades to {output_path}")


if __name__ == "__main__":
    main()
