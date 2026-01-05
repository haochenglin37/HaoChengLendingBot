import os
import sys
import threading
import time
import traceback
import datetime
import calendar
import json
import math
from decimal import Decimal, ROUND_DOWN
import requests
import pandas as pd
import sqlite3 as sqlite
from sqlite3 import Error
from modules.ExchangeApi import ApiError

# Bot libs
import modules.Configuration as Config
from modules.Data import truncate
try:
    import numpy
    use_numpy = True
except ImportError as ex:
    print(("WARN: Module Numpy not found, using manual percentile method instead. "
          "It is recommended to install Numpy. Error: {0}".format(str(ex))))
    use_numpy = False

BUCKET_ORDER = ["2d", "3-7d", "8-30d", "31-120d", "120d+"]
DEFAULT_BUCKET_MULTIPLIERS = {
    "2d": 0.05,
    "3-7d": 0.10,
    "8-30d": 0.15,
    "31-120d": 0.20,
    "120d+": 0.25
}
DEFAULT_BUCKET_ALLOCATIONS = {
    "2d": 3,
    "3-7d": 3,
    "8-30d": 2,
    "31-120d": 2
}
DEFAULT_BUCKET_DURATIONS = {
    "2d": 2,
    "3-7d": 3,
    "8-30d": 8,
    "31-120d": 31,
    "120d+": 120
}


class FundingBucketPlanner(object):
    def __init__(self, allocations=None, durations=None, best_bucket_only=False):
        self.allocations = dict(DEFAULT_BUCKET_ALLOCATIONS)
        if allocations:
            self.allocations.update(allocations)
        self.durations = dict(DEFAULT_BUCKET_DURATIONS)
        if durations:
            self.durations.update(durations)
        self.best_bucket_only = best_bucket_only

    @staticmethod
    def _quantize_amount(amount):
        return Decimal(amount).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)

    def _split_amount(self, amount, min_size):
        amount = Decimal(amount)
        min_size = Decimal(min_size)
        if amount < min_size or min_size <= 0:
            return []
        slices = []
        remaining = amount
        while remaining >= min_size:
            slices.append(min_size)
            remaining -= min_size
        if remaining > Decimal('0') and slices:
            slices[-1] += remaining
        return [self._quantize_amount(value) for value in slices]

    def _select_best_bucket(self, recommendations):
        best_bucket = None
        best_rate = None
        for bucket in BUCKET_ORDER:
            rec = recommendations.get(bucket) if recommendations else None
            rate = rec.get('recommended') if rec else None
            if rate is None:
                continue
            if (best_rate is None or rate > best_rate or
                    (best_rate is not None and rate == best_rate and
                     self.durations.get(bucket, 2) < self.durations.get(best_bucket, 2))):
                best_bucket = bucket
                best_rate = rate
        return best_bucket

    def plan_orders(self, total_amount, min_loan_size, min_rate, recommendations):
        total_amount = Decimal(total_amount)
        min_loan_size = Decimal(min_loan_size)
        min_rate = Decimal(min_rate)
        if total_amount <= 0 or min_loan_size <= 0:
            return []
        selected_buckets = [bucket for bucket in BUCKET_ORDER if self.allocations.get(bucket, 0) > 0]
        if not selected_buckets:
            return []
        best_bucket = None
        if self.best_bucket_only:
            best_bucket = self._select_best_bucket(recommendations)
            if best_bucket and self.allocations.get(best_bucket, 0) > 0:
                selected_buckets = [best_bucket]
        total_weight = Decimal(sum(self.allocations.get(bucket, 0) for bucket in selected_buckets if self.allocations.get(bucket, 0) > 0))
        if total_weight <= 0:
            return []
        prepared = []
        for bucket in BUCKET_ORDER:
            if bucket not in selected_buckets:
                continue
            weight = Decimal(self.allocations.get(bucket, 0))
            if weight <= 0:
                continue
            rec = recommendations.get(bucket) if recommendations else None
            recommended_rate = rec.get('recommended') if rec else None
            if recommended_rate is None:
                continue
            if self.best_bucket_only:
                bucket_amount = total_amount
            else:
                bucket_amount = (total_amount * weight) / total_weight
            slices = self._split_amount(bucket_amount, min_loan_size)
            if not slices:
                continue
            duration = int(self.durations.get(bucket, 2))
            rate_value = Decimal(str(recommended_rate))
            rate_value = rate_value if rate_value >= min_rate else min_rate
            for slice_amount in slices:
                prepared.append({
                    "bucket": bucket,
                    "amount": slice_amount,
                    "rate": rate_value,
                    "duration": duration,
                    "source": rec.get('recommended_source')
                })
        return prepared

class FundingTradesRecorder(object):
    """
    Persists public funding trades for a single currency by using Bitfinex REST APIs.
    """

    def __init__(self, api, db_path, currency, poll_seconds, window_seconds, clean_interval, log=None,
                 debug=False, rest_limit=250, symbol=None, rest_polling_enabled=True,
                 rest_overlap_seconds=0, archive_enabled=False, archive_db_path=None,
                 bucket_percentile=95, bucket_multipliers=None, demand_depth=50):
        self.api = api
        self.db_path = db_path
        self.currency = currency.upper()
        self.symbol = symbol or 'f{0}'.format(self.currency)
        self.poll_seconds = poll_seconds
        self.window_seconds = window_seconds
        self.clean_interval = clean_interval
        self.log = log
        self.debug = debug
        self.rest_limit = rest_limit
        self.rest_polling_enabled = rest_polling_enabled
        self.rest_overlap_seconds = rest_overlap_seconds
        self.archive_enabled = archive_enabled
        self.archive_path = archive_db_path
        self.bucket_percentile = float(bucket_percentile) if bucket_percentile else 95.0
        self.bucket_multipliers = self._build_bucket_multipliers(bucket_multipliers or {})
        self.demand_depth = int(demand_depth) if demand_depth else 50
        self.conn = None
        self.archive_conn = None
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.clean_thread = None
        self.rest_thread = None
        self.known_ids = set()
        self.latest_mts = None
        self._ensure_storage()
        if self.archive_enabled:
            self._ensure_archive_storage()

    @staticmethod
    def _build_bucket_multipliers(overrides):
        multipliers = dict(DEFAULT_BUCKET_MULTIPLIERS)
        for bucket, value in overrides.items():
            try:
                multipliers[bucket] = float(value)
            except (TypeError, ValueError):
                continue
        return multipliers

    def _ensure_storage(self):
        directory = os.path.dirname(self.db_path)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory)
        with self.lock:
            if self.conn is None:
                self.conn = sqlite.connect(self.db_path, check_same_thread=False)
                self.conn.execute("PRAGMA journal_mode=wal")
                self._create_table()
                self._load_known_ids()
        if self.archive_enabled and self.archive_conn is None:
            self._ensure_archive_storage()

    def _ensure_archive_storage(self):
        if not self.archive_path:
            base, ext = os.path.splitext(self.db_path)
            self.archive_path = base + '-archive' + ext
        directory = os.path.dirname(self.archive_path)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory)
        self.archive_conn = sqlite.connect(self.archive_path, check_same_thread=False)
        self.archive_conn.execute("PRAGMA journal_mode=wal")
        self.archive_conn.execute(
            "CREATE TABLE IF NOT EXISTS funding_trades ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "loan_id TEXT,"
            "currency TEXT NOT NULL,"
            "rate REAL NOT NULL,"
            "amount REAL NOT NULL,"
            "duration INTEGER NOT NULL,"
            "timestamp INTEGER NOT NULL)"
        )
        self.archive_conn.commit()

    def _create_table(self):
        query = (
            "CREATE TABLE IF NOT EXISTS funding_trades ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "loan_id TEXT UNIQUE,"
            "currency TEXT NOT NULL,"
            "rate REAL NOT NULL,"
            "amount REAL NOT NULL,"
            "duration INTEGER NOT NULL,"
            "timestamp INTEGER NOT NULL)"
        )
        idx_query = "CREATE INDEX IF NOT EXISTS idx_ft_currency_ts ON funding_trades(currency, timestamp);"
        self.conn.execute(query)
        self.conn.execute(idx_query)
        self.conn.commit()

    def _load_known_ids(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT loan_id FROM funding_trades WHERE currency=?", (self.currency,))
        self.known_ids = set(row[0] for row in cursor.fetchall())

    def _store_trade_entry(self, loan_id, rate, amount, duration, timestamp):
        if not loan_id:
            return False
        if loan_id in self.known_ids:
            return False
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO funding_trades (loan_id, currency, rate, amount, duration, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (loan_id, self.currency, rate, amount, duration, timestamp)
        )
        if cursor.rowcount:
            self.known_ids.add(loan_id)
            return True
        return False

    def add_manual_trade(self, loan_id, rate, amount, duration, timestamp):
        with self.lock:
            inserted = self._store_trade_entry(loan_id, rate, amount, duration, timestamp)
            if inserted:
                self.conn.commit()

    def ingest_public_trades(self, trades):
        if not isinstance(trades, list):
            return
        inserted = 0
        with self.lock:
            for entry in trades:
                try:
                    if len(entry) < 5:
                        continue
                    trade_id = str(entry[0])
                    mts = int(entry[1]) // 1000
                    amount = abs(float(entry[2]))
                    rate = float(entry[3])
                    duration = int(entry[4])
                    if self._store_trade_entry(trade_id, rate, amount, duration, mts):
                        inserted += 1
                        self.latest_mts = max(self.latest_mts or mts, mts)
                except Exception as ex:
                    self._log("Failed to process funding trade entry: {0}".format(ex))
            if inserted:
                self.conn.commit()

    def _fetch_trades_http(self, start_ms, end_ms):
        url = "https://api-pub.bitfinex.com/v2/trades/{0}/hist".format(self.symbol)
        session = requests.Session()
        current_start = start_ms
        while current_start < end_ms and not self.stop_event.is_set():
            params = {
                "start": current_start,
                "end": end_ms,
                "limit": self.rest_limit,
                "sort": 1
            }
            try:
                response = session.get(url, params=params, timeout=30)
                response.raise_for_status()
                trades = response.json()
            except Exception as ex:
                self._log("Error fetching funding trades via REST: {0}".format(ex))
                break
            if not isinstance(trades, list) or len(trades) == 0:
                break
            yield trades
            if len(trades) < self.rest_limit:
                break
            current_start = trades[-1][1]
            time.sleep(0.2)

    def _backfill_rest(self):
        if not self.symbol:
            self._log("Symbol not configured, skipping REST backfill.")
            return
        end = int(time.time() * 1000)
        start = end - (self.window_seconds * 1000)
        for trades in self._fetch_trades_http(start, end):
            self.ingest_public_trades(trades)
        stats = self.get_bucket_stats(percentiles=[self.bucket_percentile])
        self._log("Funding trades REST backfill completed. Latest timestamp: {0}".format(self.latest_mts))
        self._log_bucket_summary("backfill", stats)
        self._log_rate_recommendations("backfill", stats)

    def _poll_recent_trades(self):
        if self.latest_mts:
            overlap = max(self.rest_overlap_seconds, 0)
            start_ts = max(self.latest_mts - overlap, 0)
            start_ms = int(start_ts * 1000)
        else:
            start_ms = int((time.time() - self.window_seconds) * 1000)
        end_ms = int(time.time() * 1000)
        total_inserted = 0
        bucket_stats = {}
        for trades in self._fetch_trades_http(start_ms, end_ms):
            before = len(self.known_ids)
            self.ingest_public_trades(trades)
            inserted_now = max(len(self.known_ids) - before, 0)
            total_inserted += inserted_now
            if inserted_now and self.debug:
                for entry in trades:
                    try:
                        duration = int(entry[4])
                        rate = float(entry[3])
                        bucket_key = self._bucket_for_duration(duration)
                        stats = bucket_stats.setdefault(bucket_key, {"count": 0, "rates": []})
                        stats["count"] += 1
                        stats["rates"].append(rate)
                    except Exception:
                        continue
        if total_inserted > 0 and self.debug:
            summary_stats = self.get_bucket_stats(percentiles=[self.bucket_percentile])
            self._log("REST poll inserted {0} trades, latest mts={1}".format(total_inserted, self.latest_mts))
            self._log_bucket_summary("poll", summary_stats)
            self._log_rate_recommendations("poll", summary_stats)

    def _bucket_for_duration(self, duration):
        duration = int(duration)
        if duration <= 2:
            return "2d"
        if duration <= 7:
            return "3-7d"
        if duration <= 30:
            return "8-30d"
        if duration <= 120:
            return "31-120d"
        return "120d+"

    def _get_bucket_rates(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT rate, duration FROM funding_trades WHERE currency=?", (self.currency,))
        rows = cursor.fetchall()
        bucket_rates = {key: [] for key in BUCKET_ORDER}
        for rate, duration in rows:
            bucket = self._bucket_for_duration(duration)
            bucket_rates.setdefault(bucket, []).append(rate)
        return bucket_rates

    def _percentile_value(self, values, percentile):
        if not values:
            return None
        pct = float(percentile)
        if use_numpy:
            try:
                return float(numpy.percentile(values, pct))
            except Exception:
                pass
        sorted_vals = sorted(values)
        if not sorted_vals:
            return None
        k = (len(sorted_vals) - 1) * (pct / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        d0 = sorted_vals[int(f)] * (c - k)
        d1 = sorted_vals[int(c)] * (k - f)
        return d0 + d1

    def _build_stats_for_rates(self, rates, percentiles):
        count = len(rates)
        entry = {
            "count": count,
            "min": None,
            "max": None,
            "avg": None,
            "std": None,
            "percentiles": {}
        }
        if count:
            entry["min"] = min(rates)
            entry["max"] = max(rates)
            entry["avg"] = sum(rates) / count
            if count > 1:
                mean = entry["avg"]
                variance = sum((r - mean) ** 2 for r in rates) / (count - 1)
                entry["std"] = variance ** 0.5
            else:
                entry["std"] = 0.0
        for pct in percentiles:
            entry["percentiles"][float(pct)] = self._percentile_value(rates, pct)
        return entry

    def get_bucket_stats(self, percentiles=None):
        percentiles = percentiles or []
        rates_map = self._get_bucket_rates()
        stats = {}
        for bucket in BUCKET_ORDER:
            stats[bucket] = self._build_stats_for_rates(rates_map.get(bucket, []), percentiles)
        return stats

    def _fetch_top_bid_rates(self):
        if not self.api:
            return {}
        try:
            order_book = self.api.return_loan_orders(self.currency, self.demand_depth)
        except Exception as ex:
            self._log("Unable to fetch loan orders for recommendations: {0}".format(ex))
            return {}
        demands = order_book.get('demands') or []
        bucket_rates = {bucket: None for bucket in BUCKET_ORDER}
        for entry in demands:
            try:
                rate = float(entry.get('rate'))
                duration_val = entry.get('rangeMax') or entry.get('rangeMin') or 2
                duration = int(float(duration_val))
            except (TypeError, ValueError):
                continue
            bucket = self._bucket_for_duration(duration)
            current = bucket_rates.get(bucket)
            if current is None or rate > current:
                bucket_rates[bucket] = rate
        return bucket_rates

    def get_rate_recommendations(self, percentile=None, stats=None):
        percentile = float(percentile if percentile is not None else self.bucket_percentile)
        needs_percentile = True
        if stats:
            sample = next(iter(stats.values()))
            needs_percentile = percentile not in sample.get('percentiles', {})
        if stats is None or needs_percentile:
            stats = self.get_bucket_stats(percentiles=[percentile])
        top_bids = self._fetch_top_bid_rates()
        recommendations = {}
        for bucket in BUCKET_ORDER:
            bucket_stats = stats.get(bucket, {"percentiles": {}, "count": 0})
            percentile_value = bucket_stats.get('percentiles', {}).get(percentile)
            top_bid = top_bids.get(bucket) if top_bids else None
            multiplier = self.bucket_multipliers.get(bucket, 0.0)
            target_from_bid = top_bid * (1 + multiplier) if top_bid is not None else None
            candidates = []
            if percentile_value is not None:
                candidates.append((percentile_value, 'percentile'))
            if target_from_bid is not None:
                candidates.append((target_from_bid, 'top_bid'))
            if candidates:
                recommended, selected_source = max(candidates, key=lambda x: x[0])
            else:
                recommended = None
                selected_source = None
            recommendations[bucket] = {
                "percentile": percentile_value,
                "top_bid": top_bid,
                "multiplier": multiplier,
                "recommended": recommended,
                "recommended_source": selected_source
            }
        return {"percentile": percentile, "stats": stats, "recommendations": recommendations}

    def _log_bucket_summary(self, label, stats=None):
        if not self.debug:
            return
        stats = stats or self.get_bucket_stats()
        if not stats:
            self._log("{0} bucket stats: no data".format(label))
            return
        row_fmt = "{:<7} | {:>5} | {:>10} | {:>10} | {:>10} | {:>10}"
        header = "{0} bucket stats (%):\n{1}".format(label,
                                                     row_fmt.format("bucket", "count", "min", "max", "avg", "std"))
        lines = [header]
        for bucket in BUCKET_ORDER:
            entry = stats.get(bucket, {"count": 0})
            count = entry.get("count", 0)
            if count > 0:
                min_rate = entry.get("min", 0)
                max_rate = entry.get("max", 0)
                avg_rate = entry.get("avg", 0)
                std_dev = entry.get("std", 0)
                line = row_fmt.format(
                    bucket,
                    count,
                    f"{min_rate * 100:.4f}",
                    f"{max_rate * 100:.4f}",
                    f"{avg_rate * 100:.4f}",
                    f"{std_dev * 100:.4f}",
                )
            else:
                line = row_fmt.format(bucket, 0, "-", "-", "-", "-")
            lines.append(line)
        self._log("\n".join(lines))

    def _log_rate_recommendations(self, label, stats=None):
        if not self.debug:
            return
        result = self.get_rate_recommendations(percentile=self.bucket_percentile, stats=stats)
        recs = result["recommendations"]
        percentile = result["percentile"]
        row_fmt = "{:<7} | {:>9} | {:>10} | {:>8} | {:>10}"
        header = "{0} target rates ({1:.0f}th pct vs top bid, %):\n{2}".format(
            label, percentile, row_fmt.format("bucket", "P{0:.0f}".format(percentile), "top bid", "boost", "target")
        )
        lines = [header]

        def fmt_rate(value):
            return f"{value * 100:.4f}" if value is not None else "-"

        for bucket in BUCKET_ORDER:
            info = recs.get(bucket, {})
            lines.append(row_fmt.format(
                bucket,
                fmt_rate(info.get("percentile")),
                fmt_rate(info.get("top_bid")),
                "{:+.0%}".format(info.get("multiplier", 0.0)),
                fmt_rate(info.get("recommended"))
            ))
        self._log("\n".join(lines))

    def _rest_poll_loop(self):
        while not self.stop_event.is_set():
            self._poll_recent_trades()
            self.stop_event.wait(self.poll_seconds)

    def _handle_ws_trade(self, trade):
        try:
            trade_id = str(trade[0])
            mts = int(trade[1]) // 1000
            amount = abs(float(trade[2]))
            rate = float(trade[3])
            duration = int(trade[4])
            with self.lock:
                if self._store_trade_entry(trade_id, rate, amount, duration, mts):
                    self.conn.commit()
                    self.latest_mts = max(self.latest_mts or mts, mts)
        except Exception as ex:
            self._log("Failed to process websocket trade: {0}".format(ex))

    def _start_websocket(self):
        if websocket is None:
            self._log("websocket-client not installed, skipping WebSocket streaming.")
            return

        def on_message(_ws, message):
            if self.stop_event.is_set():
                return
            try:
                data = json.loads(message)
            except ValueError:
                return
            if isinstance(data, dict):
                if data.get('event') == 'error':
                    self._log("Funding trades websocket error: {0}".format(data.get('msg')))
                return
            if not isinstance(data, list) or len(data) < 2:
                return
            payload = data[1]
            if payload == 'hb':
                return
            if isinstance(payload, str):
                if payload in ('tu', 'te') and len(data) > 2:
                    self._handle_ws_trade(data[2])
                return
            if isinstance(payload, list):
                for trade in payload:
                    self._handle_ws_trade(trade)

        def on_error(_ws, error):
            self._log("Funding trades websocket error: {0}".format(error))

        def on_close(_ws, *_args):
            self._log("Funding trades websocket disconnected, continuing without streaming.")

        def on_open(ws_app):
            subscribe_msg = json.dumps({"event": "subscribe", "channel": "trades", "symbol": self.symbol})
            ws_app.send(subscribe_msg)

        while not self.stop_event.is_set():
            ws = websocket.WebSocketApp(
                "wss://api-pub.bitfinex.com/ws/2",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever()
            if self.stop_event.is_set():
                break
            self.stop_event.wait(self.poll_seconds)

    def _cleanup_once(self):
        cutoff = int(time.time()) - self.window_seconds
        cursor = self.conn.cursor()
        cursor.execute("SELECT loan_id, currency, rate, amount, duration, timestamp "
                       "FROM funding_trades WHERE currency=? AND timestamp < ?",
                       (self.currency, cutoff))
        rows = cursor.fetchall()
        if not rows:
            return
        if self.archive_enabled and self.archive_conn:
            self.archive_conn.executemany(
                "INSERT INTO funding_trades (loan_id, currency, rate, amount, duration, timestamp) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows
            )
            self.archive_conn.commit()
        cursor.execute("DELETE FROM funding_trades WHERE currency=? AND timestamp < ?",
                       (self.currency, cutoff))
        self.conn.commit()
        for loan_id, _, _, _, _, _ in rows:
            self.known_ids.discard(loan_id)

    def cleanup_once(self):
        with self.lock:
            self._cleanup_once()

    def _cleanup_loop(self):
        while not self.stop_event.is_set():
            self.cleanup_once()
            self.stop_event.wait(self.clean_interval)

    def start(self):
        if self.rest_thread and self.rest_thread.is_alive():
            return
        self.stop_event.clear()
        self._backfill_rest()
        if self.rest_polling_enabled:
            self.rest_thread = threading.Thread(target=self._rest_poll_loop)
            self.rest_thread.daemon = True
            self.rest_thread.start()
        else:
            self._log("REST funding polling disabled.")
        self.clean_thread = threading.Thread(target=self._cleanup_loop)
        self.clean_thread.daemon = True
        self.clean_thread.start()

    def stop(self):
        self.stop_event.set()
        if self.clean_thread:
            self.clean_thread.join(timeout=1)
        if self.rest_thread:
            self.rest_thread.join(timeout=1)
        if self.archive_conn:
            self.archive_conn.close()

    def _log(self, message):
        if self.log:
            try:
                self.log.log(message)
            except AttributeError:
                self.log(message)
        elif self.debug:
            print(message)

# Improvements
# [ ] Provide something that takes into account dust offers. (The golden cross works well on BTC, not slower markets)
# [ ] RE: above. Weighted rate.
# [ ] Add docstring to everything
# [ ] Unit tests

# NOTES
# * A possible solution for the dust problem is take the top 10 offers and if the offer amount is less than X% of the
#   total available, ignore it as dust.


class MarketDataException(Exception):
    pass


class MarketAnalysis(object):
    def __init__(self, config, api):
        self.currencies_to_analyse = config.get_currencies_list('analyseCurrencies', 'MarketAnalysis')
        self.update_interval = int(config.get('MarketAnalysis', 'analyseUpdateInterval', 10, 1, 3600))
        self.api = api
        self.lending_style = int(config.get('MarketAnalysis', 'lendingStyle', 75, 1, 99))
        self.recorded_levels = 10
        self.modules_dir = os.path.dirname(os.path.realpath(__file__))
        self.top_dir = os.path.dirname(self.modules_dir)
        self.db_dir = os.path.join(self.top_dir, 'market_data')
        self.recorded_levels = int(config.get('MarketAnalysis', 'recorded_levels', 3, 1, 100))
        self.data_tolerance = float(config.get('MarketAnalysis', 'data_tolerance', 15, 10, 90))
        self.ma_debug_log = config.getboolean('MarketAnalysis', 'ma_debug_log')
        self.MACD_long_win_seconds = int(config.get('MarketAnalysis', 'MACD_long_win_seconds',
                                                    60 * 30 * 1 * 1,
                                                    60 * 1 * 1 * 1,
                                                    60 * 60 * 24 * 7))
        self.percentile_seconds = int(config.get('MarketAnalysis', 'percentile_seconds',
                                                 60 * 60 * 24 * 1,
                                                 60 * 60 * 1 * 1,
                                                 60 * 60 * 24 * 14))
        if self.MACD_long_win_seconds > self.percentile_seconds:
            keep_sec = self.MACD_long_win_seconds
        else:
            keep_sec = self.percentile_seconds
        self.keep_history_seconds = int(config.get('MarketAnalysis', 'keep_history_seconds',
                                                   int(keep_sec * 1.1),
                                                   int(keep_sec * 1.1),
                                                   60 * 60 * 24 * 14))
        self.MACD_short_win_seconds = int(config.get('MarketAnalysis', 'MACD_short_win_seconds',
                                                     int(self.MACD_long_win_seconds / 12),
                                                     1,
                                                     self.MACD_long_win_seconds / 2))
        self.daily_min_multiplier = float(config.get('Daily_min', 'multiplier', 1.05, 1))
        self.delete_thread_sleep = float(config.get('MarketAnalysis', 'delete_thread_sleep',
                                                    self.keep_history_seconds / 2,
                                                    60,
                                                    60 * 60 * 2))
        self.exchange = config.get_exchange()
        self.collect_funding_trades = config.getboolean('MarketAnalysis', 'collectFundingTrades', False)
        self.funding_currency = config.get('MarketAnalysis', 'fundingCurrency', 'USD').upper()
        self.funding_poll_seconds = int(config.get('MarketAnalysis', 'fundingPollSeconds', 60, 5, 3600))
        self.funding_window_seconds = int(config.get('MarketAnalysis', 'fundingWindowSeconds', 60 * 60 * 4, 60,
                                                     60 * 60 * 24))
        self.funding_clean_interval = int(
            config.get('MarketAnalysis', 'fundingCleanInterval',
                       max(self.funding_window_seconds / 2, 60), 60, 60 * 60 * 24))
        self.funding_use_websocket = config.getboolean('MarketAnalysis', 'fundingUseWebSocket', False)
        self.funding_rest_polling = config.getboolean('MarketAnalysis', 'fundingRestPolling', True)
        self.funding_rest_limit = int(config.get('MarketAnalysis', 'fundingRestLimit', 1000, 25, 1000))
        self.funding_rest_overlap = int(config.get('MarketAnalysis', 'fundingRestOverlapSeconds', 60, 0, 3600))
        self.funding_archive_enabled = config.getboolean('MarketAnalysis', 'fundingArchive', False)
        self.funding_archive_path = config.get('MarketAnalysis', 'fundingArchivePath', '')
        self.funding_symbol = config.get('MarketAnalysis', 'fundingSymbol',
                                         'f{0}'.format(self.funding_currency.upper()))
        self.funding_bucket_strategy = config.getboolean('MarketAnalysis', 'fundingBucketStrategy', False)
        default_multiplier_cfg = ",".join("{0}:{1}".format(bucket, DEFAULT_BUCKET_MULTIPLIERS[bucket])
                                          for bucket in BUCKET_ORDER)
        raw_multiplier_cfg = config.get('MarketAnalysis', 'fundingBucketMultipliers', default_multiplier_cfg)
        self.funding_bucket_percentile = float(config.get('MarketAnalysis', 'fundingBucketPercentile', 95, 50, 99))
        self.funding_bucket_multipliers = self._parse_bucket_multipliers(raw_multiplier_cfg)
        self.funding_demand_depth = int(config.get('MarketAnalysis', 'fundingDemandDepth', 50, 1, 500))
        alloc_default_cfg = ",".join("{0}:{1}".format(bucket, DEFAULT_BUCKET_ALLOCATIONS[bucket])
                                     for bucket in DEFAULT_BUCKET_ALLOCATIONS)
        duration_default_cfg = ",".join("{0}:{1}".format(bucket, DEFAULT_BUCKET_DURATIONS[bucket])
                                        for bucket in DEFAULT_BUCKET_DURATIONS)
        alloc_overrides = self._parse_bucket_value_string(
            config.get('MarketAnalysis', 'fundingBucketAllocations', alloc_default_cfg), int)
        duration_overrides = self._parse_bucket_value_string(
            config.get('MarketAnalysis', 'fundingBucketDurations', duration_default_cfg), int)
        self.funding_bucket_allocations = dict(DEFAULT_BUCKET_ALLOCATIONS)
        self.funding_bucket_allocations.update(alloc_overrides)
        self.funding_bucket_durations = dict(DEFAULT_BUCKET_DURATIONS)
        self.funding_bucket_durations.update(duration_overrides)
        self.funding_best_bucket_only = config.getboolean('MarketAnalysis', 'fundingBestBucketOnly', False)
        self.funding_bucket_planner = FundingBucketPlanner(self.funding_bucket_allocations,
                                                           self.funding_bucket_durations,
                                                           best_bucket_only=self.funding_best_bucket_only)
        self.funding_recorder = None
        if not os.path.isdir(self.db_dir):
            os.makedirs(self.db_dir)
        all_currencies = config.get_all_currencies()
        if self.collect_funding_trades and self.funding_currency not in all_currencies:
            raise ValueError("fundingCurrency must be one of {0}".format(all_currencies))

        if len(self.currencies_to_analyse) != 0:
            for currency in self.currencies_to_analyse:
                try:
                    self.api.return_loan_orders(currency, 5)
                except Exception as cur_ex:
                    raise Exception("ERROR: You entered an incorrect currency: '{0}' to analyse the market of, please "
                                    "check your settings. Error message: {1}".format(currency, cur_ex))
                time.sleep(2)

    @staticmethod
    def _parse_bucket_multipliers(raw_value):
        result = {}
        if not raw_value:
            return result
        entries = raw_value.split(',')
        for entry in entries:
            if ':' not in entry:
                continue
            bucket, value = entry.split(':', 1)
            bucket_key = bucket.strip()
            if bucket_key not in BUCKET_ORDER:
                continue
            try:
                result[bucket_key] = float(value.strip())
            except ValueError:
                continue
        return result

    @staticmethod
    def _parse_bucket_value_string(raw_value, cast_func):
        results = {}
        if not raw_value:
            return results
        entries = raw_value.split(',')
        for entry in entries:
            if ':' not in entry:
                continue
            bucket, value = entry.split(':', 1)
            key = bucket.strip()
            if key not in BUCKET_ORDER and key not in DEFAULT_BUCKET_ALLOCATIONS:
                continue
            try:
                results[key] = cast_func(value.strip())
            except (TypeError, ValueError):
                continue
        return results

    def run(self):
        """
        Main entry point to start recording data. This starts all the other threads.
        """
        for cur in self.currencies_to_analyse:
            db_con = self.create_connection(cur)
            self.create_rate_table(db_con, self.recorded_levels)
            db_con.close()
        self.run_threads()
        self.run_del_threads()
        if self.collect_funding_trades:
            self.start_funding_trades_recorder()

    def run_threads(self):
        """
        Start threads for each currency we want to record. (should be configurable later)
        """
        for _ in ['thread1']:
            for cur in self.currencies_to_analyse:
                thread = threading.Thread(target=self.update_market_thread, args=(cur,))
                thread.deamon = True
                thread.start()

    def run_del_threads(self):
        """
        Start thread to start the DB cleaning threads.
        """
        for _ in ['thread1']:
            for cur in self.currencies_to_analyse:
                del_thread = threading.Thread(target=self.delete_old_data_thread, args=(cur, self.keep_history_seconds))
                del_thread.daemon = False
                del_thread.start()

    def start_funding_trades_recorder(self):
        db_name = '{0}-{1}-funding.db'.format(self.exchange, self.funding_currency)
        db_path = os.path.join(self.db_dir, db_name)
        self.funding_recorder = FundingTradesRecorder(
            self.api,
            db_path,
            self.funding_currency,
            self.funding_poll_seconds,
            self.funding_window_seconds,
            self.funding_clean_interval,
            debug=self.ma_debug_log,
            rest_limit=self.funding_rest_limit,
            symbol=self.funding_symbol,
            rest_polling_enabled=self.funding_rest_polling,
            rest_overlap_seconds=self.funding_rest_overlap,
            archive_enabled=self.funding_archive_enabled,
            archive_db_path=self.funding_archive_path,
            bucket_percentile=self.funding_bucket_percentile,
            bucket_multipliers=self.funding_bucket_multipliers,
            demand_depth=self.funding_demand_depth
        )
        self.funding_recorder.start()

    def use_funding_bucket_strategy(self, currency):
        if not self.funding_bucket_strategy:
            return False
        if not self.funding_recorder:
            return False
        return currency.upper() == self.funding_currency.upper()

    def build_funding_orders(self, currency, total_amount, min_loan_size, min_rate):
        if not self.use_funding_bucket_strategy(currency):
            return []
        try:
            result = self.funding_recorder.get_rate_recommendations(percentile=self.funding_bucket_percentile)
        except Exception as ex:
            if self.ma_debug_log:
                print("Failed to build funding recommendations: {0}".format(ex))
            return []
        return self.funding_bucket_planner.plan_orders(
            Decimal(total_amount),
            Decimal(min_loan_size),
            Decimal(min_rate),
            result["recommendations"]
        )

    def delete_old_data_thread(self, cur, seconds):
        """
        Thread to clean the DB.
        """
        while True:
            try:
                db_con = self.create_connection(cur)
                self.delete_old_data(db_con, seconds)
            except Exception as ex:
                print(("Error in MarketAnalysis: {0}".format(str(ex))))
                traceback.print_exc()
            time.sleep(self.delete_thread_sleep)

    @staticmethod
    def print_traceback(ex, log_message):
        message = str(ex)
        print(("{0}: {1}".format(log_message, message)))
        traceback.print_exc()

    @staticmethod
    def print_exception_error(ex, log_message, debug=False):
        message = str(ex)
        print(("{0}: {1}".format(log_message, message)))
        if debug:
            import traceback
            ex_type, value, tb = sys.exc_info()
            print(("DEBUG: Class:{0} Args:{1}".format(ex.__class__, ex.args)))
            print(("DEBUG: Type:{0} Value:{1} LineNo:{2}".format(ex_type, value, tb.tb_lineno)))
            traceback.print_exc()

    def update_market_thread(self, cur, levels=None):
        """
        This is where the main work is done for recording the market data. The loop will not exit and continuously
        polls exchange for the current loans in the book.

        :param cur: The currency (database) to remove data from
        :param levels: The depth of offered rates to store
        """
        if levels is None:
            levels = self.recorded_levels
        db_con = self.create_connection(cur)
        while True:
            try:
                raw_data = self.api.return_loan_orders(cur, levels)['offers']
            except ApiError as ex:
                if '429' in str(ex):
                    if self.ma_debug_log:
                        print(("Caught ERR_RATE_LIMIT, sleeping capture and increasing request delay. Current"
                              " {0}ms".format(self.api.req_period)))
                    time.sleep(130)
            except Exception as ex:
                if self.ma_debug_log:
                    self.print_traceback(ex, "Error in returning data from exchange")
                else:
                    print("Error in returning data from exchange, ignoring")

            market_data = []
            for i in range(levels):
                try:
                    market_data.append(str(raw_data[i]['rate']))
                    market_data.append(str(raw_data[i]['amount']))
                except IndexError:
                    market_data.append("5")
                    market_data.append("0.1")
            market_data.append('0')  # Percentile field not being filled yet.
            self.insert_into_db(db_con, market_data)
            time.sleep(5)

    def insert_into_db(self, db_con, market_data, levels=None):
            if levels is None:
                levels = self.recorded_levels
            insert_sql = "INSERT INTO loans ("
            for level in range(levels):
                insert_sql += "rate{0}, amnt{0}, ".format(level)
            insert_sql += "percentile) VALUES ({0});".format(','.join(market_data))  # percentile = 0
            with db_con:
                try:
                    db_con.execute(insert_sql)
                except Exception as ex:
                    self.print_traceback(ex, "Error inserting market data into DB")

    def delete_old_data(self, db_con, seconds):
        """
        Delete old data from the database

        :param db_con: Connection to the database
        :param cur: The currency (database) to remove data from
        :param seconds: The time in seconds of the oldest data to be kept
        """
        del_time = int(time.time()) - seconds
        with db_con:
            query = "DELETE FROM loans WHERE unixtime < {0};".format(del_time)
            cursor = db_con.cursor()
            cursor.execute(query)

    @staticmethod
    def get_day_difference(date_time):  # Will be a number of seconds since epoch
        """
        Get the difference in days between the supplied date_time and now.

        :param date_time: A python date time object
        :return: The number of days that have elapsed since date_time
        """
        date1 = datetime.datetime.fromtimestamp(float(date_time))
        now = datetime.datetime.now()
        diff_days = (now - date1).days
        return diff_days

    def get_rate_list(self, cur, seconds):
        """
        Query the database (cur) for rates that are within the supplied number of seconds and now.

        :param cur: The currency (database) to remove data from
        :param seconds: The number of seconds between the oldest order returned and now.

        :return: A pandas DataFrame object with named columns ('time', 'rate0', 'rate1',...)
        """
        # Request more data from the DB than we need to allow for skipped seconds
        request_seconds = int(seconds * 1.1)
        full_list = Config.get_all_currencies()
        if isinstance(cur, sqlite.Connection):
            db_con = cur
        else:
            if cur not in full_list:
                raise ValueError("{0} is not a valid currency, must be one of {1}".format(cur, full_list))
            if cur not in self.currencies_to_analyse:
                return []
            db_con = self.create_connection(cur)

        price_levels = ['rate0']
        rates = self.get_rates_from_db(db_con, from_date=time.time() - request_seconds, price_levels=price_levels)
        if len(rates) == 0:
            return []

        df = pd.DataFrame(rates)

        columns = ['time']
        columns.extend(price_levels)
        try:
            df.columns = columns
        except:
            if self.ma_debug_log:
                print(("DEBUG:get_rate_list: cols: {0} rates:{1} db:{2}".format(columns, rates, db_con)))
            raise

        # convert unixtimes to datetimes so we can resample
        df.time = pd.to_datetime(df.time, unit='s')
        # If we don't have enough data return df, otherwise the resample will fill out all values with the same data.
        # Missing data tolerance allows for a percentage to be ignored and filled in by resampling.
        if len(df) < seconds * (self.data_tolerance / 100):
            return df
        # Resample into 1 second intervals, average if we get two in the same second and fill any empty spaces with the
        # previous value
        df = df.resample('1s', on='time').mean().ffill()
        return df

    def get_analysis_seconds(self, method):
        """
        Gets the correct number of seconds to use for anylsing data depeding on the method being used.
        """
        if method == 'percentile':
            return self.percentile_seconds
        elif method == 'MACD':
            return self.MACD_long_win_seconds

    def get_rate_suggestion(self, cur, rates=None, method='percentile'):
        """
        Return the suggested rate from analysed data. This is the main method for retrieving data from this module.
        Currently this only supports returning of a single value, the suggested rate. However this will be expanded to
        suggest a lower and higher rate for spreads.

        :param cur: The currency (database) to remove data from
        :param rates: This is used for unit testing only. It allows you to populate the data used for the suggestion.
        :param method: The method by which you want to calculate the suggestion.

        :return: A float with the suggested rate for the currency.
        """
        error_msg = "WARN: Exception found when analysing markets, if this happens for more than a couple minutes " +\
                    "please create a Github issue so we can fix it. Otherwise, you can ignore it. Error"

        try:
            rates = self.get_rate_list(cur, self.get_analysis_seconds(method)) if rates is None else rates
            if not isinstance(rates, pd.DataFrame):
                raise ValueError("Rates must be a Pandas DataFrame")
            if len(rates) == 0:
                print("Rate list not populated")
                if self.ma_debug_log:
                    print(("DEBUG:get_analysis_seconds: cur: {0} method:{1} rates:{2}".format(cur, method, rates)))
                return 0
            if method == 'percentile':
                return self.get_percentile(rates.rate0.values.tolist(), self.lending_style)
            if method == 'MACD':
                macd_rate = truncate(self.get_MACD_rate(cur, rates), 6)
                if self.ma_debug_log:
                    print(("Cur:{0}, MACD:{1:.6f}, Perc:{2:.6f}, Best:{3:.6f}"
                          .format(cur, macd_rate, self.get_percentile(rates.rate0.values.tolist(), self.lending_style),
                                  rates.rate0.iloc[-1])))
                return macd_rate
        except MarketDataException:
            if method != 'percentile':
                print(("Caught exception during {0} analysis, using percentile for now".format(method)))
                return self.get_percentile(rates.rate0.values.tolist(), self.lending_style)
            else:
                raise
        except Exception as ex:
            self.print_exception_error(ex, error_msg, debug=self.ma_debug_log)
            return 0

    @staticmethod
    def percentile(N, percent, key=lambda x: x):
        """
        http://stackoverflow.com/questions/2374640/how-do-i-calculate-percentiles-with-python-numpy/2753343#2753343
        Find the percentile of a list of values.

        :parameter N: A list of values. Note N MUST BE already sorted.
        :parameter percent: A float value from 0.0 to 1.0.
        :parameter key: Optional key function to compute value from each element of N.

        :return: Percentile of the values
        """
        import math
        if not N:
            return None
        k = (len(N) - 1) * percent
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return key(N[int(k)])
        d0 = key(N[int(f)]) * (c - k)
        d1 = key(N[int(c)]) * (k - f)
        return d0 + d1

    def get_percentile(self, rates, lending_style, use_numpy=use_numpy):
        """
        Take a list of rates no matter what method is being used, simple list, no pandas / numpy array
        """
        if use_numpy:
            result = numpy.percentile(rates, int(lending_style))
        else:
            result = self.percentile(sorted(rates), lending_style / 100.0)
        result = truncate(result, 6)
        return result

    def get_MACD_rate(self, cur, rates_df):
        """
        Golden cross is a bit of a misnomer. But we're trying to look at the short term moving average and the long
        term moving average. If the short term is above the long term then the market is moving in a bullish manner and
        it's a good time to lend. So return the short term moving average (scaled with the multiplier).

        :param cur: The currency (database) to remove data from
        :param rates_df: A pandas DataFrame with times and rates
        :param short_period: Length in seconds of the short window for MACD calculations
        :param long_period: Length in seconds of the long window for MACD calculations
        :param multiplier: The multiplier to apply to the rate before returning.

        :retrun: A float of the suggested, calculated rate
        """
        if len(rates_df) < self.get_analysis_seconds('MACD') * (self.data_tolerance / 100):
            print(("{0} : Need more data for analysis, still collecting. I have {1}/{2} records"
                  .format(cur, len(rates_df), int(self.get_analysis_seconds('MACD') * (self.data_tolerance / 100)))))
            raise MarketDataException

        short_rate = rates_df.rate0.tail(self.MACD_short_win_seconds).mean()
        long_rate = rates_df.rate0.tail(self.MACD_long_win_seconds).mean()

        if self.ma_debug_log:
            sys.stdout.write("Short higher: ") if short_rate > long_rate else sys.stdout.write("Long  higher: ")

        if short_rate > long_rate:
            if rates_df.rate0.iloc[-1] < short_rate:
                return short_rate * self.daily_min_multiplier
            else:
                return rates_df.rate0.iloc[-1] * self.daily_min_multiplier
        else:
            return long_rate * self.daily_min_multiplier

    def create_connection(self, cur, db_path=None, db_type='sqlite3'):
        """
        Create a connection to the sqlite DB. This will create a new file if one doesn't exist.  We can use :memory:
        here for db_path if we don't want to store the data on disk

        :param cur: The currency (database) in the DB
        :param db_path: DB directory
        :return: Connection object or None
        """
        if db_path is None:
            prefix = Config.get_exchange()
            db_path = os.path.join(self.db_dir, '{0}-{1}.db'.format(prefix, cur))
        try:
            con = sqlite.connect(db_path)
            return con
        except Error as ex:
            print(str(ex))

    def create_rate_table(self, db_con, levels):
        """
        Create a new table to hold rate data.

        :param db_con: Connection to the database
        :param cur: The currency being stored in the DB. There's a table for each currency.
        :param levels: The depth of offered rates to store
        """
        with db_con:
            cursor = db_con.cursor()
            create_table_sql = "CREATE TABLE IF NOT EXISTS loans (id INTEGER PRIMARY KEY AUTOINCREMENT," + \
                               "unixtime integer(4) not null default (strftime('%s','now')),"
            for level in range(levels):
                create_table_sql += "rate{0} FLOAT, ".format(level)
                create_table_sql += "amnt{0} FLOAT, ".format(level)
            create_table_sql += "percentile FLOAT);"
            cursor.execute("PRAGMA journal_mode=wal")
            cursor.execute(create_table_sql)

    def get_rates_from_db(self, db_con, from_date=None, price_levels=['rate0']):
        """
        Query the DB for all rates for a particular currency

        :param db_con: Connection to the database
        :param cur: The currency you want to get the rates for
        :param from_date: The earliest data you want, specified in unix time (seconds since epoch)
        :price_level: We record multiple price levels in the DB, the best offer being rate0
        """
        with db_con:
            cursor = db_con.cursor()
            query = "SELECT unixtime, {0} FROM loans ".format(",".join(price_levels))
            if from_date is not None:
                query += "WHERE unixtime > {0}".format(from_date)
            query += ";"
            cursor.execute(query)
            return cursor.fetchall()
