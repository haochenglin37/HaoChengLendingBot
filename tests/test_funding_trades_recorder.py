import math
from decimal import Decimal
import os
import sqlite3
import tempfile
import shutil
import time

import pytest

# Allow importing project modules when running tests directly
import sys
import inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

from modules.MarketAnalysis import FundingTradesRecorder, FundingBucketPlanner


@pytest.fixture
def temp_db_path():
    tmp_dir = tempfile.mkdtemp()
    path = os.path.join(tmp_dir, 'funding.db')
    try:
        yield path
    finally:
        shutil.rmtree(tmp_dir)


def fetch_all(path):
    conn = sqlite3.connect(path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT loan_id, rate, amount, duration FROM funding_trades ORDER BY loan_id")
        return cursor.fetchall()
    finally:
        conn.close()


class DummyLoanBookApi:
    def __init__(self, demands):
        self.demands = demands

    def return_loan_orders(self, currency, limit=0):
        return {"offers": [], "demands": self.demands}


def test_process_snapshot_inserts_unique_records(temp_db_path):
    recorder = FundingTradesRecorder(
        api=None,
        db_path=temp_db_path,
        currency='USD',
        poll_seconds=60,
        window_seconds=3600,
        clean_interval=3600,
        debug=True
    )

    base_ts = int(time.time())
    recorder.add_manual_trade('1', 0.01, 100.0, 30, base_ts)
    recorder.add_manual_trade('2', 0.02, 200.0, 60, base_ts + 10)
    # second call should not duplicate entries
    recorder.add_manual_trade('1', 0.01, 100.0, 30, base_ts)

    records = fetch_all(temp_db_path)
    assert len(records) == 2
    assert records[0][0] == '1'
    assert records[1][0] == '2'


def test_cleanup_once_removes_old_entries(temp_db_path):
    recorder = FundingTradesRecorder(
        api=None,
        db_path=temp_db_path,
        currency='USD',
        poll_seconds=60,
        window_seconds=60,
        clean_interval=60,
        debug=True
    )
    now = int(time.time())
    old_ts = now - 600
    recorder.add_manual_trade('10', 0.01, 100.0, 30, old_ts)
    recorder.add_manual_trade('20', 0.02, 200.0, 30, now)
    recorder.cleanup_once()

    records = fetch_all(temp_db_path)
    assert len(records) == 1
    assert records[0][0] == '20'


def test_bucket_stats_include_percentiles(temp_db_path):
    recorder = FundingTradesRecorder(
        api=None,
        db_path=temp_db_path,
        currency='USD',
        poll_seconds=60,
        window_seconds=3600,
        clean_interval=3600,
        debug=False
    )
    base_ts = int(time.time())

    two_day_rates = [0.010, 0.012, 0.014, 0.016]
    for idx, rate in enumerate(two_day_rates):
        recorder.add_manual_trade(f"2d-{idx}", rate, 100.0, 2, base_ts + idx)

    longer_rates = [0.020, 0.021, 0.022]
    for idx, rate in enumerate(longer_rates):
        recorder.add_manual_trade(f"8d-{idx}", rate, 100.0, 10, base_ts + 10 + idx)

    stats = recorder.get_bucket_stats(percentiles=[95])

    assert stats["2d"]["count"] == len(two_day_rates)
    expected_p95 = manual_percentile(two_day_rates, 95)
    assert pytest.approx(stats["2d"]["percentiles"][95.0], rel=1e-9) == pytest.approx(expected_p95)
    assert stats["8-30d"]["count"] == len(longer_rates)
    assert stats["31-120d"]["count"] == 0


def test_rate_recommendations_use_percentile_and_top_bid(temp_db_path):
    demands = [
        {'rate': '0.014', 'amount': '50', 'rangeMin': '2', 'rangeMax': '2'},
        {'rate': '0.018', 'amount': '40', 'rangeMin': '2', 'rangeMax': '7'},
        {'rate': '0.022', 'amount': '30', 'rangeMin': '2', 'rangeMax': '30'},
        {'rate': '0.026', 'amount': '20', 'rangeMin': '2', 'rangeMax': '120'}
    ]
    api = DummyLoanBookApi(demands)
    multipliers = {"2d": 0.05, "3-7d": 0.10, "8-30d": 0.15, "31-120d": 0.20}
    recorder = FundingTradesRecorder(
        api=api,
        db_path=temp_db_path,
        currency='USD',
        poll_seconds=60,
        window_seconds=3600,
        clean_interval=3600,
        debug=False,
        bucket_percentile=95,
        bucket_multipliers=multipliers,
        demand_depth=10
    )
    base_ts = int(time.time())

    two_day_rates = [0.010, 0.012, 0.014, 0.016]
    for idx, rate in enumerate(two_day_rates):
        recorder.add_manual_trade(f"t{idx}", rate, 50.0, 2, base_ts + idx)

    for idx, rate in enumerate([0.017, 0.018, 0.019]):
        recorder.add_manual_trade(f"s{idx}", rate, 50.0, 5, base_ts + 10 + idx)

    for idx, rate in enumerate([0.020, 0.0215, 0.023]):
        recorder.add_manual_trade(f"m{idx}", rate, 50.0, 15, base_ts + 20 + idx)

    for idx, rate in enumerate([0.024, 0.025, 0.027]):
        recorder.add_manual_trade(f"l{idx}", rate, 50.0, 60, base_ts + 30 + idx)

    stats = recorder.get_bucket_stats(percentiles=[95])
    recommendations = recorder.get_rate_recommendations(percentile=95, stats=stats)
    recs = recommendations["recommendations"]

    expected_two_day = manual_percentile(two_day_rates, 95)
    assert pytest.approx(recs["2d"]["percentile"], rel=1e-9) == expected_two_day
    assert pytest.approx(recs["2d"]["top_bid"], rel=1e-9) == 0.014
    assert pytest.approx(recs["2d"]["recommended"], rel=1e-9) == expected_two_day

    long_bucket = recs["31-120d"]
    assert pytest.approx(long_bucket["top_bid"], rel=1e-9) == 0.026
    assert pytest.approx(long_bucket["recommended"], rel=1e-9) == pytest.approx(0.026 * 1.20)
    assert pytest.approx(long_bucket["multiplier"], rel=1e-9) == 0.20


def test_bucket_planner_respects_allocations_and_min_size():
    planner = FundingBucketPlanner(
        {"2d": 3, "3-7d": 3, "8-30d": 2, "31-120d": 2},
        {"2d": 2, "3-7d": 7, "8-30d": 30, "31-120d": 120}
    )
    recommendations = {
        "2d": {"recommended": 0.015},
        "3-7d": {"recommended": 0.018},
        "8-30d": {"recommended": 0.020},
        "31-120d": {"recommended": 0.025}
    }
    total_amount = Decimal('100')
    min_size = Decimal('10')
    min_rate = Decimal('0.010')
    orders = planner.plan_orders(total_amount, min_size, min_rate, recommendations)
    assert len(orders) == 10  # 3+3+2+2 slices
    bucket_counts = {}
    amount_sum = Decimal('0')
    for order in orders:
        bucket = order['bucket']
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        amount_sum += order['amount']
        assert order['duration'] in (2, 7, 30, 120)
    assert bucket_counts["2d"] == 3
    assert bucket_counts["3-7d"] == 3
    assert bucket_counts["8-30d"] == 2
    assert bucket_counts["31-120d"] == 2
    assert amount_sum == total_amount


def test_bucket_planner_clamps_rates_to_min_rate():
    planner = FundingBucketPlanner({"2d": 1, "3-7d": 0, "8-30d": 0, "31-120d": 0}, {"2d": 2})
    recommendations = {"2d": {"recommended": 0.0005}}
    orders = planner.plan_orders(Decimal('20'), Decimal('10'), Decimal('0.001'), recommendations)
    assert len(orders) == 2
    for order in orders:
        assert order['rate'] == Decimal('0.001')


def test_best_bucket_only_uses_highest_rate_shortest_duration():
    planner = FundingBucketPlanner(best_bucket_only=True)
    recommendations = {
        "2d": {"recommended": 0.01, "source": "percentile"},
        "3-7d": {"recommended": 0.02, "source": "top_bid"},
        "8-30d": {"recommended": 0.02, "source": "top_bid"},
        "31-120d": {"recommended": 0.015}
    }
    total = Decimal('100')
    orders = planner.plan_orders(total, Decimal('10'), Decimal('0.001'), recommendations)
    assert len(orders) == 10
    for order in orders:
        assert order['bucket'] == "3-7d"
        assert order['rate'] == Decimal(str(recommendations["3-7d"]["recommended"]))


def manual_percentile(values, percentile):
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ordered[int(k)]
    d0 = ordered[int(f)] * (c - k)
    d1 = ordered[int(c)] * (k - f)
    return d0 + d1
