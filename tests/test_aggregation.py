# tests/test_bucketed_sliding_aggregator.py
from datetime import datetime, timedelta

import pytest

from zmqNotifier.tick_agg import BucketedSlidingAggregator


def test_query_empty_window_raises():
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    with pytest.raises(LookupError):
        agg.query_min_max(num_buckets=5)


def test_bucket_span_must_be_positive():
    with pytest.raises(ValueError):
        BucketedSlidingAggregator(bucket_span=timedelta(seconds=0))


def test_num_buckets_must_be_non_negative():
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    now = datetime(2024, 1, 1, 12, 0)
    agg.add(now, 10.0)
    with pytest.raises(ValueError):
        agg.query_min_max(num_buckets=-1)


def test_direction_positive_for_active_bucket():
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)
    agg.add(base, 1.0)  # min arrives first
    agg.add(base + timedelta(seconds=10), 4.0)  # max arrives later

    _, _, direction, _ = agg.query_min_max()
    assert direction == pytest.approx(3.0)


def test_direction_negative_for_active_bucket():
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)
    agg.add(base, 5.0)  # max arrives first
    agg.add(base + timedelta(seconds=10), 2.0)  # min arrives later

    _, _, direction, _ = agg.query_min_max()
    assert direction == pytest.approx(-3.0)


def test_direction_none_for_historical_query():
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)
    agg.add(base, 1.0)
    agg.add(base + timedelta(seconds=30), 2.0)
    agg.add(base + timedelta(minutes=1, seconds=5), 3.0)  # moves to new bucket

    _, _, direction_active, _ = agg.query_min_max()
    assert direction_active is not None

    _, _, direction_hist, _ = agg.query_min_max(num_buckets=1)
    assert direction_hist is None


def test_active_deque_only():
    """Query with num_buckets=0 should only return active deque min/max."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)
    agg.add(base, 10.0)

    # Query active deque only (before boundary crossing)
    min_val, max_val, direction, max_count = agg.query_min_max()
    assert min_val == 10.0
    assert max_val == 10.0

    agg.add(base + timedelta(seconds=15), 5.0)

    min_val, max_val, direction, max_count = agg.query_min_max()
    assert min_val == 5.0
    assert max_val == 10.0

    agg.add(base + timedelta(seconds=30), 12.0)

    # Query active deque only (before boundary crossing)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 5.0
    assert max_val == 12.0


def test_boundary_crossing_condenses_bucket():
    """When time crosses bucket boundary, active deque should condense."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # Add points in first bucket [12:00, 12:01)
    agg.add(base, 10.0)
    agg.add(base + timedelta(seconds=30), 5.0)

    # Cross boundary - add point in next bucket [12:01, 12:02)
    agg.add(base + timedelta(minutes=1, seconds=10), 20.0)

    # Query active deque only - should only see the new bucket's data
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 20.0
    assert max_val == 20.0

    # Query with 1 past bucket - should include condensed first bucket
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 5.0  # From condensed bucket
    assert max_val == 20.0  # From active deque


def test_multiple_buckets_and_clamping():
    """Test querying multiple buckets and clamping to available."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # Add data across 3 buckets
    agg.add(base, 7.0)  # bucket [12:00, 12:01)
    agg.add(base + timedelta(minutes=1), 8.0)  # [12:01, 12:02)
    agg.add(base + timedelta(minutes=2), 15.0)  # [12:02, 12:03)

    # Query 0 buckets (active only)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 15.0
    assert max_val == 15.0

    # Query 1 bucket (active + last 1 condensed)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 8.0
    assert max_val == 15.0

    # Query 2 buckets (active + last 2 condensed)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=2)
    assert min_val == 7.0
    assert max_val == 15.0

    # Query 100 buckets (should clamp to available: 2 condensed + 1 active)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=100)
    assert min_val == 7.0
    assert max_val == 15.0


def test_max_window_eviction():
    """Test that max_window limits the number of stored buckets."""
    agg = BucketedSlidingAggregator(
        bucket_span=timedelta(minutes=1),
        max_window=2,  # Keep only 2 condensed buckets
    )
    base = datetime(2024, 1, 1, 12, 0)

    agg.add(base, 10.0)  # [12:00, 12:01)
    agg.add(base + timedelta(minutes=1), 20.0)  # [12:01, 12:02)
    agg.add(base + timedelta(minutes=2), 5.0)  # [12:02, 12:03) - evicts 12:00
    agg.add(base + timedelta(minutes=3), 7.0)  # [12:03, 12:04) - evicts 12:01

    # Query all available buckets (should only have 2 condensed buckets + active)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=100)
    # Should NOT include 10.0 (evicted from 12:00 bucket)
    # Should include: 20.0 from 12:01 bucket, 5.0 from 12:02 bucket, 7.0 from active
    assert min_val == 5.0
    assert max_val == 20.0

    # Verify the 12:00 bucket (10.0) was evicted
    # Query only 1 bucket (should not include the 20.0 from 12:01)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 5.0  # From 12:02 bucket
    assert max_val == 7.0  # From active deque


def test_no_max_window_keeps_all_buckets():
    """Test that without max_window, all buckets are retained."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    agg.add(base, 1.0)
    agg.add(base + timedelta(hours=1), 100.0)

    # Query all buckets (should include all 60 condensed buckets + active)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=100)
    assert min_val == 1.0
    assert max_val == 100.0


def test_non_decreasing_timestamps_required():
    """Test that adding timestamps out of order raises ValueError."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)
    agg.add(base, 1.0)
    with pytest.raises(ValueError):
        agg.add(base - timedelta(seconds=1), 2.0)


def test_active_deque_exact_tracking():
    """
    Test that the active deque provides exact min/max tracking within current bucket.
    This replaces the old partial bucket approximation with exact values.
    """
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # Add multiple points to active bucket
    agg.add(base, 5.0)
    agg.add(base + timedelta(seconds=10), 2.0)  # New min
    agg.add(base + timedelta(seconds=20), 9.0)  # New max
    agg.add(base + timedelta(seconds=30), 7.0)

    # Query active deque - should get exact min/max
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 2.0
    assert max_val == 9.0


def test_bucket_clock_alignment():
    """Test that buckets align to clock boundaries, not first timestamp."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))

    # Start at 12:00:37 (not on boundary)
    base = datetime(2024, 1, 1, 12, 0, 37)
    agg.add(base, 5.0)

    # Cross into next minute at 12:01:15
    agg.add(datetime(2024, 1, 1, 12, 1, 15), 10.0)

    # The first bucket should be [12:00:00, 12:01:00), not [12:00:37, 12:01:37)
    # Query with 1 bucket should include the 5.0 from 12:00:37
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 5.0
    assert max_val == 10.0


def test_empty_buckets_skipped():
    """Test that empty buckets (from time gaps) don't affect min/max."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # Add data with a 2-minute gap
    agg.add(base, 5.0)
    agg.add(base + timedelta(minutes=3), 10.0)  # Creates empty buckets at 12:01, 12:02

    assert len(agg._buckets) == 1

    min_val, max_val, direction, max_count = agg.query_min_max()
    assert min_val == 10.0
    assert max_val == 10.0

    min_val, max_val, direction, max_count = agg.query_min_max(1)
    assert min_val == 10.0
    assert max_val == 10.0

    # Query all buckets
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert min_val == 5.0
    assert max_val == 10.0


# ============================================================================
# Tests with 2-minute bucket spans
# ============================================================================


def test_two_minute_buckets_basic():
    """Test basic functionality with 2-minute buckets."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=2))
    base = datetime(2024, 1, 1, 12, 0)

    # Add points within first bucket [12:00, 12:02)
    agg.add(base, 10.0)
    agg.add(base + timedelta(seconds=30), 5.0)
    agg.add(base + timedelta(minutes=1), 15.0)

    assert len(agg._buckets) == 0

    # Query active only
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 5.0
    assert max_val == 15.0

    # Cross boundary into [12:02, 12:04)
    agg.add(base + timedelta(minutes=2, seconds=10), 20.0)

    # Query active only (should only see new bucket)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 20.0
    assert max_val == 20.0

    # Query with 1 bucket lookback (includes previous 2-minute span)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 5.0  # From [12:00, 12:02) bucket
    assert max_val == 20.0  # From active [12:02, 12:04)


def test_two_minute_buckets_clock_alignment():
    """Test that 2-minute buckets align to clock boundaries."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=2))

    # Start at 12:00:45 (should align to 12:00:00)
    base = datetime(2024, 1, 1, 12, 0, 45)
    agg.add(base, 7.0)

    # Add at 12:01:30 (still in [12:00, 12:02) bucket)
    agg.add(datetime(2024, 1, 1, 12, 1, 30), 3.0)

    # Cross into [12:02, 12:04) at 12:02:15
    agg.add(datetime(2024, 1, 1, 12, 2, 15), 12.0)

    # Query with 1 bucket should include data from [12:00, 12:02)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 3.0
    assert max_val == 12.0


def test_two_minute_buckets_time_based_lookback():
    """Test time-based lookback with 2-minute buckets and gaps."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=2))
    base = datetime(2024, 1, 1, 12, 0)

    # Add data across multiple 2-minute buckets with gaps
    agg.add(base, 5.0)  # [12:00, 12:02)
    agg.add(base + timedelta(minutes=2), 8.0)  # [12:02, 12:04)
    # Skip [12:04, 12:06)
    agg.add(base + timedelta(minutes=6), 15.0)  # [12:06, 12:08)
    agg.add(base + timedelta(minutes=8), 20.0)  # [12:08, 12:10)

    # Active is at [12:08, 12:10), query 1 bucket = look back 2 minutes (from 12:06)
    # Should include [12:06, 12:08) and active [12:08, 12:10)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 15.0
    assert max_val == 20.0

    # Query 2 buckets = look back 4 minutes (from 12:04)
    # Should include [12:06, 12:08) and active, but NOT [12:02, 12:04)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=2)
    assert min_val == 15.0
    assert max_val == 20.0

    # Query 3 buckets = look back 6 minutes (from 12:02)
    # Should include [12:02, 12:04), [12:06, 12:08), and active
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=3)
    assert min_val == 8.0
    assert max_val == 20.0

    # Query 5 buckets = look back 10 minutes (from 11:58)
    # Should include everything
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=5)
    assert min_val == 5.0
    assert max_val == 20.0


def test_two_minute_buckets_max_window():
    """Test max_window eviction with 2-minute buckets."""
    agg = BucketedSlidingAggregator(
        bucket_span=timedelta(minutes=2),
        max_window=2,  # Keep only 2 condensed buckets
    )
    base = datetime(2024, 1, 1, 12, 0)

    agg.add(base, 10.0)  # [12:00, 12:02)
    agg.add(base + timedelta(minutes=2), 20.0)  # [12:02, 12:04)
    agg.add(base + timedelta(minutes=4), 5.0)  # [12:04, 12:06) - evicts [12:00, 12:02)
    agg.add(base + timedelta(minutes=6), 7.0)  # [12:06, 12:08) - evicts [12:02, 12:04)

    # After evictions, should only have 2 condensed buckets: [12:02, 12:04) and [12:04, 12:06)
    # Plus active bucket [12:06, 12:08)
    assert len(agg._buckets) == 2

    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert min_val == 5.0
    assert max_val == 20.0  # 20.0 from [12:02, 12:04) bucket (not evicted yet)

    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 5.0  # From [12:04, 12:06)
    assert max_val == 7.0  # From active [12:06, 12:08)


# ============================================================================
# Tests with 1-hour bucket spans
# ============================================================================


def test_one_hour_buckets_basic():
    """Test basic functionality with 1-hour buckets."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(hours=1))
    base = datetime(2024, 1, 1, 12, 0)

    # Add points within first hour [12:00, 13:00)
    agg.add(base, 10.0)
    agg.add(base + timedelta(minutes=15), 5.0)
    agg.add(base + timedelta(minutes=30), 20.0)
    agg.add(base + timedelta(minutes=45), 8.0)

    # Query active only
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 5.0
    assert max_val == 20.0

    # Cross boundary into [13:00, 14:00)
    agg.add(base + timedelta(hours=1, minutes=10), 15.0)

    # Query active only
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 15.0
    assert max_val == 15.0

    # Query with 1 hour lookback
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 5.0  # From [12:00, 13:00) bucket
    assert max_val == 20.0  # From [12:00, 13:00) bucket (not 15.0 from active!)


def test_one_hour_buckets_clock_alignment():
    """Test that 1-hour buckets align to clock boundaries."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(hours=1))

    # Start at 12:37:42 (should align to 12:00:00)
    base = datetime(2024, 1, 1, 12, 37, 42)
    assert agg._align_to_bucket_boundary(base) == datetime(2024, 1, 1, 12, 0, 0)
    agg.add(base, 100.0)

    # Add at 12:58:30 (still in [12:00, 13:00) bucket)
    agg.add(datetime(2024, 1, 1, 12, 58, 30), 50.0)

    # Cross into [13:00, 14:00) at 13:05:00
    agg.add(datetime(2024, 1, 1, 13, 5, 0), 75.0)

    # Query with 1 bucket should include data from [12:00, 13:00)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 50.0
    assert max_val == 100.0


def test_one_hour_buckets_time_based_lookback():
    """Test time-based lookback with 1-hour buckets across a day."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(hours=1))
    base = datetime(2024, 1, 1, 10, 0)

    # Add data across multiple hours
    agg.add(base, 10.0)  # [10:00, 11:00)
    agg.add(base + timedelta(hours=1), 20.0)  # [11:00, 12:00)
    agg.add(base + timedelta(hours=2), 15.0)  # [12:00, 13:00)
    # Skip 13:00-14:00
    agg.add(base + timedelta(hours=4), 25.0)  # [14:00, 15:00)
    agg.add(base + timedelta(hours=5), 30.0)  # [15:00, 16:00)

    # Active is at [15:00, 16:00), query 1 hour = from 14:00 onwards
    # Should include [14:00, 15:00) and active
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 25.0
    assert max_val == 30.0

    # Query 3 hours = from 12:00 onwards
    # Should include [12:00, 13:00), [14:00, 15:00), and active
    # Note: [13:00, 14:00) has no data (gap)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=3)
    assert min_val == 15.0
    assert max_val == 30.0

    # Query 5 hours = from 10:00 onwards
    # Should include all data
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=5)
    assert min_val == 10.0
    assert max_val == 30.0

    # Query 6 hours = from 09:00 onwards (before first data point)
    # Should still include all available data
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=6)
    assert min_val == 10.0
    assert max_val == 30.0


def test_one_hour_buckets_intraday_pattern():
    """Test realistic intraday trading pattern with 1-hour buckets."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(hours=1))
    base = datetime(2024, 1, 1, 9, 0)  # Market open

    # Simulate price movements throughout the day
    prices = [
        (0, 100.0),  # 09:00 - opening
        (0.5, 102.0),  # 09:30
        (1, 98.0),  # 10:00 - dip
        (2, 105.0),  # 11:00 - rally
        (3, 107.0),  # 12:00 - peak
        (4, 103.0),  # 13:00 - pullback
        (5, 101.0),  # 14:00
        (6, 104.0),  # 15:00 - close
    ]

    for hours_offset, price in prices:
        agg.add(base + timedelta(hours=hours_offset), price)

    # Current active bucket is [15:00, 16:00) with 104.0
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert min_val == 104.0
    assert max_val == 104.0

    # Look back 2 hours (from 13:00 onwards) - includes [13:00, 14:00), [14:00, 15:00), active
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=2)
    assert min_val == 101.0  # From [14:00, 15:00)
    assert max_val == 104.0  # From active

    # Look back 4 hours (from 11:00 onwards)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=4)
    assert min_val == 101.0  # From [14:00, 15:00)
    assert max_val == 107.0  # From [12:00, 13:00)

    # Look back entire day (8+ hours)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert min_val == 98.0  # From [10:00, 11:00)
    assert max_val == 107.0  # From [12:00, 13:00)


def test_one_hour_buckets_with_gaps():
    """Test 1-hour buckets with significant time gaps (e.g., overnight)."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(hours=1))

    # Day 1: Last trade at 16:00
    day1 = datetime(2024, 1, 1, 16, 0)
    agg.add(day1, 100.0)

    # Day 2: First trade at 09:00 (17 hour gap)
    day2 = datetime(2024, 1, 2, 9, 0)
    agg.add(day2, 105.0)

    # Only 1 condensed bucket exists (day1), active has day2 data
    assert len(agg._buckets) == 1

    # Query 1 hour lookback - should only include active (from 08:00 onwards)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 105.0
    assert max_val == 105.0

    # Query 20 hours lookback - should include both
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=20)
    assert min_val == 100.0
    assert max_val == 105.0


def test_one_hour_buckets_max_window():
    """Test max_window eviction with 1-hour buckets over long periods."""
    agg = BucketedSlidingAggregator(
        bucket_span=timedelta(hours=1),
        max_window=3,  # Keep only 3 condensed buckets
    )
    base = datetime(2024, 1, 1, 10, 0)

    # Add data over 6 hours
    for i in range(6):
        agg.add(base + timedelta(hours=i), float(10 + i * 10))

    # Should only have 3 most recent condensed buckets
    assert len(agg._buckets) == 3

    # Query all should only include last 3 buckets + active
    # Buckets kept: [12:00-13:00)=30, [13:00-14:00)=40, [14:00-15:00)=50, active [15:00-16:00)=60
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert min_val == 30.0  # Should NOT include 10.0 or 20.0
    assert max_val == 60.0


# ============================================================================
# Tests for max_count functionality
# ============================================================================


def test_max_count_active_window_only():
    """Test that max_count returns active window point count when querying active only."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # Add single point
    agg.add(base, 10.0)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert max_count == 1

    # Add more points
    agg.add(base + timedelta(seconds=15), 5.0)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert max_count == 2

    # Add third point
    agg.add(base + timedelta(seconds=30), 12.0)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert max_count == 3


def test_max_count_multiple_buckets_with_different_counts():
    """Test that max_count returns the maximum count across multiple buckets."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # First bucket: 2 points
    agg.add(base, 10.0)
    agg.add(base + timedelta(seconds=30), 5.0)

    # Second bucket: 3 points
    agg.add(base + timedelta(minutes=1), 20.0)
    agg.add(base + timedelta(minutes=1, seconds=20), 15.0)
    agg.add(base + timedelta(minutes=1, seconds=40), 25.0)

    # Third bucket (active): 1 point
    agg.add(base + timedelta(minutes=2), 30.0)

    # Query all buckets - max_count should be 3 (from second bucket)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert min_val == 5.0
    assert max_val == 30.0
    assert max_count == 3


def test_max_count_empty_buckets_ignored():
    """Test that empty buckets (from time gaps) contribute 0 to max_count."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # First bucket: 3 points
    agg.add(base, 5.0)
    agg.add(base + timedelta(seconds=20), 7.0)
    agg.add(base + timedelta(seconds=40), 6.0)

    # Jump 3 minutes ahead (creating empty buckets at 12:01, 12:02)
    # Active bucket: 1 point
    agg.add(base + timedelta(minutes=3), 10.0)

    # Query with 2 buckets lookback - should only see buckets with data
    # max_count should be 3 (from first bucket), not affected by gaps
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=2)
    assert max_count == 1  # Only active bucket in range

    # Query with 10 buckets - should include first bucket
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert max_count == 3  # From first bucket


def test_max_count_after_boundary_crossing():
    """Test that max_count tracks correctly after condensing buckets."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # First bucket: 4 points
    agg.add(base, 10.0)
    agg.add(base + timedelta(seconds=15), 5.0)
    agg.add(base + timedelta(seconds=30), 8.0)
    agg.add(base + timedelta(seconds=45), 12.0)

    # Cross boundary - second bucket (active): 2 points
    agg.add(base + timedelta(minutes=1, seconds=10), 20.0)
    agg.add(base + timedelta(minutes=1, seconds=30), 18.0)

    # Query with 1 bucket lookback - should include both buckets
    # max_count should be 4 (from condensed first bucket)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert min_val == 5.0
    assert max_val == 20.0
    assert max_count == 4

    # Query active only - should be 2
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert max_count == 2


def test_max_count_with_max_window_eviction():
    """Test that max_count only considers non-evicted buckets."""
    agg = BucketedSlidingAggregator(
        bucket_span=timedelta(minutes=1),
        max_window=2,  # Keep only 2 condensed buckets
    )
    base = datetime(2024, 1, 1, 12, 0)

    # First bucket: 5 points (will be evicted)
    for i in range(5):
        agg.add(base + timedelta(seconds=i * 10), 10.0 + i)

    # Second bucket: 3 points (will be kept)
    for i in range(3):
        agg.add(base + timedelta(minutes=1, seconds=i * 20), 20.0 + i)

    # Third bucket: 4 points (will be kept)
    for i in range(4):
        agg.add(base + timedelta(minutes=2, seconds=i * 15), 30.0 + i)

    # Fourth bucket (active): 2 points
    agg.add(base + timedelta(minutes=3), 40.0)
    agg.add(base + timedelta(minutes=3, seconds=30), 41.0)

    # Query all - first bucket should be evicted
    # max_count should be 4 (from third bucket), not 5
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert max_count == 4


def test_max_count_single_point_per_bucket():
    """Test max_count when each bucket has exactly one point."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # Add 1 point per bucket for 5 buckets
    for i in range(5):
        agg.add(base + timedelta(minutes=i), 10.0 + i)

    # All buckets have count=1, so max_count should be 1
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert max_count == 1


def test_max_count_active_window_has_maximum():
    """Test when active window has the maximum count."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # First bucket: 2 points
    agg.add(base, 10.0)
    agg.add(base + timedelta(seconds=30), 5.0)

    # Second bucket (active): 5 points
    for i in range(5):
        agg.add(base + timedelta(minutes=1, seconds=i * 10), 20.0 + i)

    # max_count should be 5 (from active window)
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert max_count == 5


def test_max_count_with_varying_bucket_sizes():
    """Test max_count with buckets of varying sizes (realistic scenario)."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
    base = datetime(2024, 1, 1, 12, 0)

    # Simulate varying trading activity
    bucket_counts = [10, 3, 15, 7, 20, 5, 12]  # Points per bucket
    for bucket_idx, count in enumerate(bucket_counts):
        for i in range(count):
            timestamp = base + timedelta(minutes=bucket_idx, seconds=i)
            agg.add(timestamp, 100.0 + bucket_idx)

    # Query different ranges and verify max_count
    # Last 2 buckets: [5, 12] -> max_count = 12
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert max_count == 12

    # Last 4 buckets: [20, 5, 12] + condensed [7] -> max_count = 20
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=3)
    assert max_count == 20

    # All buckets -> max_count = 20
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=10)
    assert max_count == 20


def test_max_count_two_minute_buckets():
    """Test max_count with 2-minute bucket spans."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=2))
    base = datetime(2024, 1, 1, 12, 0)

    # First 2-minute bucket: 6 points
    for i in range(6):
        agg.add(base + timedelta(seconds=i * 15), 10.0 + i)

    # Second 2-minute bucket (active): 3 points
    for i in range(3):
        agg.add(base + timedelta(minutes=2, seconds=i * 30), 20.0 + i)

    # Query with 1 bucket lookback
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert max_count == 6  # From first condensed bucket

    # Query active only
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=0)
    assert max_count == 3


def test_max_count_one_hour_buckets():
    """Test max_count with 1-hour bucket spans (realistic intraday)."""
    agg = BucketedSlidingAggregator(bucket_span=timedelta(hours=1))
    base = datetime(2024, 1, 1, 9, 0)

    # Simulate varying intraday activity levels
    # 09:00-10:00: Low activity (5 ticks)
    for i in range(5):
        agg.add(base + timedelta(minutes=i * 10), 100.0 + i)

    # 10:00-11:00: High activity (25 ticks)
    for i in range(25):
        agg.add(base + timedelta(hours=1, minutes=i * 2), 105.0 + i * 0.5)

    # 11:00-12:00 (active): Medium activity (10 ticks)
    for i in range(10):
        agg.add(base + timedelta(hours=2, minutes=i * 5), 110.0 + i)

    # Query last 2 hours - should capture high activity period
    min_val, max_val, direction, max_count = agg.query_min_max(num_buckets=1)
    assert max_count == 25  # From 10:00-11:00 bucket
