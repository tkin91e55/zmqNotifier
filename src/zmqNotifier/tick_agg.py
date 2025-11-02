"""
Tick aggregator with bucketed sliding window support.

This module provides a high-performance tick aggregator that:
- Groups ticks into fixed-size time buckets (clock-aligned)
- Maintains active bucket using monotonic deque
- Condenses buckets to aggregates on boundary crossing
- Supports O(log n) range queries via segment tree
- Tracks maximum tick count across queried buckets for activity analysis
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from decimal import Decimal

from zmqNotifier.segment_tree import SegmentTreeMinMax
from zmqNotifier.sliding_windows import SlidingWindowMinMax

DECIMAL_POS_INF = Decimal("Infinity")
DECIMAL_NEG_INF = Decimal("-Infinity")


# ============================================================================
# Data Models
# ============================================================================


@dataclass
class Bucket:
    """
    Aggregate for one fixed-size time bucket.

    Stores min/max/count for all ticks that fall within the bucket's time range.
    Condensed from the active deque when crossing bucket boundaries.
    """

    start: datetime
    end: datetime
    min_value: Decimal = DECIMAL_POS_INF
    max_value: Decimal = DECIMAL_NEG_INF
    count: int = 0

    @property
    def is_empty(self) -> bool:
        """Check if bucket has no data points."""
        return self.count == 0


# ============================================================================
# Aggregator
# ============================================================================


class BucketedSlidingAggregator:
    """
    High-performance tick aggregator with bucketed sliding windows.

    Architecture:
    - Active bucket: Uses SlidingWindowMinMax for O(1) min/max tracking
    - Historical buckets: Stored as condensed aggregates in a deque
    - Query optimization: Segment tree for O(log n) range queries

    Time Complexity:
    - add(): O(1) amortized
    - query_min_max(): O(log n) where n = number of buckets in range

    Usage:
        agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
        agg.add(timestamp, price)

        # Query returns (min, max, max_count)
        min_val, max_val, max_count = agg.query_min_max(num_buckets=60)

        # Get direction for active bucket only
        direction = agg.get_active_direction()  # Positive = rising, negative = falling

        # max_count shows the highest tick count from any bucket in the range
        # Useful for identifying most active periods or liquidity analysis
    """

    def __init__(self, bucket_span: timedelta, max_window: Optional[int] = None):
        """
        Initialize the aggregator.

        Args:
            bucket_span: Time span for each bucket (e.g., timedelta(minutes=1))
            max_window: Maximum number of buckets to retain (None = unlimited)
        """
        if bucket_span <= timedelta(0):
            raise ValueError("bucket_span must be positive")

        self._bucket_span = bucket_span
        self._max_window = max_window

        # Storage
        self._buckets: deque[Bucket] = deque()
        # days=1 make sense, just arbitray large number
        # to keep all tickes in the partial bucket, as active window is only for current bucket
        self._active_window = SlidingWindowMinMax(window=timedelta(days=1))
        self._current_bucket_start: Optional[datetime] = None

        # Query optimization (lazy-built segment tree)
        self._segment_tree: Optional[SegmentTreeMinMax] = None
        self._tree_dirty = True

    # ========================================================================
    # Public API
    # ========================================================================

    def add(self, timestamp: datetime, value: Decimal) -> None:
        """
        Add a tick to the aggregator.

        Automatically condenses the active bucket on boundary crossing.

        Args:
            timestamp: Tick timestamp
            value: Tick value

        Raises:
            ValueError: If timestamp is not non-decreasing
        """
        self._validate_timestamp(timestamp)
        if not isinstance(value, Decimal):
            value = Decimal(str(value))

        bucket_start = self._align_to_bucket_boundary(timestamp)

        # Handle bucket boundary crossing
        if self._is_new_bucket(bucket_start):
            self._condense_active_bucket()
            self._current_bucket_start = bucket_start

        # Initialize first bucket
        if self._current_bucket_start is None:
            self._current_bucket_start = bucket_start

        # Add to active window
        self._active_window.add(timestamp, value)

    def query_min_max(self, num_buckets: int = 0) -> tuple[Decimal, Decimal, int]:
        """
        Query min/max/max_count over active bucket + historical time range.

        Args:
            num_buckets: Number of bucket time spans to look back (0 = active only)
                        e.g., num_buckets=3 means look back 3*bucket_span of time
                        Empty buckets (gaps) are naturally ignored

        Returns:
            (min_value, max_value, max_count) tuple where max_count is the
            maximum count from any bucket (or active window) in the range.

        Raises:
            ValueError: If num_buckets is negative
            LookupError: If the window is empty

        Examples:
            # Active bucket only
            min_val, max_val, max_count = agg.query_min_max(0)

            # Last hour (60 one-minute buckets)
            min_val, max_val, max_count = agg.query_min_max(60)

        Note:
            For direction information on the active bucket, use get_active_direction().
        """
        if num_buckets < 0:
            raise ValueError("num_buckets must be non-negative")

        # Start with active window min/max/count
        min_value, _, max_value, _, max_count = self._get_active_window_stats()

        # Add historical buckets if requested
        if num_buckets > 0:
            hist_min, hist_max, hist_max_count = self._query_historical_buckets(num_buckets)
            min_value = min(min_value, hist_min)
            max_value = max(max_value, hist_max)
            max_count = max(max_count, hist_max_count)

        # Check if we found any data
        if min_value == DECIMAL_POS_INF:
            raise LookupError("window is empty")

        return min_value, max_value, max_count

    @property
    def buckets_count(self) -> int:
        return len(self._buckets)

    def get_active_direction(self) -> Decimal:
        """
        Get direction for the current active bucket only.

        The direction is a signed delta between the newer and older extremum:
        - Positive value: most recent extreme is higher (rising trend)
        - Negative value: most recent extreme is lower (falling trend)
        - Zero: no clear directional movement

        Returns:
            Signed delta between newer and older extremum

        Raises:
            LookupError: If the active window is empty

        Examples:
            # Get direction for active bucket
            direction = agg.get_active_direction()
            if direction > 0:
                print("Price trending upward")
            elif direction < 0:
                print("Price trending downward")
        """
        min_value, min_ts, max_value, max_ts, _ = self._get_active_window_stats()

        if min_value == DECIMAL_POS_INF or max_value == DECIMAL_NEG_INF:
            raise LookupError("active window is empty")

        return self._compute_direction(min_value, min_ts, max_value, max_ts)

    # ========================================================================
    # Active Window Management
    # ========================================================================

    def _get_active_window_stats(
        self,
    ) -> tuple[Decimal, Optional[datetime], Decimal, Optional[datetime], int]:
        """
        Get min/max/timestamps/count from active window.

        Returns:
            (min_value, min_timestamp, max_value, max_timestamp, count)
            or (inf, None, -inf, None, 0) if empty

        Note:
            This method returns timestamps to enable direction calculation.
            For simple min/max queries without direction, timestamps can be ignored.
        """
        try:
            min_point = self._active_window.current_min()
            max_point = self._active_window.current_max()
            count = len(self._active_window._points)
            return min_point.value, min_point.timestamp, max_point.value, max_point.timestamp, count
        except LookupError:
            return DECIMAL_POS_INF, None, DECIMAL_NEG_INF, None, 0

    def _condense_active_bucket(self) -> None:
        """
        Condense active window into a bucket aggregate.

        Creates a bucket from the active window, appends it to history,
        handles eviction, and resets the active window.
        """
        if self._current_bucket_start is None:
            return

        # Create bucket from active window
        bucket = self._create_bucket_from_active_window()
        self._buckets.append(bucket)

        # Evict old buckets if needed
        self._evict_old_buckets()

        # Mark tree for rebuild and reset active window
        self._tree_dirty = True
        self._active_window = SlidingWindowMinMax(window=timedelta(days=1))

    def _create_bucket_from_active_window(self) -> Bucket:
        """
        Create a bucket from the current active window state.

        Returns:
            Bucket with aggregated data or empty bucket if no points
        """
        # Type guard - this method is only called when _current_bucket_start is not None
        assert self._current_bucket_start is not None
        bucket_end = self._current_bucket_start + self._bucket_span

        if not self._active_window._points:
            # Empty bucket for continuity
            return Bucket(start=self._current_bucket_start, end=bucket_end)

        # Extract aggregates (O(1) operations)
        return Bucket(
            start=self._current_bucket_start,
            end=bucket_end,
            min_value=self._active_window.current_min().value,
            max_value=self._active_window.current_max().value,
            count=len(self._active_window._points),
        )

    def _evict_old_buckets(self) -> None:
        """Evict buckets beyond max_window if configured."""
        if self._max_window is not None:
            while len(self._buckets) > self._max_window:
                self._buckets.popleft()

    # ========================================================================
    # Historical Bucket Queries
    # ========================================================================

    def _query_historical_buckets(self, num_buckets: int) -> tuple[Decimal, Decimal, int]:
        """
        Query min/max/max_count from historical buckets using segment tree.

        Args:
            num_buckets: Number of bucket time spans to look back

        Returns:
            (min_value, max_value, max_count) or (inf, -inf, 0) if no data
        """
        if not self._buckets or self._current_bucket_start is None:
            return DECIMAL_POS_INF, DECIMAL_NEG_INF, 0

        # Lazy rebuild segment tree if needed
        self._rebuild_tree_if_dirty()

        # Calculate time range and find bucket indices
        lookback_start = self._current_bucket_start - num_buckets * self._bucket_span
        left_idx = self._find_first_bucket_in_range(lookback_start)

        if left_idx == -1 or self._segment_tree is None:
            return DECIMAL_POS_INF, DECIMAL_NEG_INF, 0

        # Query segment tree for O(log n) min/max/max_count
        right_idx = len(self._buckets) - 1
        tree_min, tree_max, tree_max_count = self._segment_tree.query(left_idx, right_idx)

        return tree_min, tree_max, tree_max_count

    def _rebuild_tree_if_dirty(self) -> None:
        """Rebuild segment tree if marked dirty."""
        if self._tree_dirty and self._buckets:
            self._segment_tree = SegmentTreeMinMax(self._buckets)
            self._tree_dirty = False

    def _find_first_bucket_in_range(self, lookback_start: datetime) -> int:
        """
        Binary search for first bucket within time range.

        Args:
            lookback_start: Earliest timestamp to include

        Returns:
            Index of first bucket in range, or -1 if none found
        """
        if not self._buckets:
            return -1

        # Binary search for leftmost bucket with start >= lookback_start
        left, right = 0, len(self._buckets)

        while left < right:
            mid = (left + right) // 2
            if self._buckets[mid].start < lookback_start:
                left = mid + 1
            else:
                right = mid

        # Validate result
        if left < len(self._buckets) and self._buckets[left].start >= lookback_start:
            return left

        return -1

    # ========================================================================
    # Time Alignment and Validation
    # ========================================================================

    def _align_to_bucket_boundary(self, timestamp: datetime) -> datetime:
        """
        Align timestamp to bucket boundary (floor to clock boundary).

        Args:
            timestamp: Input timestamp

        Returns:
            Bucket start timestamp

        Examples:
            For 1-minute buckets: 12:05:37 -> 12:05:00
            For 1-hour buckets: 14:23:45 -> 14:00:00
        """
        epoch = datetime(1970, 1, 1, tzinfo=timestamp.tzinfo)
        delta = timestamp - epoch
        bucket_count = int(delta.total_seconds() // self._bucket_span.total_seconds())
        return epoch + bucket_count * self._bucket_span

    def _is_new_bucket(self, bucket_start: datetime) -> bool:
        """Check if we're crossing into a new bucket."""
        return self._current_bucket_start is not None and bucket_start != self._current_bucket_start

    def _validate_timestamp(self, timestamp: datetime) -> None:
        """
        Validate that timestamp is non-decreasing.

        Args:
            timestamp: Timestamp to validate

        Raises:
            ValueError: If timestamp decreases
        """
        if self._buckets and timestamp < self._buckets[-1].end:
            raise ValueError("timestamps must be non-decreasing")
        if self._active_window._points and timestamp < self._active_window._points[-1].timestamp:
            raise ValueError("timestamps must be non-decreasing")

    # ========================================================================
    # Direction Helpers
    # ========================================================================

    @staticmethod
    def _compute_direction(
        min_value: Decimal,
        min_timestamp: Optional[datetime],
        max_value: Decimal,
        max_timestamp: Optional[datetime],
    ) -> Decimal:
        """
        Compute the signed delta between the newer and older extremum.

        A positive value indicates the most recent extremum is higher than the
        earlier one (rising), while a negative value indicates a drop.
        """
        if min_timestamp is None or max_timestamp is None:
            return Decimal(0)
        if min_timestamp <= max_timestamp:
            return max_value - min_value
        return min_value - max_value
