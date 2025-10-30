"""
Tick aggregator with bucketed sliding window support.

This module provides a high-performance tick aggregator that:
- Groups ticks into fixed-size time buckets (clock-aligned)
- Maintains active bucket using monotonic deque
- Condenses buckets to aggregates on boundary crossing
- Supports O(log n) range queries via segment tree
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import inf
from typing import Optional

from zmqNotifier.segment_tree import SegmentTreeMinMax
from zmqNotifier.sliding_windows import SlidingWindowMinMax


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
    min_value: float = inf
    max_value: float = -inf
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
        min_val, max_val = agg.query_min_max(num_buckets=60)  # Last 60 minutes
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

    def add(self, timestamp: datetime, value: float) -> None:
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

    def query_min_max(self, num_buckets: int = 0) -> tuple[float, float]:
        """
        Query min/max over active bucket + historical time range.

        Args:
            num_buckets: Number of bucket time spans to look back (0 = active only)
                        e.g., num_buckets=3 means look back 3*bucket_span of time
                        Empty buckets (gaps) are naturally ignored

        Returns:
            (min_value, max_value) tuple

        Raises:
            ValueError: If num_buckets is negative
            LookupError: If the window is empty

        Examples:
            # Active bucket only
            min_val, max_val = agg.query_min_max(0)

            # Last hour (60 one-minute buckets)
            min_val, max_val = agg.query_min_max(60)
        """
        if num_buckets < 0:
            raise ValueError("num_buckets must be non-negative")

        # Start with active window min/max
        min_value, max_value = self._get_active_window_minmax()

        # Add historical buckets if requested
        if num_buckets > 0:
            hist_min, hist_max = self._query_historical_buckets(num_buckets)
            min_value = min(min_value, hist_min)
            max_value = max(max_value, hist_max)

        # Check if we found any data
        if min_value is inf:
            raise LookupError("window is empty")

        return min_value, max_value

    # ========================================================================
    # Active Window Management
    # ========================================================================

    def _get_active_window_minmax(self) -> tuple[float, float]:
        """
        Get min/max from active window.

        Returns:
            (min_value, max_value) or (inf, -inf) if empty
        """
        try:
            min_val = float(self._active_window.current_min().value)
            max_val = float(self._active_window.current_max().value)
            return min_val, max_val
        except LookupError:
            return inf, -inf

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
        bucket_end = self._current_bucket_start + self._bucket_span

        if not self._active_window._points:
            # Empty bucket for continuity
            return Bucket(start=self._current_bucket_start, end=bucket_end)

        # Extract aggregates (O(1) operations)
        return Bucket(
            start=self._current_bucket_start,
            end=bucket_end,
            min_value=float(self._active_window.current_min().value),
            max_value=float(self._active_window.current_max().value),
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

    def _query_historical_buckets(self, num_buckets: int) -> tuple[float, float]:
        """
        Query min/max from historical buckets using segment tree.

        Args:
            num_buckets: Number of bucket time spans to look back

        Returns:
            (min_value, max_value) or (inf, -inf) if no data
        """
        if not self._buckets or self._current_bucket_start is None:
            return inf, -inf

        # Lazy rebuild segment tree if needed
        self._rebuild_tree_if_dirty()

        # Calculate time range and find bucket indices
        lookback_start = self._current_bucket_start - num_buckets * self._bucket_span
        left_idx = self._find_first_bucket_in_range(lookback_start)

        if left_idx == -1 or self._segment_tree is None:
            return inf, -inf

        # Query segment tree for O(log n) min/max
        right_idx = len(self._buckets) - 1
        tree_min, tree_max = self._segment_tree.query(left_idx, right_idx)

        return tree_min, tree_max

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
