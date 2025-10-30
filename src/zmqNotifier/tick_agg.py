# Tick Aggregator with bucketed sliding window support.
# Uses monotonic deques for active bucket, condenses to aggregates on boundary crossing.

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import inf
from typing import Optional

from zmqNotifier.sliding_windows import SlidingWindowMinMax


@dataclass
class Bucket:
    """Aggregate for one fixed-size time bucket (condensed from active deque)."""

    start: datetime
    end: datetime
    min_value: float = inf
    max_value: float = -inf
    count: int = 0

    @property
    def is_empty(self) -> bool:
        return self.count == 0


class BucketedSlidingAggregator:
    """
    Maintains one active monotonic deque for current bucket, plus historical
    condensed buckets. Buckets align to clock boundaries.

    - add(): Adds ticks to active deque, condenses on boundary crossing
    - query_min_max(num_buckets): Returns min/max from active bucket + last N time spans
      - num_buckets=0: active bucket only
      - num_buckets>0: active bucket + last N*bucket_span of time (gaps ignored)
    """

    def __init__(self, bucket_span: timedelta, max_window: Optional[int] = None):
        """
        Args:
            bucket_span: Time span for each bucket (e.g., timedelta(minutes=1))
            max_window: Maximum number of buckets to retain (None = unlimited)
        """
        if bucket_span <= timedelta(0):
            raise ValueError("bucket_span must be positive")
        self._bucket_span = bucket_span
        self._max_window = max_window

        # Historical condensed buckets
        self._buckets: deque[Bucket] = deque()

        # Active sliding window for current bucket (reusing SlidingWindowMinMax)
        self._active_window = SlidingWindowMinMax(window=timedelta(days=1))
        self._current_bucket_start: Optional[datetime] = None

    def add(self, timestamp: datetime, value: float) -> None:
        """Add a tick to the aggregator. Condenses active deque on bucket boundary crossing."""
        # Check for non-decreasing timestamps
        if self._buckets and timestamp < self._buckets[-1].end:
            raise ValueError("timestamps must be non-decreasing")
        if self._active_window._points and timestamp < self._active_window._points[-1].timestamp:
            raise ValueError("timestamps must be non-decreasing")

        bucket_start = self._bucket_start_for(timestamp)

        # Detect boundary crossing and condense
        if self._current_bucket_start is not None and bucket_start != self._current_bucket_start:
            self._condense_active_bucket()
            # Update to new bucket start after condensing
            self._current_bucket_start = bucket_start

        # Initialize bucket start if first point
        if self._current_bucket_start is None:
            self._current_bucket_start = bucket_start

        # Add to active window (SlidingWindowMinMax handles the deque logic)
        self._active_window.add(timestamp, value)

    def query_min_max(self, num_buckets: int = 0) -> tuple[float, float]:
        """
        Query min/max over active bucket + last N bucket time spans.

        Args:
            num_buckets: Number of bucket time spans to look back (0 = active only)
                         e.g., num_buckets=3 means look back 3*bucket_span of time
                         Empty buckets (gaps) in the time range are naturally ignored

        Returns:
            (min_value, max_value)

        Raises:
            ValueError: If num_buckets is negative
            LookupError: If the window is empty
        """
        if num_buckets < 0:
            raise ValueError("num_buckets must be non-negative")

        min_value, max_value = inf, -inf

        # Get min/max from active window (SlidingWindowMinMax handles the deques)
        try:
            min_value = float(self._active_window.current_min().value)
            max_value = float(self._active_window.current_max().value)
        except LookupError:
            # Active window is empty, will rely on buckets
            pass

        # Calculate time-based lookback range
        if num_buckets > 0 and self._current_bucket_start is not None:
            lookback_start = self._current_bucket_start - num_buckets * self._bucket_span

            # Scan all condensed buckets that fall within the time range
            for bucket in self._buckets:
                if bucket.start >= lookback_start and not bucket.is_empty:
                    min_value = min(min_value, bucket.min_value)
                    max_value = max(max_value, bucket.max_value)

        if min_value is inf:
            raise LookupError("window is empty")

        return min_value, max_value

    def _bucket_start_for(self, timestamp: datetime) -> datetime:
        """
        Align timestamp to bucket boundary (floor to clock boundary).
        E.g., for 1-minute buckets: 12:05:37 -> 12:05:00
        """
        # Use epoch-based alignment for general case
        epoch = datetime(1970, 1, 1, tzinfo=timestamp.tzinfo)
        delta = timestamp - epoch
        bucket_count = int(delta.total_seconds() // self._bucket_span.total_seconds())
        return epoch + bucket_count * self._bucket_span

    def _condense_active_bucket(self) -> None:
        """
        Condense the active window into a bucket aggregate and append to history.
        Clear active window after condensation.
        """
        if self._current_bucket_start is None:
            return  # Nothing to condense

        if not self._active_window._points:
            # No points to condense, but create empty bucket for continuity
            bucket = Bucket(
                start=self._current_bucket_start, end=self._current_bucket_start + self._bucket_span
            )
        else:
            # Extract aggregates from SlidingWindowMinMax (O(1) operations)
            min_value = float(self._active_window.current_min().value)
            max_value = float(self._active_window.current_max().value)
            count = len(self._active_window._points)

            bucket = Bucket(
                start=self._current_bucket_start,
                end=self._current_bucket_start + self._bucket_span,
                min_value=min_value,
                max_value=max_value,
                count=count,
            )

        self._buckets.append(bucket)

        # Clear active window by creating a new instance
        self._active_window = SlidingWindowMinMax(window=timedelta(days=1))

        # Expire old buckets if max_window is set
        if self._max_window is not None:
            while len(self._buckets) > self._max_window:
                self._buckets.popleft()
