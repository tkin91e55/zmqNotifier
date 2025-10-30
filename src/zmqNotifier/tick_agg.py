# Tick Aggregator with bucketed sliding window support.
# Uses monotonic deque for active bucket, condenses to aggregates on boundary crossing.

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


class SegmentTreeMinMax:
    """
    Array-based segment tree for O(log n) range min/max queries.

    Tree layout (0-indexed):
    - Node i has children at 2*i+1 (left) and 2*i+2 (right)
    - Leaf nodes start at index n-1 where n = len(tree)//2 + 1
    - Each node stores (min_value, max_value) for its range
    """

    def __init__(self, buckets: deque[Bucket]):
        """
        Build segment tree from buckets in O(n) time.

        Args:
            buckets: Deque of condensed buckets (only non-empty buckets contribute)
        """
        n = len(buckets)
        self._n = n

        if n == 0:
            self._tree: list[tuple[float, float]] = []
            return

        # Tree size: need 4*n space for complete binary tree (conservative but safe)
        tree_size = 4 * n
        self._tree = [(inf, -inf)] * tree_size

        # Build tree bottom-up
        self._build(buckets, 0, 0, n - 1)

    def _build(self, buckets: deque[Bucket], node: int, start: int, end: int) -> None:
        """
        Recursively build segment tree.

        Args:
            buckets: Source bucket deque
            node: Current tree node index
            start: Left boundary of range (inclusive)
            end: Right boundary of range (inclusive)
        """
        if start == end:
            # Leaf node - copy bucket values (skip empty buckets)
            bucket = buckets[start]
            if not bucket.is_empty:
                self._tree[node] = (bucket.min_value, bucket.max_value)
            return

        mid = (start + end) // 2
        left_child = 2 * node + 1
        right_child = 2 * node + 2

        # Recursively build children
        self._build(buckets, left_child, start, mid)
        self._build(buckets, right_child, mid + 1, end)

        # Merge children into current node
        left_min, left_max = self._tree[left_child]
        right_min, right_max = self._tree[right_child]
        self._tree[node] = (min(left_min, right_min), max(left_max, right_max))

    def query(self, left_idx: int, right_idx: int) -> tuple[float, float]:
        """
        Query min/max over bucket index range [left_idx, right_idx] in O(log n) time.

        Args:
            left_idx: Left bucket index (inclusive)
            right_idx: Right bucket index (inclusive)

        Returns:
            (min_value, max_value) over the range

        Raises:
            ValueError: If indices are out of bounds or inverted
        """
        if self._n == 0:
            return inf, -inf

        if left_idx < 0 or right_idx >= self._n or left_idx > right_idx:
            raise ValueError(f"Invalid range [{left_idx}, {right_idx}] for tree size {self._n}")

        return self._query(0, 0, self._n - 1, left_idx, right_idx)

    def _query(
        self, node: int, node_start: int, node_end: int, query_left: int, query_right: int
    ) -> tuple[float, float]:
        """
        Recursive range query helper.

        Args:
            node: Current tree node index
            node_start: Start of node's range
            node_end: End of node's range
            query_left: Query range start
            query_right: Query range end

        Returns:
            (min_value, max_value) for the overlapping range
        """
        # No overlap
        if query_right < node_start or query_left > node_end:
            return inf, -inf

        # Complete overlap - return node value
        if query_left <= node_start and node_end <= query_right:
            return self._tree[node]

        # Partial overlap - recurse on children
        mid = (node_start + node_end) // 2
        left_child = 2 * node + 1
        right_child = 2 * node + 2

        left_min, left_max = self._query(left_child, node_start, mid, query_left, query_right)
        right_min, right_max = self._query(right_child, mid + 1, node_end, query_left, query_right)

        return min(left_min, right_min), max(left_max, right_max)

    def rebuild(self, buckets: deque[Bucket]) -> None:
        """
        Rebuild entire tree from scratch (used after bucket eviction).

        Args:
            buckets: Updated deque of buckets
        """
        self.__init__(buckets)


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

        # Segment tree for O(log n) range queries (lazily built)
        self._segment_tree: Optional[SegmentTreeMinMax] = None
        self._tree_dirty = True  # Track if tree needs rebuilding

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

        # Calculate time-based lookback range and use segment tree for O(log n) query
        if num_buckets > 0 and self._current_bucket_start is not None and self._buckets:
            # Rebuild tree if dirty (lazy rebuilding for better amortized performance)
            if self._tree_dirty:
                self._segment_tree = SegmentTreeMinMax(self._buckets)
                self._tree_dirty = False

            lookback_start = self._current_bucket_start - num_buckets * self._bucket_span

            # Binary search to find first bucket in time range
            left_idx = self._find_first_bucket_in_range(lookback_start)

            if left_idx != -1:
                # Use segment tree for O(log n) range query
                right_idx = len(self._buckets) - 1
                if self._segment_tree is not None:
                    tree_min, tree_max = self._segment_tree.query(left_idx, right_idx)
                    if tree_min != inf:  # Check if we got valid results
                        min_value = min(min_value, tree_min)
                        max_value = max(max_value, tree_max)

        if min_value is inf:
            raise LookupError("window is empty")

        return min_value, max_value

    def _find_first_bucket_in_range(self, lookback_start: datetime) -> int:
        """
        Binary search to find the index of the first bucket with start >= lookback_start.

        Args:
            lookback_start: The earliest timestamp to include in the range

        Returns:
            Index of first bucket in range, or -1 if no buckets are in range
        """
        if not self._buckets:
            return -1

        # We need to find the leftmost bucket where bucket.start >= lookback_start
        left, right = 0, len(self._buckets)

        while left < right:
            mid = (left + right) // 2
            if self._buckets[mid].start < lookback_start:
                left = mid + 1
            else:
                right = mid

        # Check if we found a valid index
        if left < len(self._buckets) and self._buckets[left].start >= lookback_start:
            return left

        return -1

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

        # Expire old buckets if max_window is set
        if self._max_window is not None:
            while len(self._buckets) > self._max_window:
                self._buckets.popleft()

        # Mark tree as dirty (will be rebuilt on next query)
        self._tree_dirty = True

        # Clear active window by creating a new instance
        self._active_window = SlidingWindowMinMax(window=timedelta(days=1))
