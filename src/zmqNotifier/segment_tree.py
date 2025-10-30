"""
Segment tree implementation for efficient range min/max queries.

This module provides a segment tree data structure optimized for
querying minimum and maximum values over arbitrary ranges in O(log n) time.
"""

from collections import deque
from math import inf
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from zmqNotifier.tick_agg import Bucket


class SegmentTreeMinMax:
    """
    Array-based segment tree for O(log n) range min/max queries.

    The tree is stored in a flat array with the following properties:
    - Node i has children at indices 2*i+1 (left) and 2*i+2 (right)
    - Each node stores (min_value, max_value) for its range
    - Empty buckets contribute (inf, -inf) and are naturally ignored

    Complexity:
    - Build: O(n)
    - Query: O(log n)
    - Space: O(n)
    """

    def __init__(self, buckets: deque["Bucket"]):
        """
        Build segment tree from buckets in O(n) time.

        Args:
            buckets: Deque of condensed buckets (empty buckets are handled)
        """
        self._n = len(buckets)

        if self._n == 0:
            self._tree: list[tuple[float, float]] = []
            return

        # Allocate tree array (4n is conservative but handles all cases)
        self._tree = [(inf, -inf)] * (4 * self._n)

        # Build tree recursively from buckets
        self._build(buckets, node=0, start=0, end=self._n - 1)

    def query(self, left_idx: int, right_idx: int) -> tuple[float, float]:
        """
        Query min/max over bucket index range in O(log n) time.

        Args:
            left_idx: Left bucket index (inclusive)
            right_idx: Right bucket index (inclusive)

        Returns:
            (min_value, max_value) over the range

        Raises:
            ValueError: If indices are out of bounds or invalid
        """
        if self._n == 0:
            return inf, -inf

        self._validate_range(left_idx, right_idx)
        return self._query_range(
            node=0, node_start=0, node_end=self._n - 1, query_left=left_idx, query_right=right_idx
        )

    def _build(self, buckets: deque["Bucket"], node: int, start: int, end: int) -> None:
        """
        Recursively build segment tree.

        Args:
            buckets: Source bucket deque
            node: Current tree node index
            start: Left boundary of range (inclusive)
            end: Right boundary of range (inclusive)
        """
        if start == end:
            # Leaf node - copy bucket values (empty buckets stay as inf, -inf)
            bucket = buckets[start]
            if not bucket.is_empty:
                self._tree[node] = (bucket.min_value, bucket.max_value)
            return

        # Internal node - recursively build children then merge
        mid = (start + end) // 2
        left_child, right_child = 2 * node + 1, 2 * node + 2

        self._build(buckets, left_child, start, mid)
        self._build(buckets, right_child, mid + 1, end)

        # Merge children values
        self._tree[node] = self._merge_values(self._tree[left_child], self._tree[right_child])

    def _query_range(
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
        # No overlap - return neutral values
        if query_right < node_start or query_left > node_end:
            return inf, -inf

        # Complete overlap - return node value directly
        if query_left <= node_start and node_end <= query_right:
            return self._tree[node]

        # Partial overlap - recurse on children and merge
        mid = (node_start + node_end) // 2
        left_child, right_child = 2 * node + 1, 2 * node + 2

        left_result = self._query_range(left_child, node_start, mid, query_left, query_right)
        right_result = self._query_range(right_child, mid + 1, node_end, query_left, query_right)

        return self._merge_values(left_result, right_result)

    def _merge_values(
        self, left: tuple[float, float], right: tuple[float, float]
    ) -> tuple[float, float]:
        """
        Merge two (min, max) tuples.

        Args:
            left: (min, max) from left child
            right: (min, max) from right child

        Returns:
            Merged (min, max) tuple
        """
        left_min, left_max = left
        right_min, right_max = right
        return min(left_min, right_min), max(left_max, right_max)

    def _validate_range(self, left_idx: int, right_idx: int) -> None:
        """
        Validate query range indices.

        Args:
            left_idx: Left index
            right_idx: Right index

        Raises:
            ValueError: If indices are invalid
        """
        if left_idx < 0 or right_idx >= self._n or left_idx > right_idx:
            raise ValueError(f"Invalid range [{left_idx}, {right_idx}] for tree size {self._n}")
