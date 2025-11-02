"""
The double-deque (monotonic queue) approach is both faster and simpler for this use case.

- Deques do O(1) amortized work per point: each element enqueues once and dequeues at most once. Lookups for min/max are straight from the front, so you avoid heap push/pop overhead.
- The heap-based version needs extra bookkeeping (lazy deletion, ID maps) and still pays O(log n) per insert due to the heap operations and occasional deletions.
- With strictly increasing timestamps and a single expiration horizon, deques are deterministic and require no random access—perfect match for your “one-hour sliding window” use case.
- Heaps shine only if you need arbitrary removals or more complex order statistics; otherwise they bring more CPU work and code complexity.

So stick with the monotonic deques unless you anticipate requirements that deques can’t satisfy.
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Deque
from decimal import Decimal


@dataclass
class WindowPoint:
    timestamp: datetime
    value: Decimal


class SlidingWindowMinMax:
    """
    Tracks min/max over a rolling time window in O(1) amortized per update.
    https://leetcode.com/problems/sliding-window-maximum/
    """

    def __init__(self, window: timedelta):
        if window <= timedelta(0):
            raise ValueError("window must be positive")
        self._window = window
        self._points: Deque[WindowPoint] = deque()
        self._min_candidates: Deque[WindowPoint] = deque()
        self._max_candidates: Deque[WindowPoint] = deque()

    def add(self, timestamp: datetime, value: Decimal) -> None:
        if self._points and timestamp < self._points[-1].timestamp:
            raise ValueError("timestamps must be non-decreasing")
        if not isinstance(value, Decimal):
            value = Decimal(str(value))
        point = WindowPoint(timestamp, value)
        self._points.append(point)

        while self._min_candidates and self._min_candidates[-1].value >= value:
            self._min_candidates.pop()
        self._min_candidates.append(point)

        while self._max_candidates and self._max_candidates[-1].value <= value:
            self._max_candidates.pop()
        self._max_candidates.append(point)

        self._expire(timestamp)

    def current_min(self):
        if not self._min_candidates:
            raise LookupError("window is empty")
        return self._min_candidates[0]

    def current_max(self):
        if not self._max_candidates:
            raise LookupError("window is empty")
        return self._max_candidates[0]

    def _expire(self, now: datetime) -> None:
        cutoff = now - self._window
        while self._points and self._points[0].timestamp <= cutoff:
            expired = self._points.popleft()
            if self._min_candidates and self._min_candidates[0] is expired:
                self._min_candidates.popleft()
            if self._max_candidates and self._max_candidates[0] is expired:
                self._max_candidates.popleft()


from heapq import heappop, heappush
from typing import Deque


class SlidingWindowMinMaxHeap:
    """
    Maintain min/max values over a time-based sliding window using heaps.
    Refer to heapq.txt, similar to workaround. This implementation can be
    more concise actually
    """

    def __init__(self, window: timedelta):
        if window <= timedelta(0):
            raise ValueError("window must be positive")
        self._window = window
        self._entries: Deque[tuple[datetime, int]] = deque()
        self._min_heap: list[tuple[Decimal, int, datetime]] = []
        self._max_heap: list[tuple[Decimal, int, datetime]] = []
        self._valid_ids: dict[int, tuple[datetime, Decimal]] = {}
        self._next_id = 0
        self._last_timestamp: datetime | None = None

    def add(self, timestamp: datetime, value: Decimal) -> None:
        if self._last_timestamp and timestamp < self._last_timestamp:
            raise ValueError("timestamps must be non-decreasing")
        if not isinstance(value, Decimal):
            value = Decimal(str(value))
        self._last_timestamp = timestamp

        entry_id = self._next_id
        self._next_id += 1

        self._entries.append((timestamp, entry_id))
        self._valid_ids[entry_id] = (timestamp, value)

        heappush(self._min_heap, (value, entry_id, timestamp))
        heappush(self._max_heap, (-value, entry_id, timestamp))

        self._expire(timestamp)

    def current_min(self) -> WindowPoint:
        self._prune(self._min_heap)
        if not self._min_heap:
            raise LookupError("window is empty")
        wp = WindowPoint(self._min_heap[0][2], self._min_heap[0][0])
        return wp

    def current_max(self) -> WindowPoint:
        self._prune(self._max_heap)
        if not self._max_heap:
            raise LookupError("window is empty")
        wp = WindowPoint(self._max_heap[0][2], -self._max_heap[0][0])
        return wp

    def _expire(self, now: datetime) -> None:
        cutoff = now - self._window
        while self._entries and self._entries[0][0] <= cutoff:
            _, entry_id = self._entries.popleft()
            self._valid_ids.pop(entry_id, None)
        self._prune(self._min_heap)
        self._prune(self._max_heap)

    def _prune(self, heap: list[tuple[Decimal, int, datetime]]) -> None:
        while heap and heap[0][1] not in self._valid_ids:
            heappop(heap)


"""
Q: it seems max and min Treaps directly solve for each small time window, but too complex

A: A treap would add a lot of moving parts here without giving you any real benefit.

- You’d still need to expire old points, so you’d be doing deletions keyed by timestamp plus maintaining augmented data for min/max—extra code and log-time work per update.
- For a simple “append new data, drop anything older than one hour, tell me min/max now” workflow, a monotonic deque gives you O(1) updates with far less bookkeeping.
- Treaps make sense when you need richer order statistics (k-th element, split/merge, persistence) or arbitrary-position updates. Your scenario only needs sliding-window extrema.

So keep the deque-based solution; it’s the most direct and efficient fit.
"""
