from datetime import datetime, timedelta

import pytest
from zmqNotifier.sliding_windows import SlidingWindowMinMax


def test_min_max_updates_within_window2():
    window = SlidingWindowMinMax(timedelta(hours=1))
    base = datetime(2024, 1, 1, 12, 0, 0)

    window.add(base, 10)
    window.add(base + timedelta(minutes=10), 5)
    window.add(base + timedelta(minutes=20), 20)

    assert window.current_min() == 5
    assert window.current_max() == 20


def test_values_expire_after_window2():
    window = SlidingWindowMinMax(timedelta(minutes=30))
    base = datetime(2024, 1, 1, 12, 0, 0)

    window.add(base, 1)
    window.add(base + timedelta(minutes=10), 2)
    window.add(base + timedelta(minutes=40), 3)  # expires first two points

    assert window.current_min() == 3
    assert window.current_max() == 3

from datetime import datetime, timedelta

from zmqNotifier.sliding_windows import SlidingWindowMinMaxHeap

def test_min_max_updates_within_window():
    window = SlidingWindowMinMaxHeap(timedelta(hours=1))
    base = datetime(2024, 1, 1, 12, 0, 0)

    window.add(base, 10)
    window.add(base + timedelta(minutes=10), 5)
    window.add(base + timedelta(minutes=20), 20)

    assert window.current_min() == 5
    assert window.current_min() == 5
    assert window.current_max() == 20


def test_values_expire_after_window():
    window = SlidingWindowMinMaxHeap(timedelta(minutes=30))
    base = datetime(2024, 1, 1, 12, 0, 0)

    window.add(base, 1)
    window.add(base + timedelta(minutes=10), 2)
    window.add(base + timedelta(minutes=40), 3)

    assert window.current_min() == 3
    assert window.current_max() == 3


def test_empty_window_raises():
    window = SlidingWindowMinMaxHeap(timedelta(minutes=5))

    with pytest.raises(LookupError):
        window.current_min()

    with pytest.raises(LookupError):
        window.current_max()


def test_non_monotonic_timestamp_rejected():
    window = SlidingWindowMinMaxHeap(timedelta(minutes=5))
    base = datetime(2024, 1, 1, 12, 0, 0)

    window.add(base, 1)

    with pytest.raises(ValueError):
        window.add(base - timedelta(seconds=1), 2)
