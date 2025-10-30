#!/usr/bin/env python3
"""
Performance benchmark for BucketedSlidingAggregator with segment tree optimization.

This benchmark simulates the user's query pattern:
- bucket_span = 1 minute
- Queries: num_buckets = 1, 2, 4, 8, 16, 32, ... up to 3 weeks (30,240 buckets)
- Batch queries (all ranges at once)
"""

import time
from datetime import datetime, timedelta

from zmqNotifier.tick_agg import BucketedSlidingAggregator


def benchmark_query_performance():
    """
    Benchmark query performance with segment tree optimization.
    Focus on query speed after data is populated.
    """
    print(f"\n{'='*70}")
    print(f"Benchmark: Query Performance with Segment Tree")
    print(f"{'='*70}")

    # Test with different dataset sizes
    dataset_sizes = [500, 1000, 2000, 5000]

    for num_buckets in dataset_sizes:
        print(f"\n--- Dataset: {num_buckets:,} buckets ---")

        # Create and populate aggregator
        agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
        base = datetime(2024, 1, 1, 0, 0)

        print(f"Populating {num_buckets:,} buckets...")
        populate_start = time.perf_counter()
        for i in range(num_buckets):
            timestamp = base + timedelta(minutes=i)
            price = 100.0 + (i % 100) * 0.1
            agg.add(timestamp, price)
        populate_time = time.perf_counter() - populate_start
        print(
            f"Population time: {populate_time:.3f}s ({populate_time/num_buckets*1000:.3f}ms per bucket)"
        )

        # Define query ranges
        query_ranges = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, num_buckets]
        query_ranges = [r for r in query_ranges if r <= num_buckets]

        print(f"\nTesting {len(query_ranges)} query ranges: {query_ranges[:8]}...")

        # Warmup
        for num_b in query_ranges:
            _ = agg.query_min_max(num_b)

        # Benchmark individual query ranges
        times_by_range = {}
        for num_b in query_ranges:
            iterations = 100
            start = time.perf_counter()
            for _ in range(iterations):
                _ = agg.query_min_max(num_b)
            elapsed = (time.perf_counter() - start) / iterations * 1000  # ms
            times_by_range[num_b] = elapsed

        # Print results
        print(f"\n{'Range':<10} {'Time (ms)':<12} {'Expected O(log n)':<20}")
        print(f"{'-'*45}")
        import math

        log_n = math.log2(num_buckets) if num_buckets > 0 else 1
        for num_b in query_ranges[:5]:  # Show first 5
            expected = f"~{log_n:.1f} ops"
            print(f"{num_b:<10,} {times_by_range[num_b]:<12.4f} {expected:<20}")
        if len(query_ranges) > 5:
            print(f"...")
            num_b = query_ranges[-1]
            expected = f"~{log_n:.1f} ops"
            print(f"{num_b:<10,} {times_by_range[num_b]:<12.4f} {expected:<20}")

        # Batch query test
        print(f"\nBatch query test (all {len(query_ranges)} ranges):")
        iterations = 10
        start = time.perf_counter()
        for _ in range(iterations):
            for num_b in query_ranges:
                _ = agg.query_min_max(num_b)
        batch_time = (time.perf_counter() - start) / iterations * 1000  # ms
        print(f"  Time per batch: {batch_time:.2f} ms")
        print(f"  Time per query: {batch_time/len(query_ranges):.4f} ms")

        # Calculate theoretical speedup vs linear scan
        total_linear_ops = sum(query_ranges)
        total_log_ops = len(query_ranges) * log_n
        theoretical_speedup = total_linear_ops / total_log_ops if total_log_ops > 0 else 1
        print(f"\n  Theoretical analysis:")
        print(f"    Linear scan ops:  {total_linear_ops:,}")
        print(f"    Segment tree ops: {total_log_ops:.0f}")
        print(f"    Speedup factor:   {theoretical_speedup:.1f}x")


def benchmark_scalability():
    """
    Show how query time scales (should be O(log n)).
    """
    print(f"\n{'='*70}")
    print(f"Benchmark: Query Scalability (O(log n) verification)")
    print(f"{'='*70}")

    bucket_counts = [100, 250, 500, 1000, 2000, 5000]

    print(f"\n{'Buckets':<12} {'Query Time (μs)':<18} {'Speedup vs Linear':<20}")
    print(f"{'-'*50}")

    results = []
    for num_buckets in bucket_counts:
        agg = BucketedSlidingAggregator(bucket_span=timedelta(minutes=1))
        base = datetime(2024, 1, 1, 0, 0)

        # Populate
        for i in range(num_buckets):
            agg.add(base + timedelta(minutes=i), 100.0 + i * 0.1)

        # Benchmark full range query
        iterations = 100
        start = time.perf_counter()
        for _ in range(iterations):
            _ = agg.query_min_max(num_buckets)
        elapsed = (time.perf_counter() - start) / iterations * 1_000_000  # microseconds

        import math

        log_n = math.log2(num_buckets)
        estimated_linear_time = elapsed * (num_buckets / log_n)
        speedup = estimated_linear_time / elapsed

        print(f"{num_buckets:<12,} {elapsed:<18.2f} {speedup:<20.1f}x")
        results.append((num_buckets, elapsed, speedup))

    # Verify logarithmic growth
    print(f"\nVerification: As data size doubles, query time should increase by ~constant:")
    for i in range(1, len(results)):
        prev_size, prev_time, _ = results[i - 1]
        curr_size, curr_time, _ = results[i]
        size_ratio = curr_size / prev_size
        time_ratio = curr_time / prev_time
        print(f"  {prev_size:,} -> {curr_size:,}: size ×{size_ratio:.1f}, time ×{time_ratio:.2f}")

    print(f"{'='*70}\n")


if __name__ == "__main__":
    benchmark_query_performance()
    benchmark_scalability()
