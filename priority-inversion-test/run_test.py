#!/usr/bin/env python3
"""
Priority Inversion Prevention Test Runner (Test 4)
==============================================================================

This script demonstrates that batch workloads cannot starve interactive traffic
by showing premium requests bypass standard queue flood and maintain low latency.

Test phases:
  Phase 1 (0-30s): Standard flood builds (50 requests queued)
  Phase 2 (30-60s): Premium bypass test (5 premium cut the queue)
  Phase 3 (60-90s): Cooldown

Expected outcome:
  - Premium requests bypass 50-deep standard queue
  - Premium P95 TTFT stays low despite massive standard backlog
  - No priority inversion: interactive traffic never waits for batch

Usage:
    python3 run_test.py --duration 90
    python3 run_test.py --duration 90 --gateway-url http://custom-gateway/llm-test/qwen32b-a/v1/completions
"""

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass
from typing import List

import aiohttp

from traffic_generator import MetricsCollector, RequestGenerator


# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Default gateway URL (can override with --gateway-url)
DEFAULT_GATEWAY = "http://aefc7e10f44604760a801dfb2c34b36b-64674702.us-west-2.elb.amazonaws.com"

# Endpoint - using qwen32b-a for all tenants
ENDPOINT = f"{DEFAULT_GATEWAY}/llm-test/qwen32b-a/v1/completions"

# Model name
MODEL_NAME = "Qwen/Qwen2.5-0.5B-Instruct"

# Request configuration
INPUT_TOKENS = 100
OUTPUT_TOKENS = 100


# ==============================================================================
# DATA MODELS
# ==============================================================================

@dataclass
class TenantConfig:
    """Configuration for a single tenant."""
    fairness_id: str
    priority: int
    objective: str
    endpoint: str
    base_concurrency: int
    phase_configs: List[dict]  # List of {start_time, duration, concurrency}


# ==============================================================================
# METRICS COLLECTOR
# ==============================================================================

class TestMetricsCollector(MetricsCollector):
    """Extends shared MetricsCollector with get_stats for test reporting."""

    def get_stats(self, tenant_id: str) -> dict:
        """Get current statistics for a tenant."""
        ttfts = self.ttft_samples.get(tenant_id, [])
        durations = self.duration_samples.get(tenant_id, [])

        stats = {
            "tenant_id": tenant_id,
            "total_requests": sum(self.status_counts.get(tenant_id, {}).values()),
            "active_requests": self.active_requests.get(tenant_id, 0),
            "status_counts": dict(self.status_counts.get(tenant_id, {})),
        }

        if ttfts:
            ttfts_sorted = sorted(ttfts)
            n = len(ttfts_sorted)
            stats["ttft_p50"] = ttfts_sorted[int(n * 0.5)] if n > 0 else None
            stats["ttft_p95"] = ttfts_sorted[int(n * 0.95)] if n > 0 else None
            stats["ttft_p99"] = ttfts_sorted[int(n * 0.99)] if n > 0 else None
            stats["ttft_min"] = ttfts_sorted[0]
            stats["ttft_max"] = ttfts_sorted[-1]

        if durations:
            dur_sorted = sorted(durations)
            n = len(dur_sorted)
            stats["duration_p50"] = dur_sorted[int(n * 0.5)] if n > 0 else None
            stats["duration_p95"] = dur_sorted[int(n * 0.95)] if n > 0 else None

        # Calculate QPS
        start_times = self.start_times.get(tenant_id, [])
        if start_times:
            time_span = time.time() - min(start_times)
            if time_span > 0:
                stats["qps"] = len(start_times) / time_span
            else:
                stats["qps"] = 0
        else:
            stats["qps"] = 0

        return stats


# ==============================================================================
# DYNAMIC CONCURRENCY CONTROLLER
# ==============================================================================

class DynamicConcurrencyController:
    """Manages dynamic concurrency changes based on time-based phases."""

    def __init__(self, generator: RequestGenerator, phase_configs: List[dict]):
        self.generator = generator
        self.phase_configs = phase_configs
        self.start_time = None

    def get_target_concurrency(self) -> int:
        """Get target concurrency based on current elapsed time."""
        if self.start_time is None:
            self.start_time = time.time()
            return self.generator.base_concurrency

        elapsed = time.time() - self.start_time

        # Find active phase
        for phase in self.phase_configs:
            phase_start = phase["start_time"]
            phase_end = phase_start + phase["duration"]
            if phase_start <= elapsed < phase_end:
                return phase["concurrency"]

        # Default to base concurrency if no phase matches
        return self.generator.base_concurrency


# ==============================================================================
# UI RENDERING
# ==============================================================================

def render_dashboard(
    elapsed: float,
    total: float,
    tenants: List[TenantConfig],
    metrics: MetricsCollector,
    is_first_render: bool = False
):
    """Render real-time metrics dashboard."""
    num_lines = len(tenants) + 7

    if not is_first_render:
        sys.stdout.write(f"\033[{num_lines}A")  # Move cursor up

    out = []
    out.append(f"\033[K\033[1;36mTest 4: Priority Inversion Prevention\033[0m")
    out.append(f"\033[KElapsed: {int(elapsed)}s / {int(total)}s")
    out.append("\033[K" + "=" * 120)
    out.append(f"\033[K{'TENANT':<20} | {'PRI':<4} | {'TARGET':<6} | {'ACTIVE':<6} | {'QPS':<6} | {'P50 TTFT':<10} | {'P95 TTFT':<10} | {'P99 TTFT':<10} | {'200s':<5} | {'429s':<5} | {'503s':<5}")
    out.append("\033[K" + "-" * 120)

    for tenant in sorted(tenants, key=lambda t: t.priority, reverse=True):
        stats = metrics.get_stats(tenant.fairness_id)

        p50_ttft = f"{stats.get('ttft_p50', 0)*1000:.0f}ms" if stats.get('ttft_p50') else "-"
        p95_ttft = f"{stats.get('ttft_p95', 0)*1000:.0f}ms" if stats.get('ttft_p95') else "-"
        p99_ttft = f"{stats.get('ttft_p99', 0)*1000:.0f}ms" if stats.get('ttft_p99') else "-"

        # Color code P95 TTFT
        p95_val = stats.get('ttft_p95', 0)
        if p95_val and p95_val < 2.0:
            p95_color = "\033[1;32m"  # Green
        elif p95_val and p95_val < 5.0:
            p95_color = "\033[1;33m"  # Yellow
        else:
            p95_color = "\033[1;31m"  # Red
        p95_ttft_colored = f"{p95_color}{p95_ttft}\033[0m" if stats.get('ttft_p95') else "-"

        counts = stats.get('status_counts', {})

        # Get current target concurrency
        target = tenant.base_concurrency

        out.append(
            f"\033[K{tenant.fairness_id:<20} | "
            f"{tenant.priority:<4} | "
            f"{target:<6} | "
            f"{stats['active_requests']:<6} | "
            f"{stats.get('qps', 0):<6.1f} | "
            f"{p50_ttft:<10} | "
            f"{p95_ttft_colored:<20} | "
            f"{p99_ttft:<10} | "
            f"{counts.get('200', 0):<5} | "
            f"{counts.get('429', 0):<5} | "
            f"{counts.get('503', 0):<5}"
        )

    out.append("\033[K" + "=" * 120)

    sys.stdout.write("\n".join(out) + "\n")
    sys.stdout.flush()


# ==============================================================================
# MAIN TEST RUNNER
# ==============================================================================

async def run_test(args: argparse.Namespace):
    """Run the priority inversion prevention test."""
    duration = args.duration

    # Override gateway URL if provided
    endpoint = ENDPOINT
    if args.gateway_url:
        endpoint = args.gateway_url

    # Test 4 configuration: 3 premium + 2 standard tenants
    # Phase 1 (0-60s): Flood with 50 standard requests (25 each tenant)
    # Phase 2 (30-60s): 5 premium requests arrive (1.7 each tenant) and should bypass queue
    # Phase 3 (60-90s): Cooldown (all to 0)

    tenants = [
        # Premium tenants (priority 100) - 0 conc initially, 5 total at 30s
        TenantConfig(
            fairness_id="premium-tenant-a",
            priority=100,
            objective="Bypass standard queue",
            endpoint=endpoint,
            base_concurrency=0,
            phase_configs=[
                {"start_time": 0, "duration": 30, "concurrency": 0},    # 0-30s: silent
                {"start_time": 30, "duration": 30, "concurrency": 2},   # 30-60s: 1.7 rounded
                {"start_time": 60, "duration": 30, "concurrency": 0},   # 60-90s: cooldown
            ]
        ),
        TenantConfig(
            fairness_id="premium-tenant-b",
            priority=100,
            objective="Bypass standard queue",
            endpoint=endpoint,
            base_concurrency=0,
            phase_configs=[
                {"start_time": 0, "duration": 30, "concurrency": 0},
                {"start_time": 30, "duration": 30, "concurrency": 2},
                {"start_time": 60, "duration": 30, "concurrency": 0},
            ]
        ),
        TenantConfig(
            fairness_id="premium-tenant-c",
            priority=100,
            objective="Bypass standard queue",
            endpoint=endpoint,
            base_concurrency=0,
            phase_configs=[
                {"start_time": 0, "duration": 30, "concurrency": 0},
                {"start_time": 30, "duration": 30, "concurrency": 1},   # 1.6 rounded down
                {"start_time": 60, "duration": 30, "concurrency": 0},
            ]
        ),
        # Standard tenants (priority 0) - heavy load 0-60s, then cooldown
        TenantConfig(
            fairness_id="standard-tenant-a",
            priority=0,
            objective="Batch workload",
            endpoint=endpoint,
            base_concurrency=25,
            phase_configs=[
                {"start_time": 0, "duration": 60, "concurrency": 25},   # 0-60s: heavy
                {"start_time": 60, "duration": 30, "concurrency": 0},   # 60-90s: cooldown
            ]
        ),
        TenantConfig(
            fairness_id="standard-tenant-b",
            priority=0,
            objective="Batch workload",
            endpoint=endpoint,
            base_concurrency=25,
            phase_configs=[
                {"start_time": 0, "duration": 60, "concurrency": 25},
                {"start_time": 60, "duration": 30, "concurrency": 0},
            ]
        ),
    ]

    print(f"\n\033[1;36m{'=' * 80}\033[0m")
    print(f"\033[1;36mTest 4: Priority Inversion Prevention\033[0m")
    print(f"\033[1;36m{'=' * 80}\033[0m\n")
    print(f"Duration: {duration}s")
    print(f"Endpoint: {endpoint}")
    print(f"\nTest phases:")
    print(f"  Phase 1 (0-30s): Standard flood builds (50 requests queued)")
    print(f"  Phase 2 (30-60s): Premium bypass test (5 premium cut the queue)")
    print(f"  Phase 3 (60-90s): Cooldown")
    print(f"\nExpected: Premium bypasses standard queue, maintaining low latency")
    print(f"\nStarting test in 3 seconds...\n")
    await asyncio.sleep(3)

    # Initialize
    metrics = TestMetricsCollector()
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)
    session = aiohttp.ClientSession(connector=connector)

    # Create generators using shared RequestGenerator
    generators = []
    controllers = []

    for t in tenants:
        gen = RequestGenerator(
            fairness_id=t.fairness_id,
            endpoint=t.endpoint,
            priority=t.priority,
            base_concurrency=t.base_concurrency,
            metrics=metrics,
            session=session,
            traffic_pattern="concurrent",
            model_name=MODEL_NAME,
            input_tokens=INPUT_TOKENS,
            output_tokens=OUTPUT_TOKENS
        )
        controller = DynamicConcurrencyController(gen, t.phase_configs)
        generators.append(gen)
        controllers.append(controller)

    # Start test
    start_time = time.time()
    is_first_render = True
    gen_tasks = []

    try:
        # Start all generators
        gen_tasks = [asyncio.create_task(g.run()) for g in generators]

        # Main loop
        while time.time() - start_time < duration:
            elapsed = time.time() - start_time

            # Update target concurrency for each generator
            for gen, controller in zip(generators, controllers):
                target = controller.get_target_concurrency()
                gen.base_concurrency = target

            render_dashboard(elapsed, duration, tenants, metrics, is_first_render)
            is_first_render = False
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
    finally:
        # Stop generators
        for g in generators:
            g.stop()

        # Cancel tasks
        for task in gen_tasks:
            task.cancel()

        # Wait for cleanup
        await asyncio.gather(*gen_tasks, return_exceptions=True)

        # Close session
        await session.close()

    # Print final summary
    print("\n\n\033[1;36mFinal Results:\033[0m")
    print("=" * 80)

    for tenant in sorted(tenants, key=lambda t: t.priority, reverse=True):
        stats = metrics.get_stats(tenant.fairness_id)
        print(f"\n\033[1m{tenant.fairness_id}\033[0m (Priority: {tenant.priority})")
        print(f"  Total Requests: {stats['total_requests']}")
        print(f"  Throughput: {stats.get('qps', 0):.2f} req/s")

        if stats.get('ttft_p50'):
            print(f"  TTFT P50: {stats['ttft_p50']*1000:.0f}ms")
            print(f"  TTFT P95: {stats['ttft_p95']*1000:.0f}ms")
            print(f"  TTFT P99: {stats['ttft_p99']*1000:.0f}ms")

        counts = stats.get('status_counts', {})
        print(f"  Status: 200={counts.get('200', 0)}, 429={counts.get('429', 0)}, 503={counts.get('503', 0)}")

    print("\n" + "=" * 80 + "\n")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Test 4: Priority Inversion Prevention - Batch workloads cannot starve interactive traffic",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=90,
        help="Test duration in seconds"
    )

    parser.add_argument(
        "--gateway-url",
        default=None,
        help="Override gateway URL (default: AWS ELB + /llm-test/qwen32b-a/v1/completions)"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    try:
        asyncio.run(run_test(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    main()
