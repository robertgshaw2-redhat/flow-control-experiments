#!/usr/bin/env python3
"""
GPU Consolidation Test Runner
==============================================================================

This script drives the 2-phase GPU consolidation demo:
  Phase 1: Separate Endpoints (2 GPUs) - Baseline metrics
  Phase 2: Consolidated Endpoint (1 GPU) - With flow control

Usage:
    # Phase 1: Test separate endpoints
    python3 run_test.py --phase 1 --duration 180

    # Phase 2: Test consolidated endpoint
    python3 run_test.py --phase 2 --duration 180

    # Quick test (30 seconds)
    python3 run_test.py --phase 1 --duration 30

The script generates closed-loop traffic with:
  - Premium tier: 8 concurrent requests (priority 100)
  - Standard tier: 8 concurrent requests (priority 0)

Metrics tracked:
  - P50, P95, P99 TTFT (Time to First Token)
  - Request status codes (200, 429, 503, errors)
  - Throughput (requests/sec)
  - Active concurrency
"""

import argparse
import asyncio
import collections
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

# Endpoint - using qwen32b-a for both tenants
ENDPOINT = f"{DEFAULT_GATEWAY}/llm-test/qwen32b-a/v1/completions"

# Model name
MODEL_NAME = "Qwen/Qwen2.5-0.5B-Instruct"

# Request configuration
INPUT_TOKENS = 100
OUTPUT_TOKENS = 100
REQUEST_TIMEOUT = 60.0  # seconds


# ==============================================================================
# DATA MODELS
# ==============================================================================

@dataclass
class TenantConfig:
    """Configuration for a single tenant/tier."""
    fairness_id: str
    priority: int
    objective: str
    endpoint: str
    concurrency: int


# ==============================================================================
# METRICS COLLECTOR (extends shared base)
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


# RequestGenerator is imported from traffic_generator module


# ==============================================================================
# UI RENDERING
# ==============================================================================

def render_dashboard(
    phase: int,
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
    out.append(f"\033[K\033[1;36mGPU Consolidation Test - Phase {phase}\033[0m")
    out.append(f"\033[KElapsed: {int(elapsed)}s / {int(total)}s")
    out.append("\033[K" + "=" * 120)
    out.append(f"\033[K{'TENANT':<20} | {'PRI':<4} | {'CONC':<5} | {'ACTIVE':<6} | {'QPS':<6} | {'P50 TTFT':<10} | {'P95 TTFT':<10} | {'P99 TTFT':<10} | {'200s':<5} | {'429s':<5} | {'503s':<5}")
    out.append("\033[K" + "-" * 120)

    for tenant in sorted(tenants, key=lambda t: t.priority, reverse=True):
        stats = metrics.get_stats(tenant.fairness_id)

        p50_ttft = f"{stats.get('ttft_p50', 0)*1000:.0f}ms" if stats.get('ttft_p50') else "-"
        p95_ttft = f"{stats.get('ttft_p95', 0)*1000:.0f}ms" if stats.get('ttft_p95') else "-"
        p99_ttft = f"{stats.get('ttft_p99', 0)*1000:.0f}ms" if stats.get('ttft_p99') else "-"

        # Color code P95 TTFT
        p95_val = stats.get('ttft_p95', 0)
        if p95_val and p95_val < 0.5:  # < 500ms
            p95_color = "\033[1;32m"  # Green
        elif p95_val and p95_val < 1.0:  # < 1s
            p95_color = "\033[1;33m"  # Yellow
        else:
            p95_color = "\033[1;31m"  # Red
        p95_ttft_colored = f"{p95_color}{p95_ttft}\033[0m" if stats.get('ttft_p95') else "-"

        counts = stats.get('status_counts', {})

        out.append(
            f"\033[K{tenant.fairness_id:<20} | "
            f"{tenant.priority:<4} | "
            f"{tenant.concurrency:<5} | "
            f"{stats['active_requests']:<6} | "
            f"{stats.get('qps', 0):<6.1f} | "
            f"{p50_ttft:<10} | "
            f"{p95_ttft_colored:<20} | "  # Extra space for color codes
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
    """Run the GPU consolidation test."""
    phase = args.phase
    duration = args.duration
    staged = args.staged
    traffic_pattern = args.traffic_pattern

    # Override gateway URL if provided
    endpoint = ENDPOINT
    if args.gateway_url:
        gateway = args.gateway_url
        endpoint = f"{gateway}/llm-test/qwen32b-a/v1/completions"

    # Configure tenants - both same priority
    tenants = [
        TenantConfig(
            fairness_id="premium-tenant-a",
            priority=100,
            objective="P95 TTFT < 500ms",
            endpoint=endpoint,
            concurrency=8,
        ),
        TenantConfig(
            fairness_id="premium-tenant-b",
            priority=100,
            objective="P95 TTFT < 500ms",
            endpoint=endpoint,
            concurrency=8,
        ),
    ]

    print(f"\n\033[1;36m{'=' * 80}\033[0m")
    if staged:
        print(f"\033[1;36mFairness Test - Phase {phase} - STAGED - {traffic_pattern.upper()}\033[0m")
        print(f"\033[1;36m{'=' * 80}\033[0m\n")
        print(f"Stage 1 (0-2min): Tenant A only")
        print(f"Stage 2 (2-4min): Tenant A + B together")
        print(f"Stage 3 (4min+): Cool down")
        print(f"Traffic Pattern: {traffic_pattern}")
        if traffic_pattern == "sinusoidal":
            print(f"  Sinusoidal: 0 to 16 concurrent (60s period)")
        print(f"Expected: Tenant A TTFT remains consistent throughout")
    else:
        print(f"\033[1;36mFairness Test - Phase {phase} - {traffic_pattern.upper()}\033[0m")
        print(f"\033[1;36m{'=' * 80}\033[0m\n")
        print(f"Duration: {duration}s")
        print(f"Traffic Pattern: {traffic_pattern}")
    print(f"Endpoint: {endpoint}")
    print(f"\nStarting test in 3 seconds...\n")
    await asyncio.sleep(3)

    # Initialize
    metrics = TestMetricsCollector()
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)
    session = aiohttp.ClientSession(connector=connector)

    # Create generators using shared RequestGenerator
    generators = [
        RequestGenerator(
            fairness_id=t.fairness_id,
            endpoint=t.endpoint,
            priority=t.priority,
            base_concurrency=t.concurrency,
            metrics=metrics,
            session=session,
            traffic_pattern=traffic_pattern,
            model_name=MODEL_NAME,
            input_tokens=INPUT_TOKENS,
            output_tokens=OUTPUT_TOKENS
        )
        for t in tenants
    ]

    # Start test
    start_time = time.time()
    is_first_render = True
    gen_tasks = []

    try:
        if staged:
            # Stage 1: Start Tenant A only (0-120s)
            print("\n\033[1;33m>>> Stage 1: Starting Tenant A only...\033[0m\n")
            gen_tasks.append(asyncio.create_task(generators[0].run()))

            while time.time() - start_time < 120:
                elapsed = time.time() - start_time
                render_dashboard(phase, elapsed, 120, [tenants[0]], metrics, is_first_render)
                is_first_render = False
                await asyncio.sleep(1)

            # Stage 2: Start Tenant B (120-240s)
            print("\n\033[1;33m>>> Stage 2: Starting Tenant B (both running)...\033[0m\n")
            gen_tasks.append(asyncio.create_task(generators[1].run()))

            stage2_start = time.time()
            while time.time() - stage2_start < 120:
                elapsed = time.time() - start_time
                render_dashboard(phase, elapsed, 240, tenants, metrics, is_first_render)
                is_first_render = False
                await asyncio.sleep(1)

            # Stage 3: Cool down - stop both and let requests drain
            print("\n\033[1;33m>>> Stage 3: Cool down (stopping new requests)...\033[0m\n")
            for g in generators:
                g.stop()

            # Wait 10s for requests to drain
            cooldown_start = time.time()
            while time.time() - cooldown_start < 10:
                elapsed = time.time() - start_time
                render_dashboard(phase, elapsed, 250, tenants, metrics, is_first_render)
                is_first_render = False
                await asyncio.sleep(1)

        else:
            # Normal mode: start all generators
            gen_tasks = [asyncio.create_task(g.run()) for g in generators]

            while time.time() - start_time < duration:
                elapsed = time.time() - start_time
                render_dashboard(phase, elapsed, duration, tenants, metrics, is_first_render)
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
        description="GPU Consolidation Test - 2 GPUs → 1 GPU Demo",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--phase",
        type=int,
        required=True,
        choices=[1, 2],
        help="Test phase: 1 (separate endpoints, 2 GPUs) or 2 (consolidated endpoint, 1 GPU)"
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=180,
        help="Test duration in seconds"
    )

    parser.add_argument(
        "--staged",
        action="store_true",
        help="Run staged test: Tenant A for 2min, then both A+B for 2min"
    )

    parser.add_argument(
        "--traffic-pattern",
        type=str,
        default="concurrent",
        choices=["concurrent", "sinusoidal", "noisy_sinusoidal"],
        help="Traffic pattern: concurrent (constant), sinusoidal (smooth wave), or noisy_sinusoidal (production-like)"
    )

    parser.add_argument(
        "--gateway-url",
        default=None,
        help="Override gateway URL (default: AWS ELB)"
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
