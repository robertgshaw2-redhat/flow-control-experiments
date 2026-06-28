#!/usr/bin/env python3
"""
Diagnostic script for Test 2 issues.

This script tests:
1. Priority routing (premium vs standard should have different latencies)
2. Traffic pattern verification (check if spike sequence is working)
3. Error analysis (what status codes are actually being returned)
"""

import asyncio
import aiohttp
import time
import json
from collections import defaultdict
from typing import Dict, List

ENDPOINT = "http://aefc7e10f44604760a801dfb2c34b36b-64674702.us-west-2.elb.amazonaws.com/llm-test/qwen32b-a/v1/completions"

class DiagnosticResults:
    def __init__(self):
        self.premium_ttfts = []
        self.standard_ttfts = []
        self.status_codes = defaultdict(int)
        self.start_time = time.monotonic()

    def add_premium(self, ttft, status):
        self.premium_ttfts.append(ttft)
        self.status_codes[f"premium_{status}"] += 1

    def add_standard(self, ttft, status):
        self.standard_ttfts.append(ttft)
        self.status_codes[f"standard_{status}"] += 1

    def get_p95(self, values):
        if not values:
            return None
        sorted_vals = sorted(values)
        idx = int(len(sorted_vals) * 0.95)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]

    def report(self):
        print("\n" + "="*80)
        print("DIAGNOSTIC RESULTS")
        print("="*80)

        print("\nPRIORITY ROUTING TEST:")
        print(f"  Premium requests: {len(self.premium_ttfts)}")
        print(f"  Standard requests: {len(self.standard_ttfts)}")

        if self.premium_ttfts:
            premium_p50 = self.get_p95(self.premium_ttfts[:len(self.premium_ttfts)//2])
            premium_p95 = self.get_p95(self.premium_ttfts)
            print(f"  Premium P50 TTFT: {premium_p50:.3f}s")
            print(f"  Premium P95 TTFT: {premium_p95:.3f}s")

        if self.standard_ttfts:
            standard_p50 = self.get_p95(self.standard_ttfts[:len(self.standard_ttfts)//2])
            standard_p95 = self.get_p95(self.standard_ttfts)
            print(f"  Standard P50 TTFT: {standard_p50:.3f}s")
            print(f"  Standard P95 TTFT: {standard_p95:.3f}s")

        if self.premium_ttfts and self.standard_ttfts:
            premium_avg = sum(self.premium_ttfts) / len(self.premium_ttfts)
            standard_avg = sum(self.standard_ttfts) / len(self.standard_ttfts)
            ratio = standard_avg / premium_avg if premium_avg > 0 else 0
            print(f"\n  Average TTFT ratio (standard/premium): {ratio:.2f}x")

            if ratio < 1.2:
                print("  ⚠️  WARNING: Standard is NOT sufficiently slower than premium!")
                print("  ⚠️  Priority routing may not be working correctly.")
            else:
                print("  ✅  Priority differentiation detected")

        print("\nSTATUS CODE DISTRIBUTION:")
        for status, count in sorted(self.status_codes.items()):
            print(f"  {status}: {count}")

        # Check for real errors (4xx/5xx excluding 429/503)
        error_count = sum(
            count for status, count in self.status_codes.items()
            if any(x in str(status) for x in ['400', '401', '402', '404', '500', '502'])
        )
        if error_count > 0:
            print(f"\n  ⚠️  Found {error_count} real errors (4xx/5xx excluding 429/503)")

        print("\n" + "="*80)


async def send_request(session, objective_name, fairness_id, results):
    """Send a single request with specified objective."""
    payload = {
        "model": "Qwen/Qwen2.5-0.5B-Instruct",
        "prompt": "hello " * 50,
        "max_tokens": 100,
        "stream": True,
        "ignore_eos": True,
    }

    headers = {
        "x-gateway-inference-fairness-id": fairness_id,
        "x-inference-objective": objective_name,
    }

    start_time = time.monotonic()
    ttft = None
    status = None

    try:
        async with session.post(
            ENDPOINT,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=120.0),
        ) as resp:
            status = resp.status

            if resp.status == 200:
                # Read streaming response to get TTFT
                async for chunk in resp.content.iter_any():
                    if ttft is None:
                        ttft = time.monotonic() - start_time
                        # Don't need to read rest of response for this test
                        break
            else:
                # Non-200 response
                msg = await resp.text()
                print(f"Non-200 response ({resp.status}): {msg[:100]}")

    except asyncio.TimeoutError:
        status = "Timeout"
    except Exception as e:
        status = f"Error_{type(e).__name__}"
        print(f"Request exception: {e}")

    # Record result
    if ttft is not None:
        if "premium" in objective_name:
            results.add_premium(ttft, status)
        else:
            results.add_standard(ttft, status)

    return ttft, status


async def test_priority_routing():
    """Test 1: Verify premium gets better latency than standard."""
    print("\nTEST 1: Priority Routing Verification")
    print("-" * 80)

    results = DiagnosticResults()

    async with aiohttp.ClientSession() as session:
        # Send 20 premium and 20 standard requests in parallel
        tasks = []

        # Premium requests (priority 100)
        for i in range(20):
            tasks.append(send_request(
                session,
                "premium-traffic",
                f"premium-tenant-a",
                results
            ))

        # Standard requests (priority 0)
        for i in range(20):
            tasks.append(send_request(
                session,
                "standard-traffic",
                f"standard-tenant-a",
                results
            ))

        print(f"Sending {len(tasks)} total requests (20 premium, 20 standard)...")
        await asyncio.gather(*tasks)

    results.report()
    return results


async def test_traffic_pattern_logging():
    """Test 2: Check if standard_spike_sequence logging works."""
    print("\nTEST 2: Traffic Pattern Logging Test")
    print("-" * 80)
    print("This would require checking the server logs.")
    print("Expected log pattern:")
    print("  [Test 2] Standard ramping from 20 to 50...")
    print("  [Test 2] Standard spiking to 80...")
    print("  [Test 2] Standard plateauing at 110...")
    print("  [Test 2] Standard dropping back to 20...")
    print("\nTo verify: kubectl logs -n llm-test <flow-ui-server-pod> | grep 'Test 2'")


async def test_concurrent_load():
    """Test 3: Generate sustained load to observe EPP behavior."""
    print("\nTEST 3: Sustained Concurrent Load")
    print("-" * 80)

    results = DiagnosticResults()
    duration = 30  # 30 seconds

    async with aiohttp.ClientSession() as session:
        start = time.monotonic()
        tasks = []

        async def keep_sending(objective, fairness_id, target_concurrency):
            """Maintain target concurrency for the duration."""
            inflight = set()
            while time.monotonic() - start < duration:
                # Clean up done tasks
                inflight = {t for t in inflight if not t.done()}

                # Spawn new tasks to maintain concurrency
                while len(inflight) < target_concurrency:
                    task = asyncio.create_task(send_request(
                        session, objective, fairness_id, results
                    ))
                    inflight.add(task)

                await asyncio.sleep(0.1)

            # Wait for remaining tasks
            if inflight:
                await asyncio.gather(*inflight, return_exceptions=True)

        print(f"Running sustained load for {duration}s...")
        print("  Premium: 10 concurrent")
        print("  Standard: 30 concurrent (higher load)")

        await asyncio.gather(
            keep_sending("premium-traffic", "premium-tenant-a", 10),
            keep_sending("standard-traffic", "standard-tenant-a", 30),
        )

    results.report()
    return results


async def main():
    print("="*80)
    print("TEST 2 DIAGNOSTIC SUITE")
    print("="*80)
    print(f"Endpoint: {ENDPOINT}")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Test 1: Basic priority routing
    await test_priority_routing()

    # Test 2: Traffic pattern logging
    await test_traffic_pattern_logging()

    # Test 3: Sustained load
    await test_concurrent_load()

    print("\n" + "="*80)
    print("DIAGNOSTICS COMPLETE")
    print("="*80)


if __name__ == "__main__":
    asyncio.run(main())
