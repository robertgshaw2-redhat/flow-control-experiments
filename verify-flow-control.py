#!/usr/bin/env python3
"""
Quick verification test to confirm EPP flow control is working.

This sends 3 requests with different priorities and checks:
1. Headers are being sent correctly
2. Requests complete successfully
3. EPP scheduler is active (check pod logs for flow control activity)
"""

import asyncio
import aiohttp
import time

GATEWAY = "GATEWAY_URL_REQUIRED"
ENDPOINT = f"{GATEWAY}/llm-test/qwen32b-a/v1/completions"

async def send_test_request(session, fairness_id, priority, color):
    """Send a single test request with flow control headers."""
    payload = {
        "model": "Qwen/Qwen2.5-0.5B-Instruct",
        "prompt": f"Test request from {fairness_id}: " + "hello " * 50,
        "max_tokens": 20,
        "stream": True,
    }

    headers = {
        "x-fairness-id": fairness_id,
        "x-inference-priority": str(priority),
    }

    print(f"{color}[{fairness_id}] Sending request with priority={priority}\033[0m")
    start = time.monotonic()

    try:
        async with session.post(ENDPOINT, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            status = resp.status
            if status == 200:
                # Read first token to get TTFT
                async for chunk in resp.content.iter_any():
                    ttft = time.monotonic() - start
                    print(f"{color}[{fairness_id}] ✓ Success! TTFT={ttft:.3f}s, status={status}\033[0m")
                    break
            else:
                body = await resp.text()
                print(f"{color}[{fairness_id}] ✗ Failed: status={status}, body={body[:100]}\033[0m")

    except Exception as e:
        print(f"{color}[{fairness_id}] ✗ Error: {e}\033[0m")

async def main():
    print("\n" + "="*70)
    print("EPP Flow Control Verification Test")
    print("="*70)
    print(f"\nEndpoint: {ENDPOINT}")
    print("\nSending 3 test requests with different priorities...\n")

    async with aiohttp.ClientSession() as session:
        # Send requests with different priorities
        tasks = [
            send_test_request(session, "premium-tenant-a", 100, "\033[92m"),  # Green - priority 100
            send_test_request(session, "standard-tenant-c", 0, "\033[93m"),   # Yellow - priority 0
            send_test_request(session, "batch-tenant-d", -10, "\033[94m"),    # Blue - priority -10
        ]

        await asyncio.gather(*tasks)

    print("\n" + "="*70)
    print("Verification complete!")
    print("="*70)
    print("\nNext steps to verify EPP is using flow control:")
    print("1. All 3 requests should succeed (✓ above)")
    print("2. Check EPP scheduler logs for flow control activity:")
    print("   kubectl logs -n llm-test -l serving.kserve.io/inference-service=qwen32b-a")
    print("      -c main --tail=50 | grep -E 'priority|fairness|band'")
    print("\n")

if __name__ == "__main__":
    asyncio.run(main())
