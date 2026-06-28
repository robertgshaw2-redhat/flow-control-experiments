#!/usr/bin/env python3
"""
Send sustained traffic to demonstrate flow control with different priorities.
Monitor EPP logs to see flow control in action.
"""

import asyncio
import aiohttp
import time

GATEWAY = "http://aefc7e10f44604760a801dfb2c34b36b-64674702.us-west-2.elb.amazonaws.com"
ENDPOINT = f"{GATEWAY}/llm-test/qwen32b-a/v1/completions"

async def send_sustained_traffic(session, fairness_id, priority, count, color):
    """Send multiple concurrent requests for a tenant."""
    async def single_request(idx):
        payload = {
            "model": "Qwen/Qwen2.5-0.5B-Instruct",
            "prompt": f"Request {idx} from {fairness_id}: " + "hello " * 50,
            "max_tokens": 50,
            "stream": True,
        }
        headers = {
            "x-fairness-id": fairness_id,
            "x-inference-priority": str(priority),
        }

        start = time.monotonic()
        try:
            async with session.post(ENDPOINT, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                status = resp.status
                if status == 200:
                    async for _ in resp.content.iter_any():
                        ttft = time.monotonic() - start
                        print(f"{color}[{fairness_id}] Request {idx}: ✓ TTFT={ttft:.3f}s\033[0m")
                        break
                else:
                    body = await resp.text()
                    print(f"{color}[{fairness_id}] Request {idx}: status={status}\033[0m")
        except Exception as e:
            print(f"{color}[{fairness_id}] Request {idx}: {type(e).__name__}\033[0m")

    # Send requests concurrently
    tasks = [single_request(i) for i in range(count)]
    await asyncio.gather(*tasks)

async def main():
    print("\n" + "="*70)
    print("Flow Control Sustained Traffic Test")
    print("="*70)
    print(f"\nSending sustained traffic with different priorities...")
    print("Monitor EPP logs in parallel with:")
    print("  kubectl logs -n llm-test -l serving.kserve.io/inference-service=qwen32b-a -c main -f | grep -E 'priority|fairness|band|request'\n")

    async with aiohttp.ClientSession() as session:
        # Send different volumes per priority to trigger flow control
        await asyncio.gather(
            send_sustained_traffic(session, "premium-tenant-a", 100, 10, "\033[92m"),   # 10 premium
            send_sustained_traffic(session, "standard-tenant-c", 0, 8, "\033[93m"),     # 8 standard
            send_sustained_traffic(session, "batch-tenant-d", -10, 5, "\033[94m"),      # 5 overflow
        )

    print("\n" + "="*70)
    print("Traffic complete! Check EPP logs for flow control activity.")
    print("="*70 + "\n")

if __name__ == "__main__":
    asyncio.run(main())
