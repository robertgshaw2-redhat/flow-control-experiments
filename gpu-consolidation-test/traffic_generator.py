"""
Shared traffic generation module for GPU consolidation tests.
Provides RequestGenerator with multiple traffic patterns (concurrent, sinusoidal).
"""

import asyncio
import math
import random
import string
import time
from typing import Optional, Set

import aiohttp


class MetricsCollector:
    """Collects and aggregates metrics per tenant."""
    
    def __init__(self):
        self.ttft_samples = {}
        self.duration_samples = {}
        self.status_counts = {}
        self.active_requests = {}
        self.start_times = {}
    
    def record_start(self, tenant_id: str):
        """Record request start."""
        if tenant_id not in self.active_requests:
            self.active_requests[tenant_id] = 0
        self.active_requests[tenant_id] += 1
    
    def record_end(self, tenant_id: str, status: str, ttft: Optional[float], duration: float):
        """Record request completion."""
        if tenant_id not in self.ttft_samples:
            self.ttft_samples[tenant_id] = []
            self.duration_samples[tenant_id] = []
            self.status_counts[tenant_id] = {}
            self.start_times[tenant_id] = []
        
        self.active_requests[tenant_id] = max(0, self.active_requests.get(tenant_id, 0) - 1)
        
        if status not in self.status_counts[tenant_id]:
            self.status_counts[tenant_id][status] = 0
        self.status_counts[tenant_id][status] += 1
        
        if ttft is not None:
            self.ttft_samples[tenant_id].append(ttft)
        if duration > 0:
            self.duration_samples[tenant_id].append(duration)
        self.start_times[tenant_id].append(time.time())


class RequestGenerator:
    """Generates and manages concurrent requests for a tenant."""

    def __init__(self, fairness_id: str, endpoint: str, priority: int,
                 base_concurrency: int, metrics: MetricsCollector,
                 session: aiohttp.ClientSession, traffic_pattern: str = "concurrent",
                 model_name: str = "Qwen/Qwen2.5-0.5B-Instruct",
                 input_tokens: int = 100, output_tokens: int = 100,
                 phase_offset: float = 0.0):
        self.fairness_id = fairness_id
        self.endpoint = endpoint
        self.priority = priority
        self.base_concurrency = base_concurrency
        self.metrics = metrics
        self.session = session
        self.traffic_pattern = traffic_pattern
        self.model_name = model_name
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.phase_offset = phase_offset  # Phase offset for sine waves (0.0 to 1.0)
        self.should_run = True
        self.inflight_tasks: Set[asyncio.Task] = set()
        self.start_time = None
        self.external_rate = None  # Can be set externally to override target concurrency

    def get_target_concurrency(self) -> int:
        """Calculate target concurrency based on traffic pattern."""
        # If external rate is set, use it (allows scenario driver to control traffic)
        if self.external_rate is not None:
            return int(self.external_rate)

        if self.traffic_pattern == "concurrent":
            return self.base_concurrency
        elif self.traffic_pattern == "sinusoidal":
            if self.start_time is None:
                return self.base_concurrency

            # Sinusoidal pattern: oscillates between 6 and 14 concurrency
            # Period: 20 seconds (faster oscillation for demos)
            elapsed = time.time() - self.start_time
            period = 20.0  # seconds
            phase = (elapsed / period) * 2 * math.pi + (self.phase_offset * 2 * math.pi)

            # Center at 10, amplitude 4 → range 6-14
            center = 10
            amplitude = 4
            target = int(center + amplitude * math.sin(phase))
            return max(0, target)
        elif self.traffic_pattern == "noisy_sinusoidal":
            if self.start_time is None:
                return self.base_concurrency

            # Sinusoidal pattern with production-like variance
            # Period: 20 seconds (faster oscillation for demos)
            elapsed = time.time() - self.start_time
            period = 20.0  # seconds
            phase = (elapsed / period) * 2 * math.pi + (self.phase_offset * 2 * math.pi)

            # Base sinusoidal wave: center at 10, amplitude 4 → range 6-14
            center = 10
            amplitude = 4
            base_sine = center + amplitude * math.sin(phase)

            # Add realistic production variance:
            # 1. Gaussian noise for natural request-level variance (±15%)
            noise_amplitude = center * 0.15
            noise = random.gauss(0, noise_amplitude / 3)

            # 2. Occasional micro-spikes (simulates retry storms, cron jobs, batch operations)
            #    5% chance per check to add a small burst
            spike = random.randint(1, int(center * 0.3)) if random.random() < 0.05 else 0

            # Combine and clamp to reasonable bounds
            target = int(base_sine + noise + spike)
            return max(0, min(target, center * 2))  # Cap at 2x center (20)

        return self.base_concurrency

    async def send_request(self):
        """Send a single request and track metrics."""
        # Generate random prompt
        random_prefix = "".join(random.choices(string.ascii_letters + string.digits, k=20))
        prompt = f"{random_prefix} " + "hello " * (self.input_tokens // 2)

        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "max_tokens": self.output_tokens,
            "stream": True,
            "ignore_eos": True,
        }

        headers = {
            "x-fairness-id": self.fairness_id,
            "x-inference-priority": str(self.priority),
        }

        start_time = time.monotonic()
        ttft: Optional[float] = None
        status = "Unknown"

        self.metrics.record_start(self.fairness_id)

        try:
            async with self.session.post(
                self.endpoint,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=60.0),
            ) as resp:
                if resp.status == 200:
                    status = "200"
                    # Read streaming response
                    async for chunk in resp.content.iter_any():
                        if ttft is None:
                            ttft = time.monotonic() - start_time
                else:
                    msg = (await resp.text()).lower()
                    if resp.status == 503 or "timed out" in msg:
                        status = "503"
                    elif resp.status == 429 or "rejected" in msg:
                        status = "429"
                    else:
                        status = str(resp.status)

        except asyncio.TimeoutError:
            status = "Timeout"
        except asyncio.CancelledError:
            status = "Cancelled"
            raise
        except Exception as e:
            status = f"Error:{type(e).__name__}"

        duration = time.monotonic() - start_time
        self.metrics.record_end(self.fairness_id, status, ttft, duration)

    async def run(self):
        """Maintain target concurrency by continuously spawning requests."""
        if self.start_time is None:
            self.start_time = time.time()

        while self.should_run:
            # Clean up completed tasks
            self.inflight_tasks = {t for t in self.inflight_tasks if not t.done()}

            # Get current target concurrency
            target = self.get_target_concurrency()

            # Spawn new tasks to reach target concurrency
            while len(self.inflight_tasks) < target and self.should_run:
                task = asyncio.create_task(self.send_request())
                self.inflight_tasks.add(task)

            await asyncio.sleep(0.1)  # Check every 100ms for pattern changes

    def stop(self):
        """Stop generating new requests."""
        self.should_run = False
