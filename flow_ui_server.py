#!/usr/bin/env python3
"""
Flow Control Demo - Interactive Web UI
=================================================================

A live, browser-driven front end for the load generator in ``client.py``.
Instead of running through a fixed narrative of stages, this server is driven
interactively from the browser and supports two live-switchable modes:

  * Concurrency (closed loop): hold an adjustable number of requests in flight
    per tenant -- drag the concurrency up and down.
  * QPS (open loop): issue an adjustable number of requests per second per
    tenant regardless of how many are already in flight.

Either way you watch latency move over a trailing time window.

It reuses the exact same building blocks as the CLI demo -- ``Tenant``,
``MetricsCollector`` and ``LoadGenerator`` from ``client.py`` -- so the traffic
shape (5000 ISL / 100 OSL streaming completions, FlowKey headers, single shared
aiohttp session) is identical. The only thing that changes is the *control
loop*: the per-tenant concurrency target is a mutable value driven by the UI
rather than a pre-baked Stage timeline.

Everything runs in one asyncio event loop: the aiohttp web server and the
load-generator coroutines share the loop, so there are no threads or locks. The
browser polls ``/api/stats`` a few times a second and renders a self-contained
canvas chart (no external JS, works offline).

Prerequisites:
    Python 3.9+ and aiohttp (pip install aiohttp)

Usage:
    python3 flow_ui_server.py --url http://localhost:80/v1/completions --capacity 16
    # then open http://localhost:8080
"""

import argparse
import asyncio
import json
import math
import os
import re
import sys
import time
from typing import Dict, List, Set

import aiohttp
from aiohttp import web

# Import shared traffic generator if available
try:
    sys.path.insert(0, os.path.dirname(__file__))
    from traffic_generator import RequestGenerator as SharedRequestGenerator, HEADER_FAIRNESS_ID, HEADER_INFERENCE_OBJECTIVE
    SHARED_GENERATOR_AVAILABLE = True
except ImportError:
    SHARED_GENERATOR_AVAILABLE = False
    # Define constants if import fails
    HEADER_FAIRNESS_ID = "x-gateway-inference-fairness-id"
    HEADER_INFERENCE_OBJECTIVE = "x-gateway-inference-objective"


# Adapter to make UI server's metrics compatible with shared generator
class MetricsAdapter:
    """Adapts UI server metrics to match shared generator's interface."""
    def __init__(self, ui_metrics):
        self.ui_metrics = ui_metrics

    def record_start(self, tenant_id: str):
        """Record request start."""
        self.ui_metrics.record_start(tenant_id)

    def record_end(self, tenant_id: str, status: str, ttft: float, duration: float):
        """Record completed request in UI metrics."""
        # UI metrics expects: fairness_id, status (string), ttft, duration, output_tokens
        print(f"[MetricsAdapter] Recording: tenant={tenant_id}, status={status}, ttft={ttft}, duration={duration}", flush=True)
        self.ui_metrics.record(tenant_id, status, ttft, duration, output_tokens=0)

# Reuse the load-generation engine and metrics from the CLI demo verbatim so the
# two tools drive traffic identically.
from client import LoadGenerator, MetricsCollector, Tenant

# ==============================================================================
# AUTH TOKENS
# ==============================================================================
# Load ServiceAccount tokens for authenticated priority routing
def load_auth_tokens():
    """Load auth tokens from environment file."""
    tokens = {}
    token_file = "/tmp/flow-control-tokens.env"
    if os.path.exists(token_file):
        with open(token_file) as f:
            for line in f:
                if '=' in line:
                    key, val = line.strip().split('=', 1)
                    tokens[key] = val.strip('"')
    return tokens

AUTH_TOKENS = load_auth_tokens()
PREMIUM_TOKEN = AUTH_TOKENS.get("PREMIUM_TOKEN")
STANDARD_TOKEN = AUTH_TOKENS.get("STANDARD_TOKEN")
BATCH_TOKEN = AUTH_TOKENS.get("BATCH_TOKEN")

# Default model if not specified in args
DEFAULT_MODEL = "Qwen/Qwen2.5-0.5B-Instruct"


# ==============================================================================
# 1. TENANTS
# ==============================================================================
# Same two flows as the CLI playbook: a high-priority "premium" flow and a
# best-effort "standard" flow. The UI exposes one slider per flow (driving
# either concurrency or QPS depending on the selected mode).

def build_tenants() -> List[Tenant]:
    return [
        # Priority 100 - Premium tier with 3 tenants for fairness testing
        Tenant(
            fairness_id="premium-tenant-a",
            inference_objective="llm-premium",
            priority=100,
        ),
        Tenant(
            fairness_id="premium-tenant-b",
            inference_objective="llm-premium",
            priority=100,
        ),
        Tenant(
            fairness_id="premium-tenant-c",
            inference_objective="llm-premium",
            priority=100,
        ),
        # Priority 0 - Standard tier with 2 tenants for fairness testing
        Tenant(
            fairness_id="standard-tenant-a",
            inference_objective="llm-standard",
            priority=0,
        ),
        Tenant(
            fairness_id="standard-tenant-b",
            inference_objective="llm-standard",
            priority=0,
        ),
    ]


# ==============================================================================
# 2. TRAILING-WINDOW STATS
# ==============================================================================
# client.MetricsCollector aggregates over a whole stage. For an interactive,
# never-ending session we instead want a *trailing* window so the numbers track
# whatever concurrency you've currently dialed in. The collector already stores
# (timestamp, value) tuples in ttft_window / duration_window and timestamps in
# completion_times, so we just filter those by time here.


def _percentile(sorted_vals: List[float], q: float):
    """Nearest-rank-ish percentile matching client.py's index convention."""
    if not sorted_vals:
        return None
    if q <= 0:
        return sorted_vals[0]
    idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * q))
    return sorted_vals[idx]


def _status_buckets(counts: dict):
    """Collapse a status_counts dict into (200, 429, 503, other) totals."""
    s_200 = counts.get("200", 0)
    s_429 = sum(c for k, c in counts.items() if "429" in str(k))
    s_503 = sum(c for k, c in counts.items() if "503" in str(k))
    # Count ALL errors: HTTP 4xx/5xx (excluding 429/503) AND client-side errors
    # (Timeout, Error, Cancelled, etc.)
    err_items = []
    for k, c in counts.items():
        k_str = str(k)
        # HTTP error codes (4xx/5xx excluding 429/503)
        is_http_error = (k_str[:1] in ('4', '5') and "429" not in k_str and "503" not in k_str)
        # Client-side errors (connection failures, timeouts, cancellations)
        is_client_error = any(k_str.startswith(prefix) for prefix in ["Error", "Timeout", "Cancelled"])

        if is_http_error or is_client_error:
            err_items.append((k, c))

    # Log what we're counting as errors for debugging
    if err_items:
        print(f"[DEBUG] Counting as errors: {err_items}")

    s_err = sum(c for k, c in err_items)
    return s_200, s_429, s_503, s_err


def raw_samples(metrics: MetricsCollector, fid: str, since: float, now: float) -> list:
    """Return [ts, ttft, total] for every completed request after ``since``.

    The browser keeps its own raw buffer and does the windowed aggregation
    client-side, so changing the avg window can recompute *all* plotted points
    instead of only future ones. ``ttft_window`` and ``duration_window`` are
    appended in lockstep (see MetricsCollector.record), so zipping is safe.
    """
    floor = (now - MAX_BUFFER_WINDOW) if since is None else since
    out = []
    for (ts, ttft), (_ts, dur) in zip(metrics.ttft_window[fid], metrics.duration_window[fid]):
        if ts > floor:
            out.append([round(ts, 3), ttft, dur])
    return out


def window_stats(metrics: MetricsCollector, fid: str, window_sec: float, now: float) -> dict:
    """Compute trailing-window latency/throughput stats for one tenant."""
    cutoff = now - window_sec

    ttfts = sorted(v for ts, v in metrics.ttft_window[fid] if ts >= cutoff)
    durs = sorted(v for ts, v in metrics.duration_window[fid] if ts >= cutoff)
    comps = [ts for ts in metrics.completion_times[fid] if ts >= cutoff]
    tpots = sorted(metrics.tpot_window[fid])  # TPOT is not timestamped, use all samples

    # Throughput over the window (guard against a degenerate tiny span).
    qps = 0.0
    if len(comps) > 1:
        span = max(now - comps[0], 0.1)
        qps = len(comps) / span

    s_200, s_429, s_503, s_err = _status_buckets(metrics.status_counts[fid])

    return {
        "med_ttft": _percentile(ttfts, 0.5),
        "p90_ttft": _percentile(ttfts, 0.90),
        "p95_ttft": _percentile(ttfts, 0.95),
        "max_ttft": _percentile(ttfts, 1.0),
        "med_total": _percentile(durs, 0.5),
        "p90_total": _percentile(durs, 0.90),
        "p95_total": _percentile(durs, 0.95),
        "max_total": _percentile(durs, 1.0),
        "med_tpot": _percentile(tpots, 0.5),
        "p95_tpot": _percentile(tpots, 0.95),
        "qps": qps,
        "samples": len(ttfts),
        "active": metrics.active_requests[fid],
        # Cumulative since process start (or last reset) -- useful for spotting
        # rejections/evictions as you push past capacity.
        "s_200": s_200,
        "s_429": s_429,
        "s_503": s_503,
        "s_err": s_err,
        # Debug: include raw status counts
        "_raw_status_counts": dict(metrics.status_counts[fid]),
    }


def experiment_stats(metrics: MetricsCollector, fid: str, start: float, now: float, counts0: dict) -> dict:
    """Latency/throughput/status accumulated since an experiment's start.

    Unlike :func:`window_stats` (a trailing moving average), this aggregates
    *every* sample recorded since ``start`` -- the full distribution for the
    whole experiment run -- and reports status-code counts as deltas from the
    ``counts0`` snapshot taken when the experiment began, so they reflect only
    what happened during the experiment rather than since process start.
    """
    ttfts = sorted(v for ts, v in metrics.ttft_window[fid] if ts >= start)
    durs = sorted(v for ts, v in metrics.duration_window[fid] if ts >= start)
    comps = [ts for ts in metrics.completion_times[fid] if ts >= start]

    elapsed = max(now - start, 0.1)
    qps = len(comps) / elapsed if comps else 0.0

    cur = _status_buckets(metrics.status_counts[fid])
    base = _status_buckets(counts0 or {})
    s_200, s_429, s_503, s_err = (max(0, c - b) for c, b in zip(cur, base))

    return {
        "med_ttft": _percentile(ttfts, 0.5),
        "p90_ttft": _percentile(ttfts, 0.90),
        "p95_ttft": _percentile(ttfts, 0.95),
        "max_ttft": _percentile(ttfts, 1.0),
        "med_total": _percentile(durs, 0.5),
        "p90_total": _percentile(durs, 0.90),
        "p95_total": _percentile(durs, 0.95),
        "max_total": _percentile(durs, 1.0),
        "qps": qps,
        "samples": len(ttfts),
        "s_200": s_200,
        "s_429": s_429,
        "s_503": s_503,
        "s_err": s_err,
    }


# ==============================================================================
# 3. INTERACTIVE LOAD CONTROLLER
# ==============================================================================
# One coroutine per tenant. It supports two driving modes, switchable live from
# the UI via the shared ``control`` dict:
#
#   * "concurrency" (closed loop) -- keep ``control["targets"][fid]`` requests in
#     flight. Raising the target spawns more immediately; lowering it lets the
#     surplus drain naturally (we never cancel in-flight work to shrink).
#
#   * "qps" (open loop) -- issue ``control["rates"][fid]`` requests per second
#     regardless of how many are already in flight. A token-bucket "credit"
#     accumulator paces fractional rates smoothly: each tick adds rate*dt credit
#     and every whole credit spawns one request. If the backend can't keep up,
#     in-flight work simply piles up (that's the overload signal in this mode).

TICK_SEC = 0.02  # control-loop granularity (50 Hz); also QPS pacing resolution


async def run_interactive_worker(
    gen: LoadGenerator,
    tenant: Tenant,
    control: dict,
    stop_event: asyncio.Event,
) -> None:
    fid = tenant.fairness_id
    local: Set["asyncio.Task"] = set()
    last = time.monotonic()
    credit = 0.0  # fractional requests owed in QPS mode
    while not stop_event.is_set():
        now = time.monotonic()
        dt = now - last
        last = now

        if control["mode"] == "qps":
            rate = max(0.0, float(control["rates"].get(fid, 0.0)))
            credit += rate * dt
            # Cap pending credit so a stall/idle period can't later unleash a
            # burst: allow at most ~1s of catch-up (or a single request).
            credit = min(credit, max(rate, 1.0))
            while credit >= 1.0:
                gen._spawn(tenant, local)
                credit -= 1.0
        else:  # "concurrency"
            credit = 0.0
            target = max(0, int(control["targets"].get(fid, 0)))
            # Top the in-flight pool back up to target. If target dropped below
            # the current count we simply don't spawn; completed tasks aren't
            # replaced.
            while len(local) < target:
                gen._spawn(tenant, local)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=TICK_SEC)
            break
        except asyncio.TimeoutError:
            pass


# ==============================================================================
# 3b. SCENARIO PLAYER
# ==============================================================================
# A "scenario" drives per-tenant *traffic rate* (QPS) along a curve that repeats
# every `period` seconds, so you can replay canonical multi-tenant shapes
# (phase-offset sines, sudden spikes, day/night batch) hands-free.
#
# The player does not touch the load-generation path at all: it simply writes
# into the same ``control["rates"]`` dict the QPS sliders write to, and flips the
# server into "qps" mode. The existing per-tenant workers then issue traffic at
# whatever rate the curve currently dictates. Stopping a scenario zeroes the
# rates and hands control back to the sliders.
#
# Each curve is a small JSON blob ``{"type": ..., "base": ..., "amplitude": ...}``
# evaluated at a normalized phase ``p`` in [0, 1) (the fraction through the
# period). ``eval_curve`` is mirrored verbatim in the browser so the on-screen
# preview matches exactly what the server plays.


# --- Deterministic PRNG ------------------------------------------------------
# Random-looking but fully reproducible: spike positions and jitter are derived
# from a per-curve integer ``seed`` via this hash, so (a) the browser preview
# matches server playback bit-for-bit, and (b) a saved scenario replays the
# exact same "random" arrangement every time. ``_hash01`` is mirrored verbatim
# in flow_ui.html as ``hash01``; keep the two in lockstep.
#
# All arithmetic is unsigned 32-bit. ``& 0xFFFFFFFF`` here reduces mod 2**32
# exactly as ``Math.imul(a, b) >>> 0`` does in JS, so the two implementations
# produce identical sequences.
JITTER_BUCKETS = 120  # noise is piecewise-constant over this many slots / period


def _imul(a: int, b: int) -> int:
    return ((a & 0xFFFFFFFF) * (b & 0xFFFFFFFF)) & 0xFFFFFFFF


def _hash01(seed: int, n: int) -> float:
    """Hash (seed, n) -> a deterministic float in [0, 1)."""
    x = (_imul(seed, 0x9E3779B1) + _imul(n, 0x85EBCA77)) & 0xFFFFFFFF
    x ^= x >> 16
    x = _imul(x, 0x7FEB352D)
    x ^= x >> 15
    x = _imul(x, 0x846CA68B)
    x ^= x >> 16
    return (x & 0xFFFFFFFF) / 4294967296.0


def eval_curve(curve: dict, p: float) -> float:
    """Evaluate one traffic curve at normalized phase ``p`` in [0, 1).

    A curve has a *base shape* (set by ``type``) and two optional *modifiers*
    that stack on top of any shape: ``spikes`` (superimposed random pulses) and
    ``jitter`` (a noise band). Returns a non-negative QPS value with no upper
    cap -- the curve's own ``base``/``amplitude`` (q/s) set its magnitude.
    Unknown fields are ignored and sensible defaults are applied so
    partially-specified curves still work.
    """
    ctype = curve.get("type", "constant")
    base = float(curve.get("base", 0.0) or 0.0)
    amp = float(curve.get("amplitude", 0.0) or 0.0)
    phase = float(curve.get("phase", 0.0) or 0.0)
    ph = (p + phase) % 1.0

    if ctype == "sine":
        v = base + amp * math.sin(2 * math.pi * ph)
    elif ctype == "ramp":
        # Linear trend from ``base`` up to ``base + amp`` across the period.
        v = base + amp * ph
    elif ctype == "triangle":
        # Rise to the peak at mid-period, then fall symmetrically back.
        v = base + amp * (1.0 - abs(2.0 * ph - 1.0))
    elif ctype == "square":
        # On/off, phase-shiftable: high for the first ``duty`` of the period.
        duty = float(curve.get("duty", 0.5) or 0.0)
        v = base + amp if ph < duty else base
    elif ctype == "spike":
        pos = float(curve.get("pos", 0.5) or 0.0)
        width = float(curve.get("width", 0.08) or 0.0)
        # Distance to the spike center, measured the short way around the loop so
        # a spike near the period boundary still reads as a single pulse.
        d = abs(ph - pos)
        d = min(d, 1.0 - d)
        v = base + amp if d < width / 2.0 else base
    elif ctype == "day":
        # High for the first `duty` fraction of the period, low after.
        duty = float(curve.get("duty", 0.5) or 0.0)
        v = base + amp if ph < duty else base
    elif ctype == "night":
        # Mirror of "day": low first, high for the trailing `duty` fraction.
        duty = float(curve.get("duty", 0.5) or 0.0)
        v = base + amp if ph >= (1.0 - duty) else base
    elif ctype == "pulses":
        # Explicit multi-spike timeline. ``pulses`` is a list of rectangular
        # spikes, each with its own start ``at`` and duration ``dur`` (both in
        # SECONDS within the period) and height ``amp`` (q/s, on top of ``base``).
        # Lets you script N independent spikes — ramp traffic up and back down a
        # set number of times — instead of the random ``spikes`` modifier where
        # every pulse is randomly placed and identically sized.
        per = float(curve.get("period", 0.0) or 0.0) or 60.0
        t = ph * per  # seconds elapsed into the current period
        v = base
        for pl in (curve.get("pulses") or []):
            dur = float(pl.get("dur", 0.0) or 0.0)
            if dur <= 0:
                continue
            at = float(pl.get("at", 0.0) or 0.0)
            # Seconds since this pulse's start, wrapped into the period so a pulse
            # whose tail runs past the period boundary resumes at the start. The
            # double-mod normalizes to [0, per) identically to the JS mirror,
            # whose ``%`` would otherwise return a negative remainder.
            local = ((t - at) % per + per) % per
            if local < dur:
                v += float(pl.get("amp", 0.0) or 0.0)
    else:  # "constant" and any unknown type
        v = base

    # --- Modifier: superimposed random spikes --------------------------------
    # ``spikes`` pulses per period at deterministic-random positions, each of
    # height ``spike_amp`` and fractional ``spike_width``. Lets a steady or
    # smooth flow read as a bursty "noisy neighbor".
    nspk = int(curve.get("spikes", 0) or 0)
    if nspk > 0:
        seed = int(curve.get("seed", 1) or 0)
        spk_amp = float(curve.get("spike_amp", amp or base) or 0.0)
        spk_w = float(curve.get("spike_width", 0.04) or 0.0)
        for k in range(nspk):
            pos = _hash01(seed, k)
            d = abs(ph - pos)
            d = min(d, 1.0 - d)
            if d < spk_w / 2.0:
                v += spk_amp

    # --- Modifier: jitter / noise band ---------------------------------------
    # Multiplies the value by a piecewise-constant noise factor in
    # [1-jit, 1+jit]. Piecewise-constant (per bucket) so it reads as realistic
    # second-to-second variation rather than per-tick flicker.
    jit = float(curve.get("jitter", 0.0) or 0.0)
    if jit > 0:
        seed = int(curve.get("seed", 1) or 0)
        bucket = int(ph * JITTER_BUCKETS)
        r = _hash01(seed ^ 0x5BD1E995, bucket)
        v *= 1.0 + jit * (r - 0.5) * 2.0

    return max(0.0, v)


async def run_scenario_driver(control: dict, stop_event: asyncio.Event) -> None:
    """Continuously project the active scenario's curves onto ``control["rates"]``.

    Idle (writes nothing) unless a scenario is playing, so the QPS sliders keep
    working between scenarios. A non-looping scenario zeroes traffic and stops
    once it has run for a full period.
    """
    rates: Dict[str, float] = control["rates"]
    while not stop_event.is_set():
        scn = control["scenario"]
        if scn["playing"]:
            elapsed = time.monotonic() - scn["start"]
            period = max(1.0, float(scn["period"]))
            curves = scn["curves"]

            # Skip rate updates if using shared generator (GPU Consolidation test)
            # The shared generator handles actual traffic, but we still need to populate
            # rates with the curve values for the QPS graph display
            if scn.get("using_shared_generator"):
                scn["elapsed"] = elapsed
                scn["phase"] = min(elapsed / period, 1.0)

                # Evaluate curves for display purposes only (not for load generation)
                def eff_period(curve: dict) -> float:
                    cp = float(curve.get("period") or 0.0)
                    return min(max(1.0, cp if cp > 0 else period), 3600.0)

                # Zero out all tenants first, then set rates for tenants in the scenario
                for fid in rates:
                    rates[fid] = 0.0
                for fid, curve in curves.items():
                    if fid in rates:
                        eff = eff_period(curve)
                        # Both tenants run from start; tenant-b switches endpoints at 120s
                        # Always use modulo for curves so they keep oscillating
                        p = (elapsed % eff) / eff
                        target_qps = eval_curve(curve, p)
                        rates[fid] = target_qps

                        # For Test 3-4, update generator's external_rate (Test 2 doesn't use external_rate)
                        if "test3_gen_a" in control:
                            if fid == "premium-tenant-a":
                                control["test3_gen_a"].external_rate = target_qps
                            elif fid == "premium-tenant-b":
                                control["test3_gen_b"].external_rate = target_qps
                            elif fid == "premium-tenant-c":
                                control["test3_gen_c"].external_rate = target_qps

                        if "test4_gen_standard" in control:
                            if fid == "premium-tenant-a":
                                control["test4_gen_premium"].external_rate = target_qps
                            elif fid == "standard-tenant-a":
                                control["test4_gen_standard"].external_rate = target_qps
            else:
                # Normal scenario driver logic
                # Each tenant cycles on its own period (curve["period"]), falling back
                # to the scenario default. The playhead/non-loop window spans the
                # longest period so a faster tenant repeats inside it. Mirrored in the
                # browser's effPeriod()/windowPeriod().
                def eff_period(curve: dict) -> float:
                    cp = float(curve.get("period") or 0.0)
                    return min(max(1.0, cp if cp > 0 else period), 3600.0)

                window = period
                for curve in curves.values():
                    window = max(window, eff_period(curve))

                if not scn["loop"] and elapsed >= window:
                    for fid in rates:
                        rates[fid] = 0.0
                    scn["playing"] = False
                    scn["elapsed"] = window
                    scn["phase"] = 1.0
                else:
                    scn["elapsed"] = elapsed
                    scn["phase"] = (elapsed % window) / window if scn["loop"] else min(elapsed / window, 1.0)
                    # Zero out all tenants first, then set rates for tenants in the scenario
                    for fid in rates:
                        rates[fid] = 0.0
                    for fid, curve in curves.items():
                        if fid in rates:
                            eff = eff_period(curve)
                            p = (elapsed % eff) / eff if scn["loop"] else min(elapsed / eff, 1.0)
                            rates[fid] = eval_curve(curve, p)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=TICK_SEC)
            break
        except asyncio.TimeoutError:
            pass


async def prune_loop(metrics: MetricsCollector, tenants: List[Tenant], max_window: float, control: dict, stop_event: asyncio.Event) -> None:
    """Trim timestamped sample buffers so a long-running session stays bounded.

    Status counts are left cumulative on purpose (the UI shows them as running
    totals); only the per-sample latency/throughput buffers are trimmed.

    While an experiment is running we retain everything back to its start (even
    past ``max_window``) so the cumulative experiment stats cover the whole run,
    not just the trailing moving-average window.
    """
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2.0)
            break
        except asyncio.TimeoutError:
            pass
        cutoff = time.monotonic() - max_window
        exp = control["experiment"]
        if exp["running"]:
            cutoff = min(cutoff, exp["start"])
        for t in tenants:
            fid = t.fairness_id
            metrics.ttft_window[fid] = [(ts, v) for ts, v in metrics.ttft_window[fid] if ts >= cutoff]
            metrics.duration_window[fid] = [(ts, v) for ts, v in metrics.duration_window[fid] if ts >= cutoff]
            metrics.completion_times[fid] = [ts for ts in metrics.completion_times[fid] if ts >= cutoff]


# ==============================================================================
# 4. HTTP ROUTES
# ==============================================================================

HERE = os.path.dirname(os.path.abspath(__file__))
MAX_BUFFER_WINDOW = 120.0  # seconds of latency samples retained for windowing


async def handle_index(request: web.Request) -> web.Response:
    path = os.path.join(HERE, "flow_ui.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html")
    except FileNotFoundError:
        return web.Response(status=500, text="flow_ui.html not found next to flow_ui_server.py")


async def handle_config(request: web.Request) -> web.Response:
    app = request.app

    # Try to fetch EPP metrics if available
    epp_metrics = {}
    try:
        # Extract EPP host from URL
        from urllib.parse import urlparse
        parsed = urlparse(app["args"].url)
        epp_host = parsed.netloc.split(':')[0] if parsed.netloc else None

        if epp_host:
            # Try to fetch Prometheus metrics from EPP (usually on port 9090)
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://{epp_host}:9090/metrics", timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        metrics_text = await resp.text()
                        # Parse relevant metrics
                        for line in metrics_text.split('\n'):
                            if line.startswith('#') or not line.strip():
                                continue
                            if 'inference_extension_flow_control_queue_size' in line:
                                epp_metrics['queue_size'] = line.split()[-1]
                            elif 'inference_extension_flow_control_pool_saturation' in line:
                                epp_metrics['saturation'] = line.split()[-1]
    except Exception:
        pass  # Metrics unavailable, continue without them

    return web.json_response({
        "url": app["args"].url,
        "model": app["args"].model,
        "capacity": app["args"].capacity,
        "max_concurrency": app["args"].max_concurrency,
        "max_qps": app["control"]["max_qps"],
        "mode": app["control"]["mode"],
        "tenants": [
            {"fairness_id": t.fairness_id, "objective": t.inference_objective, "priority": t.priority}
            for t in app["tenants"]
        ],
        "epp_metrics": epp_metrics,
    })


async def handle_debug_status_counts(request: web.Request) -> web.Response:
    """Debug endpoint to show raw status_counts for all tenants."""
    app = request.app
    metrics: MetricsCollector = app["metrics"]

    result = {}
    for fid, counts in metrics.status_counts.items():
        result[fid] = dict(counts)

    return web.json_response(result)


async def handle_stats(request: web.Request) -> web.Response:
    app = request.app
    metrics: MetricsCollector = app["metrics"]
    tenants: List[Tenant] = app["tenants"]
    control: dict = app["control"]
    targets: Dict[str, int] = control["targets"]
    rates: Dict[str, float] = control["rates"]
    mode: str = control["mode"]

    try:
        win = float(request.query.get("window", "10"))
    except ValueError:
        win = 10.0
    win = max(1.0, min(win, MAX_BUFFER_WINDOW))

    # Cursor for incremental raw-sample streaming. Absent on first poll, which
    # backfills the whole retained buffer so a window change can recompute history.
    try:
        since = float(request.query["since"])
    except (KeyError, ValueError):
        since = None

    now = time.monotonic()
    exp = control["experiment"]
    per_tenant = []
    total_target = 0
    total_target_qps = 0.0
    total_active = 0
    total_qps = 0.0
    for t in tenants:
        fid = t.fairness_id
        s = window_stats(metrics, fid, win, now)
        tgt = int(targets.get(fid, 0))
        tgt_qps = float(rates.get(fid, 0.0))
        total_target += tgt
        total_target_qps += tgt_qps
        total_active += s["active"]
        total_qps += s["qps"]
        # Cumulative stats for the active experiment, or the frozen final stats
        # captured when the last experiment ended; None if none has run.
        if exp["running"]:
            exp_stats = experiment_stats(metrics, fid, exp["start"], now, exp["counts0"].get(fid, {}))
        else:
            exp_stats = exp["results"].get(fid)
        # For GPU Consolidation test, track actual endpoint being used
        endpoint_name = None
        if fid == "premium-tenant-b" and "shared_generators" in app:
            for gen in app["shared_generators"]:
                if gen.fairness_id == fid:
                    # Extract endpoint name from URL (e.g., qwen32b-a or qwen32b-b)
                    if "qwen32b-a" in gen.endpoint or "qwen-a" in gen.endpoint:
                        endpoint_name = "qwen-a"
                    elif "qwen32b-b" in gen.endpoint or "qwen-b" in gen.endpoint:
                        endpoint_name = "qwen-b"
                    break

        per_tenant.append({
            "fairness_id": fid,
            "objective": t.inference_objective,
            "priority": t.priority,
            "target": tgt,
            "target_qps": tgt_qps,
            **s,
            "exp": exp_stats,
            "samples": raw_samples(metrics, fid, since, now),
            "endpoint": endpoint_name,  # Actual endpoint for GPU consolidation
        })

    capacity = app["args"].capacity
    # Saturation signal depends on the mode. In closed-loop concurrency mode the
    # target itself can exceed deployment capacity. In open-loop QPS mode the
    # backpressure shows up as in-flight requests piling past capacity.
    saturated = (total_active > capacity) if mode == "qps" else (total_target > capacity)
    scn = control["scenario"]
    return web.json_response({
        "now": now,
        "ts_ms": int(time.time() * 1000),
        "window": win,
        "mode": mode,
        "capacity": capacity,
        "total_target": total_target,
        "total_target_qps": total_target_qps,
        "total_active": total_active,
        "total_qps": total_qps,
        "saturated": saturated,
        "scenario": {
            "playing": scn["playing"],
            "name": scn["name"],
            "period": scn["period"],
            "loop": scn["loop"],
            "elapsed": scn["elapsed"],
            "phase": scn["phase"],
            "switchTime": scn.get("switchTime"),  # For GPU Consolidation test
        },
        # An "experiment" is an independent measurement session: it marks a start
        # time and accumulates every metric since then, alongside (not instead of)
        # the trailing moving-average window. It is decoupled from scenario
        # playback -- you can play/stop any number of scenarios within one
        # experiment and the cumulative numbers keep accruing.
        "experiment": {
            "running": exp["running"],
            "name": exp["name"],
            "started_ms": exp["started_ms"],
            "elapsed": (now - exp["start"]) if exp["running"] else exp["result_elapsed"],
            "has_result": bool(exp["results"]),
        },
        "tenants": per_tenant,
    })


async def handle_set_concurrency(request: web.Request) -> web.Response:
    app = request.app
    targets: Dict[str, int] = app["control"]["targets"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    fid = body.get("fairness_id")
    if fid not in targets:
        return web.json_response({"error": f"unknown fairness_id {fid!r}"}, status=400)
    try:
        target = int(body.get("target"))
    except (TypeError, ValueError):
        return web.json_response({"error": "target must be an int"}, status=400)

    target = max(0, min(target, app["args"].max_concurrency))
    targets[fid] = target
    return web.json_response({"fairness_id": fid, "target": target})


async def handle_set_qps(request: web.Request) -> web.Response:
    app = request.app
    rates: Dict[str, float] = app["control"]["rates"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    fid = body.get("fairness_id")
    if fid not in rates:
        return web.json_response({"error": f"unknown fairness_id {fid!r}"}, status=400)
    try:
        target = float(body.get("target"))
    except (TypeError, ValueError):
        return web.json_response({"error": "target must be a number"}, status=400)

    # No upper cap -- the dialed-in rate is whatever the UI sends (its slider
    # range auto-sizes to the setup client-side).
    target = max(0.0, target)
    rates[fid] = target
    return web.json_response({"fairness_id": fid, "target": target})


async def handle_set_mode(request: web.Request) -> web.Response:
    app = request.app
    control: dict = app["control"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    mode = body.get("mode")
    if mode not in ("concurrency", "qps"):
        return web.json_response({"error": "mode must be 'concurrency' or 'qps'"}, status=400)
    control["mode"] = mode
    return web.json_response({"mode": mode})


async def handle_scenario_start(request: web.Request) -> web.Response:
    """Begin playing a scenario: per-tenant QPS curves over a repeating period.

    Body: {"name": str, "period": float, "loop": bool,
           "curves": {fairness_id: {"type": ..., "base": ..., ...}}}

    Switches the server into QPS mode; the scenario driver then takes over the
    per-tenant rates until /api/scenario/stop (or, for non-looping runs, the
    period elapses).

    Special: GPU Consolidation uses the shared traffic generator module.
    """
    app = request.app
    control: dict = app["control"]
    rates: Dict[str, float] = control["rates"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    name = str(body.get("name", ""))[:120]

    # GPU Consolidation test uses shared traffic_generator.py
    if ("GPU Consolidation" in name or "Test 1" in name) and SHARED_GENERATOR_AVAILABLE:
        # Stop any existing shared generators first
        if "shared_generators" in app:
            print("[Test 1] Stopping previous generators...")
            for gen in app["shared_generators"]:
                gen.stop()
            app.pop("shared_generators", None)

        # Launch staged test using shared module
        ui_metrics = app["metrics"]
        metrics = MetricsAdapter(ui_metrics)  # Wrap UI metrics with adapter
        session: aiohttp.ClientSession = app["session"]
        model_name = getattr(app["args"], "model", None) or DEFAULT_MODEL

        # Create shared generators for both tenants
        # Tenant A always hits qwen-a
        endpoint_a = app["args"].url  # qwen32b-a

        # Tenant B starts on qwen-b, switches to qwen-a at 120s
        gateway = app["args"].url.rsplit("/", 4)[0]  # Extract base gateway URL
        endpoint_b_initial = f"{gateway}/llm-test/qwen32b-b/v1/completions"
        endpoint_b_final = app["args"].url  # qwen32b-a

        generators = []

        # Tenant A - runs for full duration with noisy sinusoidal pattern on qwen-a
        gen_a = SharedRequestGenerator(
            fairness_id="premium-tenant-a",
            endpoint=endpoint_a,
            priority=100,
            base_concurrency=8,
            metrics=metrics,
            session=session,
            traffic_pattern="noisy_sinusoidal",  # 6-14 concurrency with variance, 20s period
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            phase_offset=0.0,  # No phase offset
            auth_token=PREMIUM_TOKEN,
            inference_objective="llm-premium"
        )

        # Tenant B - starts on qwen-b, then switches to qwen-a at 120s
        gen_b = SharedRequestGenerator(
            fairness_id="premium-tenant-b",
            endpoint=endpoint_b_initial,  # Starts on qwen-b
            priority=100,
            base_concurrency=8,
            metrics=metrics,
            session=session,
            traffic_pattern="noisy_sinusoidal",  # 6-14 concurrency with variance, 20s period
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            phase_offset=0.35,  # 126° offset to prevent wave overlap
            auth_token=PREMIUM_TOKEN,
            inference_objective="llm-premium"
        )

        # Start BOTH tenants immediately:
        # - Tenant A on qwen-a
        # - Tenant B on qwen-b (will switch to qwen-a at 120s)
        app["shared_gen_task_a"] = asyncio.create_task(gen_a.run())
        app["shared_gen_task_b"] = asyncio.create_task(gen_b.run())

        async def switch_tenant_b_endpoint():
            # Wait 120s then switch tenant B from qwen-b → qwen-a
            await asyncio.sleep(120)
            gen_b.endpoint = endpoint_b_final  # Switch to qwen-a
            # Record the actual switch time for UI display (use wall-clock to match ts_ms)
            control["scenario"]["switchTime"] = time.time()
            print(f"[GPU Consolidation] Switched premium-tenant-b from qwen-b → qwen-a (consolidation)")

        async def auto_stop_test():
            # Auto-stop test after 240s (4 minutes)
            await asyncio.sleep(240)
            print(f"[GPU Consolidation] Test completed (240s), stopping...")
            control["scenario"]["playing"] = False
            for gen in app.get("shared_generators", []):
                gen.stop()

        asyncio.create_task(switch_tenant_b_endpoint())
        asyncio.create_task(auto_stop_test())
        app["shared_generators"] = [gen_a, gen_b]

        # Mark scenario as playing and set up curves for QPS graph display
        # Even though shared generators handle actual requests, we need curves
        # for the UI to populate target_qps in history for the live QPS graph
        # Set using_shared_generator flag so scenario driver doesn't update rates
        curves = {
            "premium-tenant-a": {
                "type": "sine",
                "base": 10,
                "amplitude": 4,
                "phase": 0,
                "period": 20
            },
            "premium-tenant-b": {
                "type": "sine",
                "base": 10,
                "amplitude": 4,
                "phase": 0.35,
                "period": 20
            }
        }
        # Force QPS mode for proper saturation detection with SharedRequestGenerator
        control["mode"] = "qps"
        control["scenario"].update(
            playing=True,
            name=name,
            period=240,  # 4 minutes total
            loop=False,
            start=time.monotonic(),
            elapsed=0.0,
            phase=0.0,
            curves=curves,
            using_shared_generator=True,  # Flag to skip rate updates in scenario driver
            switchTime=None,  # Will be set when switch actually happens
        )

        return web.json_response({"ok": True, "name": name, "using_shared_generator": True})

    # Test 2: Priority Differentiation - also use SharedRequestGenerator
    if ("Priority Differentiation" in name or "Test 2" in name) and SHARED_GENERATOR_AVAILABLE:
        # Stop any existing shared generators first
        if "shared_generators" in app:
            print("[Test 2] Stopping previous generators...")
            for gen in app["shared_generators"]:
                gen.stop()
            app.pop("shared_generators", None)

        ui_metrics = app["metrics"]
        metrics = MetricsAdapter(ui_metrics)
        model_name = getattr(app["args"], "model", None) or DEFAULT_MODEL
        session: aiohttp.ClientSession = app["session"]
        endpoint = app["args"].url

        # Premium tenant: noisy sinusoidal around 2 concurrent (lowered for small 7B model)
        gen_premium = SharedRequestGenerator(
            fairness_id="premium-tenant-a",
            endpoint=endpoint,
            priority=100,
            base_concurrency=2,  # 2 concurrent with noisy sinusoidal
            metrics=metrics,
            session=session,
            traffic_pattern="noisy_sinusoidal",  # Noisy sinusoidal
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            phase_offset=0.0,
            period_override=30.0,  # Smooth long waves
            inference_objective="llm-premium"  # Priority 100
        )

        # Standard tenant: noisy sinusoidal, will spike from 1 → 3 → 5 → 6 concurrent
        gen_standard = SharedRequestGenerator(
            fairness_id="standard-tenant-a",
            endpoint=endpoint,
            priority=0,
            base_concurrency=1,  # Start at 1 concurrent
            metrics=metrics,
            session=session,
            traffic_pattern="noisy_sinusoidal",  # Noisy sinusoidal
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            phase_offset=0.0,
            period_override=15.0,  # Shorter waves for more chop
            inference_objective="llm-standard"  # Priority 0
        )

        # Store generators in control dict so scenario driver can update external_rate
        control["test2_gen_premium"] = gen_premium
        control["test2_gen_standard"] = gen_standard

        app["shared_gen_task_premium"] = asyncio.create_task(gen_premium.run())
        app["shared_gen_task_standard"] = asyncio.create_task(gen_standard.run())

        async def auto_stop_test2():
            await asyncio.sleep(120)
            print(f"[Test 2] Completed (120s), stopping...")
            control["scenario"]["playing"] = False
            for gen in [gen_premium, gen_standard]:
                gen.stop()

        # Add ramping logic for standard tenant - noisy sinusoidal with base_concurrency changes
        # 1 → 3 → 5 → 6 → 1 concurrent
        async def standard_spike_sequence():
            import sys
            print("[Test 2] Spike sequence started!", file=sys.stderr, flush=True)
            await asyncio.sleep(15)  # Start baseline for 15s at 1 concurrent
            print(f"[Test 2] T+15s: Standard ramping from 1 to 3 concurrent (current: {gen_standard.base_concurrency})", file=sys.stderr, flush=True)
            gen_standard.base_concurrency = 3  # Ramp phase
            print(f"[Test 2] Set base_concurrency to 3, confirmed: {gen_standard.base_concurrency}", file=sys.stderr, flush=True)
            await asyncio.sleep(10)
            print(f"[Test 2] T+25s: Standard spiking to 5 concurrent (current: {gen_standard.base_concurrency})", file=sys.stderr, flush=True)
            gen_standard.base_concurrency = 5  # Spike phase
            print(f"[Test 2] Set base_concurrency to 5, confirmed: {gen_standard.base_concurrency}", file=sys.stderr, flush=True)
            await asyncio.sleep(10)
            print(f"[Test 2] T+35s: Standard plateauing at 6 concurrent (current: {gen_standard.base_concurrency})", file=sys.stderr, flush=True)
            gen_standard.base_concurrency = 6  # PLATEAU - lowered for small model
            print(f"[Test 2] Set base_concurrency to 6, confirmed: {gen_standard.base_concurrency}", file=sys.stderr, flush=True)
            await asyncio.sleep(60)  # Hold plateau for 60s
            print(f"[Test 2] T+95s: Standard dropping back to 1 concurrent (current: {gen_standard.base_concurrency})", file=sys.stderr, flush=True)
            gen_standard.base_concurrency = 1  # Drop back to baseline
            print(f"[Test 2] Set base_concurrency to 1, confirmed: {gen_standard.base_concurrency}", file=sys.stderr, flush=True)
            print("[Test 2] Spike sequence completed!", file=sys.stderr, flush=True)

        spike_task = asyncio.create_task(standard_spike_sequence())
        print(f"[Test 2] Created spike sequence task: {spike_task}", file=sys.stderr, flush=True)
        asyncio.create_task(auto_stop_test2())
        app["shared_generators"] = [gen_premium, gen_standard]

        curves = {
            # Premium: steady noisy sine around 3 req/s (lowered for small model)
            "premium-tenant-a": {"type": "sine", "base": 3, "amplitude": 1, "phase": 0, "period": 30, "jitter": 0.15, "seed": 100},
            # Standard: spike/plateau shape lowered for small model: 2→5→8→10→2 req/s
            "standard-tenant-a": {"type": "pulses", "base": 2, "period": 120, "pulses": [
                {"at": 0, "dur": 15, "amp": 0},      # 0-15s: baseline 2
                {"at": 15, "dur": 10, "amp": 3},     # 15-25s: ramp to 5
                {"at": 25, "dur": 10, "amp": 6},     # 25-35s: spike to 8
                {"at": 35, "dur": 60, "amp": 8},     # 35-95s: PLATEAU at 10
                {"at": 95, "dur": 25, "amp": 0}      # 95-120s: drop to 2
            ], "jitter": 0.10, "seed": 200},
        }
        # Force QPS mode
        control["mode"] = "qps"
        control["scenario"].update(
            playing=True,
            name=name,
            period=120,
            loop=False,
            start=time.monotonic(),
            elapsed=0.0,
            phase=0.0,
            curves=curves,
            using_shared_generator=True,
        )

        return web.json_response({"ok": True, "name": name, "using_shared_generator": True})

    # Test 3: Fairness Validation - three premium tenants with different patterns
    if ("Fairness Validation" in name or "Test 3" in name) and SHARED_GENERATOR_AVAILABLE:
        # Stop any existing shared generators first
        if "shared_generators" in app:
            print("[Test 3] Stopping previous generators...")
            for gen in app["shared_generators"]:
                gen.stop()
            app.pop("shared_generators", None)

        ui_metrics = app["metrics"]
        model_name = getattr(app["args"], "model", None) or DEFAULT_MODEL
        metrics = MetricsAdapter(ui_metrics)
        session: aiohttp.ClientSession = app["session"]
        endpoint = app["args"].url

        # Tenant A: LOWEST baseline (15), will spike to 50 at 90s
        gen_a = SharedRequestGenerator(
            fairness_id="premium-tenant-a",
            endpoint=endpoint,
            priority=100,
            base_concurrency=15,
            metrics=metrics,
            session=session,
            traffic_pattern="concurrent",
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            phase_offset=0.0,
            auth_token=PREMIUM_TOKEN,
            inference_objective="llm-premium"
        )

        # Tenant B: MIDDLE baseline (25), noisy sine
        gen_b = SharedRequestGenerator(
            fairness_id="premium-tenant-b",
            endpoint=endpoint,
            priority=100,
            base_concurrency=25,
            metrics=metrics,
            session=session,
            traffic_pattern="noisy_sinusoidal",
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            phase_offset=0.0,
            auth_token=PREMIUM_TOKEN,
            inference_objective="llm-premium"
        )

        # Tenant C: SLIGHTLY HIGHER than B (30), noisy sine, delayed start at 30s
        gen_c = SharedRequestGenerator(
            fairness_id="premium-tenant-c",
            endpoint=endpoint,
            priority=100,
            base_concurrency=30,
            metrics=metrics,
            session=session,
            traffic_pattern="noisy_sinusoidal",
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            auth_token=PREMIUM_TOKEN,
            phase_offset=0.5
        )

        control["test3_gen_a"] = gen_a
        control["test3_gen_b"] = gen_b
        control["test3_gen_c"] = gen_c

        # Start tenant-a and tenant-b immediately
        app["shared_gen_task_test3_a"] = asyncio.create_task(gen_a.run())
        app["shared_gen_task_test3_b"] = asyncio.create_task(gen_b.run())

        # Stage tenant-c to start at 30s
        async def start_tenant_c_delayed():
            print("[Test 3] Waiting 30s to start premium-tenant-c...")
            await asyncio.sleep(30)
            print("[Test 3] Starting premium-tenant-c now")
            app["shared_gen_task_test3_c"] = asyncio.create_task(gen_c.run())

        asyncio.create_task(start_tenant_c_delayed())

        # Tenant-A spike logic: spike from 15 to 50 at 90s
        async def tenant_a_spike():
            print("[Test 3] Waiting 90s for premium-tenant-a spike...")
            await asyncio.sleep(90)
            print("[Test 3] Spiking premium-tenant-a from 15 to 50")
            gen_a.external_rate = 50
            # Hold spike for 60s (until test end at 150s)

        asyncio.create_task(tenant_a_spike())

        async def auto_stop_test3():
            await asyncio.sleep(150)
            print(f"[Test 3] Completed (150s), stopping...")
            control["scenario"]["playing"] = False
            for gen in [gen_a, gen_b, gen_c]:
                gen.stop()

        asyncio.create_task(auto_stop_test3())
        app["shared_generators"] = [gen_a, gen_b, gen_c]

        # Update curves to match UI - tenant-a lowest, tenant-c slightly higher than b
        curves = {
            "premium-tenant-a": {"type": "pulses", "base": 15, "period": 150, "pulses": [
                {"at": 0, "dur": 90, "amp": 0},     # 0-90s: stay at baseline 15
                {"at": 90, "dur": 60, "amp": 35}    # 90-150s: spike from 15 to 50
            ]},
            "premium-tenant-b": {"type": "sine", "base": 25, "amplitude": 3, "phase": 0, "period": 20, "jitter": 0.15, "seed": 202},
            "premium-tenant-c": {"type": "pulses", "base": 0, "period": 150, "pulses": [
                {"at": 30, "dur": 120, "amp": 30}   # 30-150s: baseline 30 (with sine modulation)
            ], "amplitude": 3, "phase": 0.5, "jitter": 0.15, "seed": 303},
        }
        control["mode"] = "qps"
        control["scenario"].update(
            playing=True,
            name=name,
            period=150,
            loop=False,
            start=time.monotonic(),
            elapsed=0.0,
            phase=0.0,
            curves=curves,
            using_shared_generator=True,
        )

        return web.json_response({"ok": True, "name": name, "using_shared_generator": True})

    # Test 4: Priority Inversion Prevention - standard flood then premium arrival
    if ("Priority Inversion" in name or "Test 4" in name) and SHARED_GENERATOR_AVAILABLE:
        # Stop any existing shared generators first
        if "shared_generators" in app:
            print("[Test 4] Stopping previous generators...")
            for gen in app["shared_generators"]:
                gen.stop()
            app.pop("shared_generators", None)

        model_name = getattr(app["args"], "model", None) or DEFAULT_MODEL
        ui_metrics = app["metrics"]
        metrics = MetricsAdapter(ui_metrics)
        session: aiohttp.ClientSession = app["session"]
        endpoint = app["args"].url

        # Standard tenant: starts high (32), drops to 0 at 60s
        gen_standard = SharedRequestGenerator(
            fairness_id="standard-tenant-a",
            endpoint=endpoint,
            priority=0,
            base_concurrency=32,
            metrics=metrics,
            session=session,
            traffic_pattern="concurrent",
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            phase_offset=0.0,
            auth_token=STANDARD_TOKEN,
            inference_objective="llm-standard"
        )

        # Premium tenant: starts at 30s with 8 concurrency
        gen_premium = SharedRequestGenerator(
            fairness_id="premium-tenant-a",
            endpoint=endpoint,
            priority=100,
            base_concurrency=0,  # Starts at 0
            metrics=metrics,
            session=session,
            traffic_pattern="concurrent",
            model_name=model_name,
            input_tokens=100,
            output_tokens=100,
            phase_offset=0.0,
            auth_token=PREMIUM_TOKEN,
            inference_objective="llm-premium"
        )

        control["test4_gen_standard"] = gen_standard
        control["test4_gen_premium"] = gen_premium

        app["shared_gen_task_test4_standard"] = asyncio.create_task(gen_standard.run())
        app["shared_gen_task_test4_premium"] = asyncio.create_task(gen_premium.run())

        async def auto_stop_test4():
            await asyncio.sleep(90)
            print(f"[Test 4] Completed (90s), stopping...")
            control["scenario"]["playing"] = False
            for gen in [gen_standard, gen_premium]:
                gen.stop()

        asyncio.create_task(auto_stop_test4())
        app["shared_generators"] = [gen_standard, gen_premium]

        curves = {
            "premium-tenant-a": {"type": "pulses", "base": 0, "period": 90, "pulses": [
                {"at": 30, "dur": 30, "amp": 8},    # Starts at 30s with 8
                {"at": 60, "dur": 30, "amp": -8}    # Back to 0 at 60s
            ]},
            "standard-tenant-a": {"type": "pulses", "base": 32, "period": 90, "pulses": [
                {"at": 60, "dur": 30, "amp": -32}   # Drops to 0 at 60s
            ]},
        }
        control["mode"] = "qps"
        control["scenario"].update(
            playing=True,
            name=name,
            period=90,
            loop=False,
            start=time.monotonic(),
            elapsed=0.0,
            phase=0.0,
            curves=curves,
            using_shared_generator=True,
        )

        return web.json_response({"ok": True, "name": name, "using_shared_generator": True})

    curves_in = body.get("curves") or {}
    if not isinstance(curves_in, dict):
        return web.json_response({"error": "curves must be an object"}, status=400)
    curves = {fid: c for fid, c in curves_in.items() if fid in rates and isinstance(c, dict)}
    if not curves:
        return web.json_response({"error": "no curves for known tenants"}, status=400)

    try:
        period = float(body.get("period", 60.0))
    except (TypeError, ValueError):
        period = 60.0
    period = max(1.0, min(period, 3600.0))
    loop = bool(body.get("loop", True))
    name = str(body.get("name", ""))[:120]

    control["mode"] = "qps"
    control["scenario"].update(
        playing=True,
        name=name,
        period=period,
        loop=loop,
        start=time.monotonic(),
        elapsed=0.0,
        phase=0.0,
        curves=curves,
    )
    return web.json_response({"ok": True, "name": name, "period": period, "loop": loop})


async def handle_scenario_stop(request: web.Request) -> web.Response:
    """Stop the active scenario and zero out all tenant traffic rates."""
    app = request.app
    control: dict = app["control"]
    control["scenario"]["playing"] = False
    for fid in control["rates"]:
        control["rates"][fid] = 0.0

    # Stop ALL shared generators regardless of test type
    # 1. Stop generators stored in app["shared_generators"] list
    if "shared_generators" in app:
        for gen in app["shared_generators"]:
            if hasattr(gen, 'stop'):
                gen.stop()
        app.pop("shared_generators", None)

    # 2. Stop generators stored in control dict (any key ending with _gen_)
    gen_keys = [k for k in control.keys() if '_gen_' in k]
    for key in gen_keys:
        gen = control[key]
        if hasattr(gen, 'stop'):
            gen.stop()
        control.pop(key, None)

    # 3. Cancel ALL task keys that look like generator tasks
    task_keys = [k for k in app.keys() if 'gen_task' in k or 'shared_gen' in k]
    for key in task_keys:
        task = app[key]
        if hasattr(task, 'cancel'):
            task.cancel()
        app.pop(key, None)

    return web.json_response({"ok": True})


async def handle_experiment_start(request: web.Request) -> web.Response:
    """Begin an experiment: mark a start time and accumulate all metrics from it.

    Body: {"name": str}. Independent of scenario playback and of the
    play/stop controls -- it only snapshots the current status counters (so
    later counts can be reported as deltas) and records the start time. The
    stats endpoint then reports cumulative-since-start figures per tenant.
    """
    app = request.app
    metrics: MetricsCollector = app["metrics"]
    control: dict = app["control"]
    rates: Dict[str, float] = control["rates"]
    try:
        body = await request.json()
    except Exception:
        body = {}
    name = str(body.get("name", "") or "")[:120]

    control["experiment"].update(
        running=True,
        name=name,
        start=time.monotonic(),
        started_ms=int(time.time() * 1000),
        # Snapshot per-tenant status counts so the experiment reports deltas.
        counts0={fid: dict(metrics.status_counts[fid]) for fid in rates},
        results={},
        result_elapsed=0.0,
    )
    return web.json_response({"ok": True, "name": name})


async def handle_experiment_stop(request: web.Request) -> web.Response:
    """End the active experiment, freezing its final cumulative stats.

    The per-tenant totals are computed once here and stored so the UI keeps
    showing the final experiment result after it stops (the trailing sample
    buffers get pruned shortly after, so we can't recompute it later).
    """
    app = request.app
    metrics: MetricsCollector = app["metrics"]
    control: dict = app["control"]
    exp = control["experiment"]
    if exp["running"]:
        now = time.monotonic()
        exp["results"] = {
            fid: experiment_stats(metrics, fid, exp["start"], now, exp["counts0"].get(fid, {}))
            for fid in control["rates"]
        }
        exp["result_elapsed"] = now - exp["start"]
        exp["running"] = False
    return web.json_response({"ok": True})


async def handle_reset(request: web.Request) -> web.Response:
    """Clear cumulative counters and latency buffers without touching targets."""
    app = request.app
    metrics: MetricsCollector = app["metrics"]
    metrics.reset()
    # A reset wipes the buffers the experiment accumulates from, so any frozen
    # experiment result is no longer meaningful -- clear it too.
    exp = app["control"]["experiment"]
    exp["running"] = False
    exp["results"] = {}
    exp["result_elapsed"] = 0.0
    return web.json_response({"ok": True})


# ==============================================================================
# 4b. SAVED SCENARIOS (on-disk JSON "trace" files)
# ==============================================================================
# Scenarios are persisted one-per-file under ``scenarios/`` next to this script
# so they can be version-controlled, shared, and replayed exactly. Each file is
# the same JSON shape the scenario player and builder use:
#   {"name": str, "period": float, "loop": bool,
#    "curves": {fairness_id: {"type": ..., "base": ..., ...}}}
# QPS values are stored absolute (not as fractions of --max-qps) so a saved
# scenario plays back identically regardless of the server's slider bounds.

SCN_DIR = os.path.join(HERE, "scenarios")


def _slug(name: str) -> str:
    """Filesystem-safe slug for a scenario name (also the file stem)."""
    s = re.sub(r"[^a-z0-9]+", "-", str(name).strip().lower()).strip("-")
    return (s or "scenario")[:64]


def _scn_path(slug: str) -> str:
    """Resolve a slug to a path inside SCN_DIR, refusing traversal."""
    safe = _slug(slug)
    path = os.path.abspath(os.path.join(SCN_DIR, safe + ".json"))
    if os.path.dirname(path) != os.path.abspath(SCN_DIR):
        raise ValueError("invalid scenario name")
    return path


def _load_scn_files() -> List[dict]:
    """Read every saved scenario, newest first. Skips unreadable/invalid files."""
    out: List[dict] = []
    try:
        names = os.listdir(SCN_DIR)
    except FileNotFoundError:
        return out
    for fn in names:
        if not fn.endswith(".json"):
            continue
        path = os.path.join(SCN_DIR, fn)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or not isinstance(data.get("curves"), dict):
                continue
            data["slug"] = fn[:-5]
            try:
                data["mtime"] = os.path.getmtime(path)
            except OSError:
                data["mtime"] = 0.0
            out.append(data)
        except (OSError, ValueError):
            continue
    out.sort(key=lambda d: d.get("mtime", 0.0), reverse=True)
    return out


async def handle_scenarios_list(request: web.Request) -> web.Response:
    """List saved scenarios (full definitions, so the UI can load without a 2nd call)."""
    return web.json_response({"scenarios": _load_scn_files()})


async def handle_scenarios_save(request: web.Request) -> web.Response:
    """Persist a scenario to ``scenarios/<slug>.json``.

    Body matches the player/builder shape: {name, period, loop, curves}. The
    name's slug is the filename, so saving under an existing name overwrites it.
    """
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    name = str(body.get("name", "")).strip()
    if not name:
        return web.json_response({"error": "name is required"}, status=400)
    curves = body.get("curves")
    if not isinstance(curves, dict) or not curves:
        return web.json_response({"error": "curves must be a non-empty object"}, status=400)

    try:
        period = float(body.get("period", 60.0))
    except (TypeError, ValueError):
        period = 60.0
    period = max(1.0, min(period, 3600.0))

    try:
        window = float(body.get("window", period))
    except (TypeError, ValueError):
        window = period
    window = max(10.0, min(window, 300.0))

    record = {
        "name": name,
        "period": period,
        "window": window,
        "loop": bool(body.get("loop", True)),
        "curves": curves,
    }
    try:
        os.makedirs(SCN_DIR, exist_ok=True)
        path = _scn_path(name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2)
    except (OSError, ValueError) as e:
        return web.json_response({"error": f"could not save: {e}"}, status=400)

    return web.json_response({"ok": True, "slug": _slug(name), "name": name})


async def handle_scenarios_delete(request: web.Request) -> web.Response:
    """Delete a saved scenario by slug."""
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)
    slug = body.get("slug") or body.get("name")
    if not slug:
        return web.json_response({"error": "slug is required"}, status=400)
    try:
        path = _scn_path(slug)
        os.remove(path)
    except FileNotFoundError:
        return web.json_response({"error": "not found"}, status=404)
    except (OSError, ValueError) as e:
        return web.json_response({"error": str(e)}, status=400)
    return web.json_response({"ok": True})


async def handle_epp_config(request: web.Request) -> web.Response:
    """Fetch current EPP Flow Control configuration from Kubernetes using in-cluster API."""
    import json as json_module
    import os

    try:
        # Extract namespace and service name from URL
        url = request.app["args"].url
        parts = url.split('/')
        namespace = "llm-test"  # default
        service_name = "qwen-basic"  # default

        # Try to extract from URL path
        for i, part in enumerate(parts):
            if part and i > 0 and '.' not in part and ':' not in part:
                if i + 1 < len(parts) and parts[i+1] and '.' not in parts[i+1]:
                    namespace = part
                    service_name = parts[i+1]
                    break

        # Use Kubernetes service account to call API
        token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ca_cert_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

        if not os.path.exists(token_path):
            return web.json_response({
                "error": "Not running in Kubernetes pod (no service account token found)",
                "note": "Config display requires kubectl or Kubernetes API access"
            }, status=503)

        with open(token_path, 'r') as f:
            token = f.read()

        # Call Kubernetes API
        k8s_host = os.environ.get('KUBERNETES_SERVICE_HOST', 'kubernetes.default.svc')
        k8s_port = os.environ.get('KUBERNETES_SERVICE_PORT', '443')
        api_url = f"https://{k8s_host}:{k8s_port}/apis/serving.kserve.io/v1alpha2/namespaces/{namespace}/llminferenceservices/{service_name}"

        import ssl
        ssl_context = ssl.create_default_context(cafile=ca_cert_path)

        import aiohttp
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            async with session.get(api_url, headers=headers, ssl=ssl_context, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return web.json_response({
                        "error": f"K8s API returned {resp.status}",
                        "details": await resp.text()
                    }, status=500)

                svc = await resp.json()
                config = svc.get("spec", {}).get("router", {}).get("scheduler", {}).get("config", {}).get("inline", {})

        # Extract flow control relevant parameters
        flow_control = config.get("flowControl", {})
        plugins = config.get("plugins", [])

        # Find utilization-detector settings
        util_detector = {}
        for plugin in plugins:
            if plugin.get("type") == "utilization-detector":
                util_detector = plugin.get("parameters", {})
                break

        return web.json_response({
            "namespace": namespace,
            "service": service_name,
            "flowControl": {
                "maxRequests": flow_control.get("maxRequests"),
                "maxBytes": flow_control.get("maxBytes"),
                "defaultRequestTTL": flow_control.get("defaultRequestTTL"),
                "priorityBands": flow_control.get("priorityBands", []),
            },
            "utilizationDetector": {
                "queueDepthThreshold": util_detector.get("queueDepthThreshold"),
                "kvCacheUtilThreshold": util_detector.get("kvCacheUtilThreshold"),
                "metricsStalenessThreshold": util_detector.get("metricsStalenessThreshold"),
            },
            "saturationDetector": config.get("saturationDetector", {}).get("pluginRef"),
        })

    except Exception as e:
        import traceback
        return web.json_response({
            "error": str(e),
            "traceback": traceback.format_exc()
        }, status=500)


async def handle_apply_preset(request: web.Request) -> web.Response:
    """Apply a configuration preset to the LLMInferenceService.

    NOTE: This requires kubectl to be available. When running in a pod without kubectl,
    the user should apply presets manually via kubectl on their local machine.
    """
    import subprocess
    import shutil
    import json
    import tempfile
    import os

    try:
        data = await request.json()
        preset = data.get("preset")

        # Check if kubectl is available
        if not shutil.which("kubectl"):
            return web.json_response({
                "error": "kubectl not available in this environment",
                "message": "Config presets require kubectl. Apply manually via: kubectl patch llminferenceservice qwen-basic -n llm-test ...",
                "preset_requested": preset
            }, status=503)

        # Define presets
        PRESETS = {
            "tier1": {
                "name": "Tier 1: Defaults",
                "queueDepthThreshold": None,  # Remove parameter
                "kvCacheUtilThreshold": None,
                "metricsStalenessThreshold": None,
            },
            "tier3": {
                "name": "Tier 3: Strict Queuing",
                "queueDepthThreshold": 1,
                "kvCacheUtilThreshold": 0.8,
                "metricsStalenessThreshold": None,
            },
            "test9": {
                "name": "Test 9: Fast Failover",
                "queueDepthThreshold": 1,
                "kvCacheUtilThreshold": 0.8,
                "metricsStalenessThreshold": "150ms",
            },
        }

        if preset not in PRESETS:
            return web.json_response({"error": "Unknown preset"}, status=400)

        config = PRESETS[preset]

        # Build kubectl patch JSON
        params = {}
        if config["queueDepthThreshold"] is not None:
            params["queueDepthThreshold"] = config["queueDepthThreshold"]
        if config["kvCacheUtilThreshold"] is not None:
            params["kvCacheUtilThreshold"] = config["kvCacheUtilThreshold"]
        if config["metricsStalenessThreshold"] is not None:
            params["metricsStalenessThreshold"] = config["metricsStalenessThreshold"]

        # If tier1 (defaults), remove all parameters
        if preset == "tier1":
            params = {}

        patch = {
            "spec": {
                "router": {
                    "scheduler": {
                        "config": {
                            "inline": {
                                "plugins": [
                                    {"type": "queue-scorer"},
                                    {"type": "prefix-cache-scorer"},
                                    {"type": "max-score-picker"},
                                    {"type": "round-robin-fairness-policy"},
                                    {"type": "fcfs-ordering-policy"},
                                    {"type": "utilization-detector", "parameters": params} if params else {"type": "utilization-detector"},
                                ]
                            }
                        }
                    }
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(patch, f)
            patch_file = f.name

        try:
            result = subprocess.run(
                ["kubectl", "patch", "llminferenceservice", "qwen-basic",
                 "-n", "llm-test", "--type=merge", f"--patch-file={patch_file}"],
                capture_output=True,
                text=True,
                timeout=10
            )

            os.unlink(patch_file)

            if result.returncode != 0:
                return web.json_response({
                    "error": "kubectl patch failed",
                    "details": result.stderr
                }, status=500)

            return web.json_response({
                "success": True,
                "preset": config["name"],
                "message": f"Applied {config['name']}. EPP scheduler will restart."
            })
        finally:
            if os.path.exists(patch_file):
                os.unlink(patch_file)

    except Exception as e:
        import traceback
        return web.json_response({
            "error": str(e),
            "traceback": traceback.format_exc()
        }, status=500)


# ==============================================================================
# 5. LIFECYCLE
# ==============================================================================

async def on_startup(app: web.Application) -> None:
    args = app["args"]
    metrics = MetricsCollector()
    tenants = build_tenants()
    # Shared, mutable control surface driven by the UI. Both the per-tenant
    # concurrency targets and QPS rates live here so switching modes preserves
    # the other mode's dialed-in values; `mode` selects which one is active.
    control: dict = {
        "mode": "concurrency",
        # Nominal QPS scale (from --max-qps). No longer a cap: it only seeds the
        # client's default curve shapes and the floor of the auto-sizing slider
        # range. Nothing clamps rates to it.
        "max_qps": float(args.max_qps),
        "targets": {t.fairness_id: 0 for t in tenants},
        "rates": {t.fairness_id: 0.0 for t in tenants},
        # Active scenario state (see run_scenario_driver). `curves` maps
        # fairness_id -> curve dict; empty/`playing: False` means idle.
        "scenario": {
            "playing": False,
            "name": "",
            "period": 60.0,
            "loop": True,
            "start": 0.0,
            "elapsed": 0.0,
            "phase": 0.0,
            "curves": {},
        },
        # Active/last experiment (see handle_experiment_*). `counts0` snapshots
        # per-tenant status counts at start so they can be reported as deltas;
        # `results` holds the frozen final stats once stopped.
        "experiment": {
            "running": False,
            "name": "",
            "start": 0.0,
            "started_ms": 0,
            "counts0": {},
            "results": {},
            "result_elapsed": 0.0,
        },
    }

    # Use curl user-agent to bypass ext_proc rejection of Python/aiohttp
    default_headers = {"User-Agent": "curl/8.4.0"}

    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    session = aiohttp.ClientSession(connector=connector, headers=default_headers)
    generator = LoadGenerator(args, metrics, args.model, session)

    # Best-effort connectivity probe -- warn but keep serving so the UI can come
    # up before the backend is ready.
    try:
        async with session.post(
            args.url,
            json={"model": args.model, "prompt": ""},
            timeout=aiohttp.ClientTimeout(total=2.0),
        ):
            pass
        print(f"[ok] reached {args.url}")
    except Exception as e:  # noqa: BLE001 -- informational only
        print(f"[warn] could not reach {args.url} yet ({type(e).__name__}); the UI will still start.")

    stop_event = asyncio.Event()
    workers = [
        asyncio.create_task(run_interactive_worker(generator, t, control, stop_event))
        for t in tenants
    ]
    pruner = asyncio.create_task(prune_loop(metrics, tenants, MAX_BUFFER_WINDOW, control, stop_event))
    scenario_driver = asyncio.create_task(
        run_scenario_driver(control, stop_event)
    )

    app["metrics"] = metrics
    app["tenants"] = tenants
    app["control"] = control
    app["session"] = session
    app["generator"] = generator
    app["stop_event"] = stop_event
    app["workers"] = workers
    app["pruner"] = pruner
    app["scenario_driver"] = scenario_driver


async def on_cleanup(app: web.Application) -> None:
    stop_event: asyncio.Event = app["stop_event"]
    generator: LoadGenerator = app["generator"]
    session: aiohttp.ClientSession = app["session"]

    stop_event.set()
    for w in app["workers"]:
        w.cancel()
    app["pruner"].cancel()
    app["scenario_driver"].cancel()
    for task in list(generator.inflight):
        task.cancel()
    await asyncio.gather(
        *app["workers"], app["pruner"], app["scenario_driver"], *generator.inflight,
        return_exceptions=True,
    )
    await session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flow Control Demo - Interactive Web UI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # For RHAII/gateway-based routing, use URL to override with full path
    if os.environ.get('URL'):
        default_url = os.environ.get('URL')
    elif os.environ.get('GATEWAY_URL'):
        default_url = os.environ.get('GATEWAY_URL')
    else:
        default_url = f"http://{os.environ.get('EPP_IP', 'localhost')}:80/v1/completions"
    parser.add_argument("--url", default=default_url, help="Target gateway completions endpoint.")
    parser.add_argument("--capacity", type=int, default=16, help="Deployment concurrency capacity (for the saturation banner).")
    parser.add_argument("--model", default=os.environ.get("MODEL_NAME", "default"), help="Model / InferenceObjective name sent in the payload.")
    parser.add_argument("--host", default="0.0.0.0", help="Web UI bind host.")
    parser.add_argument("--port", type=int, default=8080, help="Web UI port.")
    parser.add_argument("--max-concurrency", dest="max_concurrency", type=int, default=32, help="Upper bound for the concurrency sliders.")
    parser.add_argument("--max-qps", dest="max_qps", type=float, default=2.0, help="Nominal QPS scale: seeds default curve shapes and the QPS sliders' starting range (not a cap).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = web.Application()
    app["args"] = args
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/config", handle_config)
    app.router.add_get("/api/stats", handle_stats)
    app.router.add_get("/api/epp-config", handle_epp_config)
    app.router.add_post("/api/concurrency", handle_set_concurrency)
    app.router.add_post("/api/qps", handle_set_qps)
    app.router.add_post("/api/mode", handle_set_mode)
    app.router.add_post("/api/scenario/start", handle_scenario_start)
    app.router.add_post("/api/scenario/stop", handle_scenario_stop)
    app.router.add_post("/api/experiment/start", handle_experiment_start)
    app.router.add_post("/api/experiment/stop", handle_experiment_stop)
    app.router.add_get("/api/scenarios", handle_scenarios_list)
    app.router.add_post("/api/scenarios/save", handle_scenarios_save)
    app.router.add_post("/api/scenarios/delete", handle_scenarios_delete)
    app.router.add_post("/api/reset", handle_reset)
    app.router.add_post("/api/apply-preset", handle_apply_preset)
    app.router.add_get("/api/debug/status-counts", handle_debug_status_counts)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    print(f"Flow Control UI -> http://{args.host}:{args.port}  (target {args.url}, capacity {args.capacity})")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
