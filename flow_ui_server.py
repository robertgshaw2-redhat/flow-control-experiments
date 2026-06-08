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
import time
from typing import Dict, List, Set

import aiohttp
from aiohttp import web

# Reuse the load-generation engine and metrics from the CLI demo verbatim so the
# two tools drive traffic identically.
from client import LoadGenerator, MetricsCollector, Tenant


# ==============================================================================
# 1. TENANTS
# ==============================================================================
# Same two flows as the CLI playbook: a high-priority "premium" flow and a
# best-effort "standard" flow. The UI exposes one slider per flow (driving
# either concurrency or QPS depending on the selected mode).

def build_tenants() -> List[Tenant]:
    return [
        Tenant(
            fairness_id="premium-tenant",
            inference_objective="premium-traffic",
            priority=100,
        ),
        Tenant(
            fairness_id="standard-tenant",
            inference_objective="standard-traffic",
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


def window_stats(metrics: MetricsCollector, fid: str, window_sec: float, now: float) -> dict:
    """Compute trailing-window latency/throughput stats for one tenant."""
    cutoff = now - window_sec

    ttfts = sorted(v for ts, v in metrics.ttft_window[fid] if ts >= cutoff)
    durs = sorted(v for ts, v in metrics.duration_window[fid] if ts >= cutoff)
    comps = [ts for ts in metrics.completion_times[fid] if ts >= cutoff]

    # Throughput over the window (guard against a degenerate tiny span).
    qps = 0.0
    if len(comps) > 1:
        span = max(now - comps[0], 0.1)
        qps = len(comps) / span

    counts = metrics.status_counts[fid]
    s_200 = counts.get("200", 0)
    s_429 = sum(c for k, c in counts.items() if "429" in str(k))
    s_503 = sum(c for k, c in counts.items() if "503" in str(k))
    s_err = sum(
        c for k, c in counts.items()
        if "200" not in str(k) and "429" not in str(k) and "503" not in str(k)
    )

    return {
        "med_ttft": _percentile(ttfts, 0.5),
        "p95_ttft": _percentile(ttfts, 0.95),
        "max_ttft": _percentile(ttfts, 1.0),
        "med_total": _percentile(durs, 0.5),
        "p95_total": _percentile(durs, 0.95),
        "max_total": _percentile(durs, 1.0),
        "qps": qps,
        "samples": len(ttfts),
        "active": metrics.active_requests[fid],
        # Cumulative since process start (or last reset) -- useful for spotting
        # rejections/evictions as you push past capacity.
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
# every `period` seconds, so you can replay canonical multi-tenant shapes (sine
# vs cosine, overlapping sines, sudden spikes, day/night batch) hands-free.
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


def eval_curve(curve: dict, p: float, max_val: float) -> float:
    """Evaluate one traffic curve at normalized phase ``p`` in [0, 1).

    A curve has a *base shape* (set by ``type``) and two optional *modifiers*
    that stack on top of any shape: ``spikes`` (superimposed random pulses) and
    ``jitter`` (a noise band). Returns a QPS value clamped to [0, max_val].
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
    elif ctype == "cosine":
        v = base + amp * math.cos(2 * math.pi * ph)
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

    return max(0.0, min(v, max_val))


async def run_scenario_driver(control: dict, max_qps: float, stop_event: asyncio.Event) -> None:
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
            if not scn["loop"] and elapsed >= period:
                for fid in rates:
                    rates[fid] = 0.0
                scn["playing"] = False
                scn["elapsed"] = period
                scn["phase"] = 1.0
            else:
                p = (elapsed % period) / period if scn["loop"] else min(elapsed / period, 1.0)
                scn["elapsed"] = elapsed
                scn["phase"] = p
                for fid, curve in scn["curves"].items():
                    if fid in rates:
                        rates[fid] = eval_curve(curve, p, max_qps)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=TICK_SEC)
            break
        except asyncio.TimeoutError:
            pass


async def prune_loop(metrics: MetricsCollector, tenants: List[Tenant], max_window: float, stop_event: asyncio.Event) -> None:
    """Trim timestamped sample buffers so a long-running session stays bounded.

    Status counts are left cumulative on purpose (the UI shows them as running
    totals); only the per-sample latency/throughput buffers are trimmed.
    """
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2.0)
            break
        except asyncio.TimeoutError:
            pass
        cutoff = time.monotonic() - max_window
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
    return web.json_response({
        "url": app["args"].url,
        "model": app["args"].model,
        "capacity": app["args"].capacity,
        "max_concurrency": app["args"].max_concurrency,
        "max_qps": app["args"].max_qps,
        "mode": app["control"]["mode"],
        "tenants": [
            {"fairness_id": t.fairness_id, "objective": t.inference_objective, "priority": t.priority}
            for t in app["tenants"]
        ],
    })


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

    now = time.monotonic()
    per_tenant = []
    total_target = 0
    total_target_qps = 0.0
    total_active = 0
    total_qps = 0.0
    for t in tenants:
        s = window_stats(metrics, t.fairness_id, win, now)
        tgt = int(targets.get(t.fairness_id, 0))
        tgt_qps = float(rates.get(t.fairness_id, 0.0))
        total_target += tgt
        total_target_qps += tgt_qps
        total_active += s["active"]
        total_qps += s["qps"]
        per_tenant.append({
            "fairness_id": t.fairness_id,
            "objective": t.inference_objective,
            "priority": t.priority,
            "target": tgt,
            "target_qps": tgt_qps,
            **s,
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

    target = max(0.0, min(target, float(app["args"].max_qps)))
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
    """
    app = request.app
    control: dict = app["control"]
    rates: Dict[str, float] = control["rates"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

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
    return web.json_response({"ok": True})


async def handle_reset(request: web.Request) -> web.Response:
    """Clear cumulative counters and latency buffers without touching targets."""
    app = request.app
    metrics: MetricsCollector = app["metrics"]
    metrics.reset()
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

    record = {
        "name": name,
        "period": period,
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
    }

    connector = aiohttp.TCPConnector(limit=0)
    session = aiohttp.ClientSession(connector=connector)
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
    pruner = asyncio.create_task(prune_loop(metrics, tenants, MAX_BUFFER_WINDOW, stop_event))
    scenario_driver = asyncio.create_task(
        run_scenario_driver(control, float(args.max_qps), stop_event)
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
    default_url = f"http://{os.environ.get('EPP_IP', 'localhost')}:80/v1/completions"
    parser.add_argument("--url", default=default_url, help="Target gateway completions endpoint.")
    parser.add_argument("--capacity", type=int, default=16, help="Deployment concurrency capacity (for the saturation banner).")
    parser.add_argument("--model", default=os.environ.get("MODEL_NAME", "default"), help="Model / InferenceObjective name sent in the payload.")
    parser.add_argument("--host", default="0.0.0.0", help="Web UI bind host.")
    parser.add_argument("--port", type=int, default=8080, help="Web UI port.")
    parser.add_argument("--max-concurrency", dest="max_concurrency", type=int, default=32, help="Upper bound for the concurrency sliders.")
    parser.add_argument("--max-qps", dest="max_qps", type=float, default=2.0, help="Upper bound for the QPS sliders.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = web.Application()
    app["args"] = args
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/config", handle_config)
    app.router.add_get("/api/stats", handle_stats)
    app.router.add_post("/api/concurrency", handle_set_concurrency)
    app.router.add_post("/api/qps", handle_set_qps)
    app.router.add_post("/api/mode", handle_set_mode)
    app.router.add_post("/api/scenario/start", handle_scenario_start)
    app.router.add_post("/api/scenario/stop", handle_scenario_stop)
    app.router.add_get("/api/scenarios", handle_scenarios_list)
    app.router.add_post("/api/scenarios/save", handle_scenarios_save)
    app.router.add_post("/api/scenarios/delete", handle_scenarios_delete)
    app.router.add_post("/api/reset", handle_reset)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    print(f"Flow Control UI -> http://{args.host}:{args.port}  (target {args.url}, capacity {args.capacity})")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
