# Flow Control Test Scenarios

This document outlines the automated test scenarios available in the Flow Control UI.

## Completed Tests

### Test 2: Priority Differentiation
**Duration:** 2 minutes  
**Purpose:** Validate that premium traffic maintains low latency when standard traffic saturates the system

**Phases:**
1. `0s` - Baseline: 8 premium + 8 standard (healthy)
2. `30s` - Saturate with standard: 8 premium + 32 standard
3. `60s` - Both saturated: 16 premium + 32 standard
4. `90s` - Return to baseline: 8 premium + 8 standard

**Expected Results:**
- Premium TTFT should remain low even when standard saturates
- Premium requests should be prioritized over standard in queue

---

### Test 3: Fairness Validation
**Duration:** 1.5 minutes  
**Purpose:** Verify fair resource allocation within same priority tier

**Phases:**
1. `0s` - Equal load: 16 premium + 16 standard
2. `30s` - Premium heavy: 32 premium + 8 standard
3. `60s` - Standard heavy: 8 premium + 32 standard

**Expected Results:**
- Round-robin fairness within each priority tier
- No single tenant monopolizes resources within their tier

---

## Tier 1: Gateway Fundamentals

### Test 4: TTL Expiration
**Duration:** 30 seconds  
**Purpose:** Validate that requests expire after TTL when queue is saturated

**Phases:**
1. `0s` - Overload: 32 premium requests (exceeds capacity)
2. `20s` - Cooldown: all stop

**Expected Results:**
- Requests waiting beyond TTL should return 503
- Queue should not grow unbounded

---

### Test 5: Capacity Rejection
**Duration:** 30 seconds  
**Purpose:** Verify hard capacity limits are enforced

**Phases:**
1. `0s` - Exceed limits: 60 premium + 10 batch
2. `20s` - Cooldown: all stop

**Expected Results:**
- 429 responses when maxRequests exceeded
- Global capacity limits respected

---

## Tier 2: Priority & Ordering

### Test 7: Priority Inversion Prevention
**Duration:** 1.5 minutes  
**Purpose:** Ensure high priority requests don't get blocked by low priority flood

**Phases:**
1. `0s` - Flood low priority: 50 standard requests
2. `30s` - High priority arrives: 5 premium requests
3. `60s` - Cooldown: all stop

**Expected Results:**
- Premium requests should bypass standard queue
- Premium TTFT should be low despite standard saturation

---

### Test 10: Batch Job Interference
**Duration:** 1.5 minutes  
**Purpose:** Validate interactive workloads aren't delayed by batch processing

**Phases:**
1. `0s` - Batch jobs: 20 standard (long prompts)
2. `30s` - Interactive arrives: 16 premium
3. `60s` - Cooldown: all stop

**Expected Results:**
- Interactive (premium) should not be blocked by batch
- Batch should yield to interactive workload

---

## Tier 3: Stateful Optimization

### Test 6: Prefix Cache-Aware Routing
**Duration:** 2 minutes  
**Purpose:** Verify routing considers prefix cache hits for optimal placement

**Phases:**
1. `0s` - Warm up: 4 premium + 4 standard
2. `30s` - Steady load: 16 premium + 16 standard
3. `90s` - Cooldown: 4 premium + 4 standard

**Expected Results:**
- Requests with cache hits routed to pods with matching prefixes
- Lower TTFT for cache-hit requests

---

## Plugin Configuration Tests

### Test 11: Queue vs Cache Scoring
**Duration:** 2 minutes  
**Purpose:** Compare routing decisions with different scoring strategies

**Phases:**
1. `0s` - Baseline load: 8 premium + 8 standard
2. `90s` - Cooldown

**Expected Results:**
- Observe trade-offs between queue depth and cache hit optimization

---

### Test 12: Bypass vs Strict Queuing
**Duration:** 2 minutes  
**Purpose:** Validate queueDepthThreshold=1 forces strict queuing

**Phases:**
1. `0s` - Baseline load: 8 premium + 8 standard
2. `90s` - Cooldown

**Expected Results:**
- With queueDepthThreshold=1: no request bypasses queue
- Saturation detection triggers earlier

---

## Demos

### Demo: Gradual Saturation
**Duration:** 2 minutes  
**Purpose:** Demonstrate system behavior under increasing load

**Phases:**
1. `0s` - Light load: 4+4
2. `20s` - At capacity: 8+8
3. `40s` - Moderate overload: 12+12
4. `60s` - Heavy overload: 16+16
5. `90s` - Cool down: 4+4

---

### Demo: Burst Spike
**Duration:** 1.5 minutes  
**Purpose:** Show resilience during traffic spikes

**Phases:**
1. `0s` - Steady state: 8+8
2. `20s` - 🔥 BURST! Standard spikes to 32
3. `50s` - Recovery: 8+8

---

## Metrics to Monitor

For all tests, observe:
- **P95 TTFT** - Time to first token (latency)
- **P95 TPOT** - Time per output token (throughput)
- **Status codes** - 200 (success), 429 (rate limit), 503 (timeout/saturation)
- **Queue depth** - From EPP config viewer
- **Active requests** - Per tenant concurrency

## Running Tests

1. Open UI at http://localhost:8080
2. Select test from "Auto Test" dropdown
3. Click "▶ Run Selected Test"
4. Monitor metrics in real-time
5. Test completes automatically and resets to zero load
