# Flow Control Environment Readiness Check

**Date:** June 27, 2026  
**Cluster:** eks-ore (H100, us-west-2)  
**Status:** ✅ READY TO TEST

---

## ✅ Infrastructure Status

| Component | Status | Details |
|-----------|--------|---------|
| **Cluster** | ✅ Connected | `eks-ore` (p5.48xlarge H100 nodes) |
| **Gateway** | ✅ Running | `aefc7e10f44604760a801dfb2c34b36b-64674702.us-west-2.elb.amazonaws.com` |
| **EPP Scheduler** | ✅ Running | RHAII 3.4 TP image, flow control enabled |
| **vLLM Backend** | ✅ Running | Qwen2.5-0.5B-Instruct on H100 |
| **InferenceObjectives** | ✅ Configured | premium (100), standard (0), batch (-10) |
| **ext_proc Routing** | ✅ WORKING | EPP receiving requests |

---

## ✅ Configuration Verified

### Gateway URL
```
http://aefc7e10f44604760a801dfb2c34b36b-64674702.us-west-2.elb.amazonaws.com/llm-test/qwen32b-a/v1/completions
```

### Flow Control Config (RHAII 3.4 TP)
```yaml
saturationDetector:
  queueDepthThreshold: 2
  kvCacheUtilThreshold: 0.8

flowControl:
  maxBytes: 2147483648  # 2 GiB
  defaultRequestTTL: 30s
  priorityBands:
  - priority: 100   # premium-traffic
  - priority: 0     # standard-traffic
  - priority: -10   # batch-traffic
```

---

## 📊 Queue Depth Threshold Recommendation

**Based on GuideLLM testing and EPP behavior:**

### Current Setting: `queueDepthThreshold: 2`

**✅ GOOD FOR TESTING** - Here's why:

| Metric | Your Setup | Recommendation |
|--------|-----------|----------------|
| **Model size** | Qwen 0.5B (tiny) | Low threshold (1-3) |
| **GPU** | H100 (very fast) | Low threshold (1-3) |
| **Avg tokens/request** | ~40-100 | Low threshold |
| **Request rate** | Bursty test traffic | **2 is optimal** |
| **Goal** | Test flow control | **2 forces EPP queueing** |

### Why queueDepthThreshold: 2 is Perfect

**With threshold = 2:**
1. vLLM processes 2 requests → **EPP queues the rest**
2. Priority-based decisions happen **in EPP**, not vLLM
3. You can observe priority differentiation clearly
4. Prevents vLLM queue buildup (which ignores priorities)

**If threshold was higher (e.g., 10):**
- vLLM would batch more requests
- BUT vLLM processes FCFS (no priority awareness)
- Flow control benefits would be hidden
- Harder to see priority differentiation

### GuideLLM Data Points

From your earlier testing:
- **Tiny model (0.5B)** processes requests in **0.5-1.5 seconds**
- **H100 is fast** - little queuing at low concurrency
- **Burst traffic** is where flow control shines

**Threshold = 2 means:**
- Queue forms at ~3+ concurrent requests
- EPP takes over scheduling at realistic load
- Perfect for demonstrating priority enforcement

### Production Recommendation (Different!)

For **production** with larger models/slower GPUs:

| Model Size | GPU | Recommended Threshold |
|------------|-----|----------------------|
| 0.5-7B | H100/A100 | 2-3 |
| 7-30B | H100/A100 | 3-5 |
| 30-70B | H100 | 5-10 |
| 70B+ | H100 8-way | 10-15 |

**Larger threshold** = more batching in vLLM = higher throughput  
**Smaller threshold** = more EPP control = better priority enforcement

**Your threshold of 2 is perfect for testing on small model + fast GPU!**

---

## ✅ Test Scripts Ready

| Script | Purpose | Status |
|--------|---------|--------|
| `verify-flow-control.py` | ✅ Quick smoke test | URL correct |
| `client.py` | ✅ Load generator | Ready |
| `flow_ui_server.py` | ✅ Interactive UI | Ready |
| `Justfile` | ✅ Test runner | Ready |

---

## 🚀 Quick Start Commands

### 1. Verify EPP is Working (30 seconds)
```bash
cd ~/github/flow-control-experiments
python3 verify-flow-control.py
```

**Expected:** 3 successful requests, different priorities

### 2. Run Interactive Flow Control UI
```bash
cd ~/github/flow-control-experiments
just flow-ui
```

Then open: http://localhost:8080

### 3. Run Automated Test Scenarios

**Test 2: Priority Differentiation (2 min)**
```bash
cd ~/github/flow-control-experiments/priority-differentiation-test
# Run via UI: Load Test 2, click "Start Test"
```

**Test 3: Fairness Validation (1.5 min)**
```bash
cd ~/github/flow-control-experiments/fairness-validation-test
# Run via UI: Load Test 3, click "Start Test"
```

---

## 📈 What to Monitor

### EPP Metrics (Port 9090)
```bash
kubectl port-forward -n llm-test svc/qwen32b-a-epp-service 9090:9090
curl http://localhost:9090/metrics | grep inference_objective
```

**Key metrics:**
- `inference_objective_request_total` - requests by priority
- `inference_objective_request_duration_seconds` - latency by priority
- `vllm:num_requests_waiting` - vLLM queue depth

### EPP Logs
```bash
kubectl logs -n llm-test -l app=qwen32b-a-kserve-router-scheduler -c main --follow
```

**Look for:**
- `"EPP received request"` - ext_proc working
- `"FlowRegistry"` - flow control active
- Priority-based routing decisions

---

## ⚠️ Known Limitations (RHAII 3.4 TP)

❌ **Not available:**
- `utilization-detector` as plugin (hardcoded)
- Per-band `maxRequests` / `maxBytes`
- `saturationDetector.pluginRef` field

✅ **Available:**
- Priority bands (100, 0, -10)
- Round-robin fairness
- FCFS ordering
- Global maxBytes limit
- Queue depth + KV cache saturation

---

## 🎯 Success Criteria for Tests

**Priority Differentiation (Test 2):**
- Premium P95 TTFT < 2s even under load
- Standard P95 TTFT > 5s when saturated
- Premium requests bypass standard queue

**Fairness Validation (Test 3):**
- All tenants at same priority get ~equal throughput
- No single tenant monopolizes within priority tier

**Queue Behavior:**
- vLLM queue stays at ≤2 requests
- EPP queue grows/shrinks based on priority
- Requests expire after 30s TTL if not dispatched

---

## ✅ Environment Variables (Optional)

```bash
# Set gateway URL (already correct in scripts)
export GATEWAY_URL="http://aefc7e10f44604760a801dfb2c34b36b-64674702.us-west-2.elb.amazonaws.com/llm-test/qwen32b-a/v1/completions"

# For Justfile commands
export NAMESPACE=llm-test
export GUIDE_NAME=qwen32b-a
export MODEL_NAME="Qwen/Qwen2.5-0.5B-Instruct"
```

---

## 🔍 Troubleshooting

**If requests timeout:**
```bash
# Check EPP is receiving requests
kubectl logs -n llm-test -l app=qwen32b-a-kserve-router-scheduler -c main | grep "EPP received"
```

**If no priority differentiation:**
```bash
# Check InferenceObjectives are reconciled
kubectl logs -n llm-test -l app=qwen32b-a-kserve-router-scheduler -c main | grep InferenceObjective
```

**If metrics missing:**
```bash
# Check EPP metrics endpoint
kubectl port-forward -n llm-test svc/qwen32b-a-epp-service 9090:9090
curl http://localhost:9090/metrics
```

---

## 📝 Next Steps

1. ✅ Run `verify-flow-control.py` - confirm basic functionality
2. ✅ Start `just flow-ui` - launch interactive testing UI
3. ✅ Run Test 2 - observe priority differentiation
4. ✅ Run Test 3 - verify fairness within priority tiers
5. 📊 Capture metrics - document P95 TTFT by priority
6. 📸 Screenshot UI - show queue depth + priority separation

**You're ready to test!** 🚀
