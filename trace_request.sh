#!/bin/bash
# Trace a single request to see where it goes

echo "=== Sending test request with premium-traffic objective ==="
curl -v -X POST \
  "http://aefc7e10f44604760a801dfb2c34b36b-64674702.us-west-2.elb.amazonaws.com/llm-test/qwen32b-a/v1/completions" \
  -H "Content-Type: application/json" \
  -H "x-gateway-inference-fairness-id: premium-tenant-a" \
  -H "x-inference-objective: premium-traffic" \
  -d '{
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "prompt": "hello",
    "max_tokens": 10,
    "stream": false
  }' 2>&1 | grep -E "HTTP/|x-|X-|< "

echo ""
echo "=== Checking EPP logs for this request ==="
sleep 2
kubectl logs -n llm-test qwen32b-a-kserve-router-scheduler-65bbcfbcd5-xf9tx --tail=10 2>&1 | grep -E "premium|objective|priority"
