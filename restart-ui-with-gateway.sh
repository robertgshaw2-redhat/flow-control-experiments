#!/bin/bash
# Restart Flow UI with correct gateway URL

# Kill existing UI server
pkill -f flow_ui_server.py

# Get current gateway IP
GATEWAY_IP=$(kubectl get gateway inference-gateway -n redhat-ods-applications -o jsonpath='{.status.addresses[0].value}')
GATEWAY_URL="http://${GATEWAY_IP}/llm-test/qwen32b-a/v1/completions"

echo "Starting Flow UI with gateway: $GATEWAY_URL"

# Start UI server with correct URL
cd ~/github/flow-control-experiments
python3 flow_ui_server.py --url "$GATEWAY_URL" --capacity 200 &

echo ""
echo "✅ Flow UI started!"
echo "📊 Open: http://localhost:8080"
echo "🔗 Backend: $GATEWAY_URL"
