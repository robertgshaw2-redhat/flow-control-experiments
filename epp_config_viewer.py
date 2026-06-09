#!/usr/bin/env python3
"""
EPP Configuration Viewer & Editor

Displays current EPP Flow Control configuration and allows live editing.
Connects to Kubernetes to fetch and update LLMInferenceService scheduler config.
"""

import argparse
import json
import subprocess
import sys
from typing import Dict, Any, Optional


def run_kubectl(args: list) -> tuple[str, int]:
    """Run kubectl command and return output."""
    try:
        result = subprocess.run(
            ["kubectl"] + args,
            capture_output=True,
            text=True,
            check=False
        )
        return result.stdout, result.returncode
    except Exception as e:
        return f"Error: {e}", 1


def get_current_config(namespace: str, service_name: str) -> Optional[Dict[str, Any]]:
    """Fetch current LLMInferenceService scheduler config."""
    output, code = run_kubectl([
        "get", "llminferenceservice", service_name,
        "-n", namespace,
        "-o", "jsonpath={.spec.router.scheduler.config.inline}"
    ])

    if code != 0:
        print(f"Error fetching config: {output}", file=sys.stderr)
        return None

    try:
        return json.loads(output) if output else None
    except json.JSONDecodeError as e:
        print(f"Error parsing config JSON: {e}", file=sys.stderr)
        return None


def extract_flow_control_params(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract flow control relevant parameters."""
    if not config:
        return {}

    flow_control = config.get("flowControl", {})
    plugins = config.get("plugins", [])

    # Find utilization-detector settings
    util_detector = {}
    for plugin in plugins:
        if plugin.get("type") == "utilization-detector":
            util_detector = plugin.get("parameters", {})
            break

    return {
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
    }


def display_config(params: Dict[str, Any]):
    """Display configuration in a readable format."""
    print("\n" + "="*70)
    print("EPP FLOW CONTROL CONFIGURATION")
    print("="*70)

    fc = params.get("flowControl", {})
    print("\n📊 GLOBAL LIMITS:")
    print(f"  maxRequests:       {fc.get('maxRequests', 'N/A')}")
    print(f"  maxBytes:          {fc.get('maxBytes', 'N/A')}")
    print(f"  defaultRequestTTL: {fc.get('defaultRequestTTL', 'N/A')}")

    print("\n🎯 PRIORITY BANDS:")
    for i, band in enumerate(fc.get("priorityBands", []), 1):
        print(f"\n  Band {i}:")
        print(f"    Priority:         {band.get('priority')}")
        print(f"    maxRequests:      {band.get('maxRequests', 'N/A')}")
        print(f"    maxBytes:         {band.get('maxBytes', 'N/A')}")
        print(f"    Fairness Policy:  {band.get('fairnessPolicyRef', 'N/A')}")
        print(f"    Ordering Policy:  {band.get('orderingPolicyRef', 'N/A')}")

    ud = params.get("utilizationDetector", {})
    print("\n🔍 SATURATION DETECTOR:")
    print(f"  Plugin:                   {params.get('saturationDetector', 'N/A')}")
    print(f"  queueDepthThreshold:      {ud.get('queueDepthThreshold', 'N/A')}")
    print(f"  kvCacheUtilThreshold:     {ud.get('kvCacheUtilThreshold', 'N/A')}")
    print(f"  metricsStalenessThreshold: {ud.get('metricsStalenessThreshold', 'N/A')}")

    print("\n" + "="*70 + "\n")


def get_epp_metrics(namespace: str, service_name: str) -> Dict[str, str]:
    """Fetch current EPP metrics from Prometheus endpoint."""
    # Try to get EPP pod
    output, code = run_kubectl([
        "get", "pods",
        "-n", namespace,
        "-l", f"app={service_name}-kserve-router-scheduler",
        "-o", "jsonpath={.items[0].metadata.name}"
    ])

    if code != 0 or not output:
        return {"error": "EPP pod not found"}

    pod_name = output.strip()

    # Port-forward to metrics endpoint (9090)
    # This is complex - for now just return placeholder
    return {
        "note": "Live metrics require port-forward to pod:9090/metrics",
        "command": f"kubectl port-forward -n {namespace} {pod_name} 9090:9090"
    }


def interactive_edit(namespace: str, service_name: str, current_params: Dict[str, Any]):
    """Interactive configuration editor."""
    print("\n🔧 CONFIGURATION EDITOR")
    print("=" * 70)
    print("What would you like to change?")
    print()
    print("  1) queueDepthThreshold (current: {})".format(
        current_params.get("utilizationDetector", {}).get("queueDepthThreshold", "N/A")))
    print("  2) kvCacheUtilThreshold (current: {})".format(
        current_params.get("utilizationDetector", {}).get("kvCacheUtilThreshold", "N/A")))
    print("  3) defaultRequestTTL (current: {})".format(
        current_params.get("flowControl", {}).get("defaultRequestTTL", "N/A")))
    print("  4) Global maxRequests (current: {})".format(
        current_params.get("flowControl", {}).get("maxRequests", "N/A")))
    print("  5) Priority band limits")
    print("  6) Cancel")
    print()

    choice = input("Enter choice (1-6): ").strip()

    if choice == "6":
        print("Cancelled.")
        return

    print("\n⚠️  EDITING NOT YET IMPLEMENTED")
    print("To modify configuration, use:")
    print(f"  kubectl edit llminferenceservice {service_name} -n {namespace}")
    print("\nThen edit the .spec.router.scheduler.config.inline section")


def main():
    parser = argparse.ArgumentParser(
        description="EPP Flow Control Configuration Viewer & Editor"
    )
    parser.add_argument(
        "--namespace", "-n",
        default="llm-test",
        help="Kubernetes namespace"
    )
    parser.add_argument(
        "--service",
        default="qwen-basic",
        help="LLMInferenceService name"
    )
    parser.add_argument(
        "--edit",
        action="store_true",
        help="Enter interactive edit mode"
    )
    parser.add_argument(
        "--metrics",
        action="store_true",
        help="Show how to access live EPP metrics"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON configuration"
    )

    args = parser.parse_args()

    print(f"Fetching configuration for {args.service} in namespace {args.namespace}...")

    config = get_current_config(args.namespace, args.service)
    if not config:
        print("Failed to fetch configuration")
        sys.exit(1)

    params = extract_flow_control_params(config)

    if args.json:
        print(json.dumps(params, indent=2))
    else:
        display_config(params)

    if args.metrics:
        print("\n📈 EPP METRICS ACCESS:")
        metrics_info = get_epp_metrics(args.namespace, args.service)
        for key, value in metrics_info.items():
            print(f"  {key}: {value}")
        print()

    if args.edit:
        interactive_edit(args.namespace, args.service, params)


if __name__ == "__main__":
    main()
