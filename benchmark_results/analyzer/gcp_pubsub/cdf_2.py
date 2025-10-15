import re
import sys
import time
import os
import json
import csv
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

from google.cloud import logging_v2
from google.api_core import exceptions as google_api_exceptions
import numpy as np
import matplotlib.pyplot as plt

# --- Configuration ---
GCP_PROJECT_ID = "caribou-460422"

SERVICE_NAMES = {
    "get_requests": "pubs-ting-0-0-3-get-ests-gcp-us-ea1-6f83f8992d06",
    "destination": "pubs-ting-0-0-3-dest-tion-gcp-us-ea1-2895579b0724",
}

SCENARIOS = {
    "Large Message (0.1 rps)": {
        "start_time": datetime(2025, 10, 9, 2, 9, 41, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 9, 2, 51, 58, tzinfo=timezone.utc),
    },
    "Large Message (5 rps)": {
        "start_time": datetime(2025, 10, 9, 2, 59, 30, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 9, 3, 1, 7, tzinfo=timezone.utc),
    },
    "Small Message (0.1 rps)": {
        "start_time": datetime(2025, 10, 8, 21, 42, 14, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 8, 22, 24, 31, tzinfo=timezone.utc),
    },
    "Small Message (5 rps)": {
        "start_time": datetime(2025, 10, 9, 3, 6, 34, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 9, 3, 8, 9, tzinfo=timezone.utc),
    },
}

CACHE_DIR = "log_cache"
# -------------------

# --- Regular Expressions for Parsing ---
LOG_PATTERN = re.compile(r"TIME \((.*?)\) RUN_ID \((.*?)\) MESSAGE \((.*?)\) LOG_VERSION")
DEBUG_LOG_PATTERN = re.compile(r"TIME (.*) RUN_ID (\S+) MESSAGE\((.*)\)")
TAINT_PATTERN = re.compile(r"TAINT \((.*?)\)")
INSTANCE_PATTERN = re.compile(r"INSTANCE \((.*?)\)")


# ---------------------------------------


def fetch_logs(client: logging_v2.Client, scenario_name: str, service_key: str, service_name: str, start_time: datetime,
               end_time: datetime) -> List[dict]:
    """Fetches logs for a service, using a local cache if available."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    safe_scenario_name = scenario_name.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "")
    cache_filename = os.path.join(CACHE_DIR, f"pubsub_{safe_scenario_name}_{service_key}.json")

    if os.path.exists(cache_filename):
        print(f"Loading logs for '{service_name}' from cache: {cache_filename}")
        with open(cache_filename, 'r') as f:
            return json.load(f)

    print(f"Fetching logs for '{service_name}' from API...")
    time_start_str = start_time.isoformat()
    time_end_str = end_time.isoformat()
    filter_parts = [
        f'resource.type="cloud_run_revision"',
        f'resource.labels.service_name="{service_name}"',
        f'timestamp >= "{time_start_str}"',
        f'timestamp <= "{time_end_str}"',
        '(logName=~ "run.googleapis.com%2Frequests" OR logName =~ "run.googleapis.com%2Fvarlog%2Fsystem" OR jsonPayload.severity = "CARIBOU" OR logName =~ "run.googleapis.com%2Fstdout")'
    ]
    query = " AND ".join(filter_parts)

    max_retries, initial_delay = 5, 5
    for attempt in range(max_retries):
        try:
            entries_iterator = client.list_entries(resource_names=[f"projects/{GCP_PROJECT_ID}"], filter_=query,
                                                   page_size=1000)
            entries = [entry.to_api_repr() for entry in entries_iterator]
            print(f"Saving {len(entries)} log entries to cache: {cache_filename}")
            with open(cache_filename, 'w') as f:
                json.dump(entries, f)
            return entries
        except google_api_exceptions.ResourceExhausted as e:
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)
                print(f"  [WARNING] Quota limit likely hit. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"  [ERROR] Failed to retrieve logs after {max_retries} attempts. Error: {e}")
                return []
        except Exception as e:
            print(f"  [ERROR] An unexpected error occurred. Error: {e}")
            return []
    return []


def parse_log(log_entry: dict) -> Optional[Tuple[datetime, str, str]]:
    """Parses a GCP log, handling both text and json payloads and using the reliable outer timestamp."""
    message = None
    if "jsonPayload" in log_entry and isinstance(log_entry["jsonPayload"], dict) and "message" in log_entry[
        "jsonPayload"]:
        message = log_entry["jsonPayload"]["message"]
    elif "textPayload" in log_entry:
        message = log_entry["textPayload"]

    if not message: return None

    try:
        timestamp = datetime.fromisoformat(log_entry["timestamp"].replace("Z", "+00:00"))
    except (ValueError, KeyError):
        return None

    match = LOG_PATTERN.search(message)
    if match:
        _, run_id, core_message = match.groups()
        return timestamp, run_id, core_message

    match = DEBUG_LOG_PATTERN.search(message)
    if match:
        _, run_id, core_message = match.groups()
        return timestamp, run_id, core_message

    return None


# NEW: Function to export the final processed data to a CSV file.
def export_to_csv(all_metrics: Dict[str, Dict[str, list]]):
    """Exports all processed latency data to a single CSV file."""
    output_filename = "pubsub_latency_results.csv"
    print(f"\nExporting detailed results to {output_filename}...")

    fieldnames = [
        'scenario', 'message_size', 'invocation_rate', 'invocation_index',
        'total_pubsub_latency_ms', 'publish_latency_ms', 'transport_latency_ms'
    ]

    with open(output_filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for scenario_name, metrics in all_metrics.items():
            size_part = "small" if "Small" in scenario_name else "large"
            rate_part = "0.1_rps" if "0.1" in scenario_name else "5_rps"

            num_invocations = len(metrics.get('total_pubsub_latency', []))

            for i in range(num_invocations):
                writer.writerow({
                    'scenario': scenario_name,
                    'message_size': size_part,
                    'invocation_rate': rate_part,
                    'invocation_index': i + 1,
                    'total_pubsub_latency_ms': metrics['total_pubsub_latency'][i] * 1000,
                    'publish_latency_ms': metrics['publish_latency'][i] * 1000,
                    'transport_latency_ms': metrics['transport_latency'][i] * 1000,
                })


def plot_results(all_metrics: Dict[str, Dict[str, list]]):
    """Generates a 2x2 grid of CDFs for the TOTAL Pub/Sub latency across all scenarios."""
    fig, axes = plt.subplots(2, 2, figsize=(12, 10), sharex=True, sharey=True)
    fig.suptitle('Total End-to-End Pub/Sub Latency Distribution (Warm Starts Only)', fontsize=16)

    scenario_map = {
        "Small Message (0.1 rps)": axes[0, 0], "Small Message (5 rps)": axes[0, 1],
        "Large Message (0.1 rps)": axes[1, 0], "Large Message (5 rps)": axes[1, 1],
    }

    for name, ax in scenario_map.items():
        metrics = all_metrics.get(name)
        if not metrics or 'total_pubsub_latency' not in metrics:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center');
            continue

        latencies_ms = np.array(metrics['total_pubsub_latency']) * 1000.0
        sorted_latencies, cdf_y = np.sort(latencies_ms), np.arange(1, len(latencies_ms) + 1) / len(latencies_ms)

        ax.plot(sorted_latencies, cdf_y, lw=2)
        title = name.replace("Small Message", "10.0 KB").replace("Large Message", "1024.0 KB")
        ax.set_title(title, fontsize=12)
        ax.set_xscale('log')
        ax.grid(True, which="both", linestyle='--', linewidth=0.5)

    fig.text(0.5, 0.04, 'Latency (milliseconds)', ha='center', va='center', fontsize=14)
    fig.text(0.06, 0.5, 'CDF', ha='center', va='center', rotation='vertical', fontsize=14)
    plt.xlim(left=10)
    plt.tight_layout(rect=[0.08, 0.05, 1, 0.95])

    filename = "pubsub_latency_cdf.png"
    plt.savefig(filename)
    print(f"\nCDF plot of total Pub/Sub latencies saved to {filename}")


def analyze_scenarios():
    """Main function to loop through scenarios, fetch, parse, and analyze logs."""
    try:
        logging_client = logging_v2.Client()
    except Exception:
        print("[ERROR] Could not authenticate with Google Cloud.");
        sys.exit(1)

    all_scenario_metrics = {}

    for name, window in SCENARIOS.items():
        print(f"\n--- Analyzing Scenario: {name} ---")
        all_logs = {k: fetch_logs(logging_client, name, k, s_name, window['start_time'], window['end_time']) for
                    k, s_name in SERVICE_NAMES.items()}
        flat_logs = [log for logs in all_logs.values() for log in logs]
        if not flat_logs:
            print(f"  - [WARNING] No logs found. Skipping.");
            continue

        cold_start_instance_ids, instance_to_run_id, trace_to_request_start = set(), {}, {}
        for log in flat_logs:
            if "run.googleapis.com%2Fvarlog%2Fsystem" in log.get("logName", "") and "Starting new instance" in log.get(
                    "textPayload", ""):
                instance_id = log.get("labels", {}).get("instanceId");
                if instance_id: cold_start_instance_ids.add(instance_id)
            if "run.googleapis.com%2Frequests" in log.get("logName", ""):
                trace, payload = log.get("trace"), log.get("protoPayload") or log.get("httpRequest")
                if trace and payload and "latency" in payload:
                    trace_id, end_time = trace.split("/")[-1], datetime.fromisoformat(
                        log["timestamp"].replace("Z", "+00:00"))
                    trace_to_request_start[trace_id] = end_time - timedelta(
                        seconds=float(payload["latency"].rstrip("s")))
            parsed, instance_id = parse_log(log), log.get("labels", {}).get("instanceId")
            if parsed and instance_id: instance_to_run_id[instance_id] = parsed[1]

        cold_start_run_ids = {instance_to_run_id[inst_id] for inst_id in cold_start_instance_ids if
                              inst_id in instance_to_run_id}
        print(f"  - Found {len(cold_start_run_ids)} runs that included a cold start. Excluding.")

        run_events = defaultdict(list)
        for logs in all_logs.values():
            for log in logs:
                parsed = parse_log(log)
                if parsed and parsed[1] not in cold_start_run_ids:
                    trace_id = log.get("trace", "").split("/")[-1] if log.get("trace") else None
                    run_events[parsed[1]].append({'timestamp': parsed[0], 'message': parsed[2], 'trace_id': trace_id})

        metrics = defaultdict(list)
        for run_id, events in run_events.items():
            events.sort(key=lambda x: x['timestamp'])
            invocations = defaultdict(dict)
            for event in events:
                timestamp, message, trace_id = event['timestamp'], event['message'], event['trace_id']
                taint_match = TAINT_PATTERN.search(message)
                if not taint_match: continue
                taint = taint_match.group(1)
                if message.startswith("DEBUG_PUBLISHING_TO_MESSAGING_SERVICE"):
                    invocations[taint]['publish_start_time'] = timestamp
                elif message.startswith("INVOKING_SUCCESSOR"):
                    invocations[taint]['publish_acked_time'] = timestamp
                elif message.startswith("INVOKED"):
                    instance_match = INSTANCE_PATTERN.search(message)
                    if instance_match and instance_match.group(1).startswith('destination'):
                        invocations[taint]['subscriber_invoked_time'] = timestamp
                        if trace_id: invocations[taint]['subscriber_trace_id'] = trace_id

            for taint, data in invocations.items():
                if all(k in data for k in ['publish_start_time', 'publish_acked_time', 'subscriber_invoked_time']):
                    subscriber_start_time = trace_to_request_start.get(data.get('subscriber_trace_id')) or data[
                        'subscriber_invoked_time']
                    total_latency = (subscriber_start_time - data['publish_start_time']).total_seconds()
                    publish_latency = (data['publish_acked_time'] - data['publish_start_time']).total_seconds()
                    transport_latency = (subscriber_start_time - data['publish_acked_time']).total_seconds()
                    if total_latency >= 0 and publish_latency >= 0:
                        metrics['total_pubsub_latency'].append(total_latency)
                        metrics['publish_latency'].append(publish_latency)
                        metrics['transport_latency'].append(transport_latency)

        if not metrics['total_pubsub_latency']:
            print("  - [ERROR] Found no complete, correlated warm workflow runs.");
            continue

        median_results = {f'median_{key}': np.median(val) for key, val in metrics.items() if val}
        print(f"Results for '{name}':")
        print(f"  - Found {len(metrics['total_pubsub_latency'])} complete warm runs.")
        print(f"  - Median Total E2E Latency: {median_results.get('median_total_pubsub_latency', 0):.4f}s")
        print("  --- Breakdown ---")
        print(f"    - Median Publish Latency:   {median_results.get('median_publish_latency', 0):.4f}s")
        print(f"    - Median Transport Latency: {median_results.get('median_transport_latency', 0):.4f}s")
        all_scenario_metrics[name] = metrics

    if all_scenario_metrics:
        export_to_csv(all_scenario_metrics)
        plot_results(all_scenario_metrics)


if __name__ == "__main__":
    analyze_scenarios()