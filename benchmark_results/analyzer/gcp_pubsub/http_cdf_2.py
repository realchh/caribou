import re
import sys
import time
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
    "get_requests": "pubs-ting-0-0-4-get-ests-gcp-us-ea1-93de73e8a025",
    "destination": "pubs-ting-0-0-4-dest-tion-gcp-us-ea1-a7f30e8a05b2",
}

SCENARIOS = {
    "Large Message (0.1 rps)": {
        "start_time": datetime(2025, 10, 14, 21, 29, 00, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 14, 22, 14, 00, tzinfo=timezone.utc),
    },
    "Large Message (5 rps)": {
        "start_time": datetime(2025, 10, 14, 20, 31, 25, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 14, 20, 34, 15, tzinfo=timezone.utc),
    },
    "Small Message (0.1 rps)": {
        "start_time": datetime(2025, 10, 14, 20, 43, 30, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 14, 21, 27, 00, tzinfo=timezone.utc),
    },
    "Small Message (5 rps)": {
        "start_time": datetime(2025, 10, 14, 20, 37, 30, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 14, 20, 41, 9, tzinfo=timezone.utc),
    },
}
# -------------------

# --- Regular Expressions for Parsing ---
LOG_PATTERN = re.compile(r"TIME \((.*?)\) RUN_ID \((.*?)\) MESSAGE \((.*?)\) LOG_VERSION")
DEBUG_LOG_PATTERN = re.compile(r"TIME (.*) RUN_ID (\S+) MESSAGE\((.*)\)")
TAINT_PATTERN = re.compile(r"TAINT \((.*?)\)")
INSTANCE_PATTERN = re.compile(r"INSTANCE \((.*?)\)")


# ---------------------------------------


def fetch_logs(client: logging_v2.Client, service_name: str, start_time: datetime, end_time: datetime) -> List[dict]:
    """Fetches structured logs for a specific Cloud Run service with exponential backoff."""
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

    print(f"Fetching logs for '{service_name}'...")
    # ... (rest of function is unchanged) ...
    max_retries = 5
    initial_delay = 5

    for attempt in range(max_retries):
        try:
            entries = client.list_entries(resource_names=[f"projects/{GCP_PROJECT_ID}"], filter_=query, page_size=1000)
            return [entry.to_api_repr() for entry in entries]
        except google_api_exceptions.ResourceExhausted as e:
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)
                print(f"  [WARNING] Quota limit likely hit. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"  [ERROR] Failed to retrieve logs for {service_name} after {max_retries} attempts. Error: {e}")
                return []
        except Exception as e:
            print(f"  [ERROR] An unexpected error occurred while fetching logs for {service_name}. Error: {e}")
            return []
    return []


# MODIFIED: Rewritten parse_log to handle both textPayload and jsonPayload structures.
def parse_log(log_entry: dict) -> Optional[Tuple[datetime, str, str]]:
    """Parses a structured GCP log to extract the core log message, handling multiple formats."""
    message = None

    # Try to get the message from a structured JSON payload first.
    if "jsonPayload" in log_entry and isinstance(log_entry["jsonPayload"], dict) and "message" in log_entry[
        "jsonPayload"]:
        message = log_entry["jsonPayload"]["message"]
    # If that fails, try to get it from a simple text payload.
    elif "textPayload" in log_entry:
        message = log_entry["textPayload"]

    if not message:
        return None

    # Now, attempt to match the message content against our known formats.
    match = LOG_PATTERN.search(message)
    if match:
        timestamp_str, run_id, core_message = match.groups()
        try:
            if ',' in timestamp_str:
                timestamp_str = timestamp_str.replace(',', '.', 1)
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f%z")
            return timestamp, run_id, core_message
        except ValueError:
            return None

    match = DEBUG_LOG_PATTERN.search(message)
    if match:
        timestamp_str, run_id, core_message = match.groups()
        try:
            timestamp_str = timestamp_str.strip()
            timestamp = datetime.fromisoformat(timestamp_str)
            return timestamp, run_id, core_message
        except ValueError:
            return None

    return None


def plot_results(all_metrics: Dict[str, Dict[str, list]]):
    """Generates a 2x2 grid of CDFs for the TOTAL HTTP latency across all scenarios."""
    fig, axes = plt.subplots(2, 2, figsize=(12, 10), sharex=True, sharey=True)
    fig.suptitle('Total End-to-End HTTP Latency Distribution (Warm Starts Only)', fontsize=16)

    scenario_map = {
        "Small Message (0.1 rps)": axes[0, 0],
        "Small Message (5 rps)": axes[0, 1],
        "Large Message (0.1 rps)": axes[1, 0],
        "Large Message (5 rps)": axes[1, 1],
    }
    # ... (rest of function is unchanged) ...
    for name, ax in scenario_map.items():
        metrics = all_metrics.get(name)
        if not metrics or 'total_e2e_latency' not in metrics:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center')
            continue

        latencies_sec = metrics['total_e2e_latency']
        latencies_ms = np.array(latencies_sec) * 1000.0

        sorted_latencies = np.sort(latencies_ms)
        cdf_y = np.arange(1, len(sorted_latencies) + 1) / len(sorted_latencies)

        ax.plot(sorted_latencies, cdf_y, lw=2)

        title = name.replace("Small Message", "10.0 KB").replace("Large Message", "1024.0 KB")
        ax.set_title(title, fontsize=12)
        ax.set_xscale('log')
        ax.grid(True, which="both", linestyle='--', linewidth=0.5)

    fig.text(0.5, 0.04, 'Latency (milliseconds)', ha='center', va='center', fontsize=14)
    fig.text(0.06, 0.5, 'CDF', ha='center', va='center', rotation='vertical', fontsize=14)

    plt.xlim(left=10)
    plt.tight_layout(rect=[0.08, 0.05, 1, 0.95])

    filename = "http_2_pubsub_total_latency_cdf.png"
    plt.savefig(filename)
    print(f"\nCDF plot of total HTTP latencies saved to {filename}")


def analyze_scenarios():
    """Main function to loop through scenarios, fetch, parse, and analyze logs."""
    try:
        logging_client = logging_v2.Client()
    except Exception:
        print("[ERROR] Could not authenticate with Google Cloud.")
        sys.exit(1)

    all_scenario_metrics = {}
    # ... (rest of function is unchanged) ...
    for name, window in SCENARIOS.items():
        print(f"\n--- Analyzing Scenario: {name} ---")

        all_logs = {key: fetch_logs(logging_client, s_name, window['start_time'], window['end_time']) for key, s_name in
                    SERVICE_NAMES.items()}
        flat_logs = [log for logs in all_logs.values() for log in logs]
        if not flat_logs:
            print(f"  - [WARNING] No logs found for scenario '{name}'. Skipping.")
            continue

        cold_start_instance_ids, instance_to_run_id, trace_to_request_start = set(), {}, {}
        for log in flat_logs:
            if "run.googleapis.com%2Fvarlog%2Fsystem" in log.get("logName", "") and "Starting new instance" in log.get(
                    "textPayload", ""):
                instance_id = log.get("labels", {}).get("instanceId")
                if instance_id: cold_start_instance_ids.add(instance_id)
            if "run.googleapis.com%2Frequests" in log.get("logName", ""):
                trace = log.get("trace")
                payload = log.get("protoPayload") or log.get("httpRequest")
                if trace and payload and "latency" in payload:
                    trace_id, end_time = trace.split("/")[-1], datetime.fromisoformat(
                        log["timestamp"].replace("Z", "+00:00"))
                    latency_s = float(payload["latency"].rstrip("s"))
                    trace_to_request_start[trace_id] = end_time - timedelta(seconds=latency_s)
            parsed = parse_log(log)
            instance_id = log.get("labels", {}).get("instanceId")
            if parsed and instance_id:
                _, run_id, _ = parsed
                instance_to_run_id[instance_id] = run_id

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
                        if trace_id:
                            invocations[taint]['subscriber_trace_id'] = trace_id

            for taint, data in invocations.items():
                if all(k in data for k in ['publish_start_time', 'publish_acked_time', 'subscriber_invoked_time']):
                    subscriber_start_time = None
                    if 'subscriber_trace_id' in data and data['subscriber_trace_id'] in trace_to_request_start:
                        subscriber_start_time = trace_to_request_start[data['subscriber_trace_id']]
                    else:
                        subscriber_start_time = data['subscriber_invoked_time']

                    total_latency = (subscriber_start_time - data['publish_start_time']).total_seconds()
                    publish_latency = (data['publish_acked_time'] - data['publish_start_time']).total_seconds()
                    transport_latency = (subscriber_start_time - data['publish_acked_time']).total_seconds()

                    if total_latency > 0:
                        metrics['total_e2e_latency'].append(total_latency)
                        metrics['publish_latency'].append(publish_latency)
                        metrics['transport_latency'].append(transport_latency)

        if not metrics['total_e2e_latency']:
            print("  - [ERROR] Found no complete, correlated warm workflow runs.")
            continue

        median_results = {f'median_{key}': np.median(val) for key, val in metrics.items() if val}
        print(f"Results for '{name}':")
        print(f"  - Found {len(metrics['total_e2e_latency'])} complete warm runs.")
        print(f"  - Median Total E2E Latency: {median_results.get('median_total_e2e_latency', 0):.4f}s")
        print("  --- Breakdown ---")
        print(f"    - Median Publish Latency:   {median_results.get('median_publish_latency', 0):.4f}s")
        print(f"    - Median Transport Latency: {median_results.get('median_transport_latency', 0):.4f}s")

        all_scenario_metrics[name] = metrics

    if all_scenario_metrics:
        plot_results(all_scenario_metrics)


if __name__ == "__main__":
    analyze_scenarios()