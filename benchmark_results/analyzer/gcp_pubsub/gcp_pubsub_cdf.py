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
    "get_requests": "pubs-ting-0-0-1-get-ests-gcp-us-ea1-97b98094d0a0",
    "destination": "pubs-ting-0-0-1-dest-tion-gcp-us-ea1-3c3c79569ac3",
}

SCENARIOS = {
    "Large Message (0.1 rps)": {
        "start_time": datetime(2025, 10, 8, 1, 18, 4, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 8, 2, 0, 18, tzinfo=timezone.utc),
    },
    "Large Message (5 rps)": {
        "start_time": datetime(2025, 10, 8, 1, 14, 23, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 8, 1, 15, 51, tzinfo=timezone.utc),
    },
    "Small Message (0.1 rps)": {
        "start_time": datetime(2025, 10, 8, 2, 3, 37, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 8, 2, 46, 0, tzinfo=timezone.utc),
    },
    "Small Message (5 rps)": {
        "start_time": datetime(2025, 10, 8, 1, 11, 55, tzinfo=timezone.utc),
        "end_time": datetime(2025, 10, 8, 1, 13, 30, tzinfo=timezone.utc),
    },
}
# -------------------

# --- Regular Expressions for Parsing ---
LOG_PATTERN = re.compile(r"TIME \((.*?)\) RUN_ID \((.*?)\) MESSAGE \((.*?)\) LOG_VERSION")
TAINT_PATTERN = re.compile(r"TAINT \((.*?)\)")
INSTANCE_PATTERN = re.compile(r"INSTANCE \((.*?)\)")
SUCCESSOR_PATTERN = re.compile(r"SUCCESSOR \((.*?)\)")


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
        '(logName=~ "run.googleapis.com%2Frequests" OR logName =~ "run.googleapis.com%2Fvarlog%2Fsystem" OR jsonPayload.severity = "CARIBOU")'
    ]
    query = " AND ".join(filter_parts)

    print(f"Fetching logs for '{service_name}'...")

    max_retries = 5
    initial_delay = 5

    for attempt in range(max_retries):
        try:
            entries = client.list_entries(resource_names=[f"projects/{GCP_PROJECT_ID}"], filter_=query, page_size=1000)
            return [entry.to_api_repr() for entry in entries]
        except google_api_exceptions.ResourceExhausted as e:
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)
                print(
                    f"  [WARNING] Quota limit likely hit. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
            else:
                print(f"  [ERROR] Failed to retrieve logs for {service_name} after {max_retries} attempts. Error: {e}")
                return []
        except Exception as e:
            print(f"  [ERROR] An unexpected error occurred while fetching logs for {service_name}. Error: {e}")
            return []
    return []


def parse_caribou_log(log_entry: dict) -> Optional[Tuple[datetime, str, str]]:
    """Parses a structured GCP log to extract the core Caribou log message."""
    payload = log_entry.get("jsonPayload", {})
    message = payload.get("message")
    if not message: return None

    match = LOG_PATTERN.search(message)
    if not match: return None

    timestamp_str, run_id, core_message = match.groups()
    try:
        if ',' in timestamp_str:
            timestamp_str = timestamp_str.replace(',', '.', 1)
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f%z")
        return timestamp, run_id, core_message
    except ValueError:
        return None


# MODIFIED: Rewritten plot function to generate a 2x2 grid of CDFs.
def plot_results(all_metrics: Dict[str, Dict[str, list]]):
    """Generates a 2x2 grid of CDFs for Pub/Sub latency across all scenarios."""

    # Setup a 2x2 grid of plots that share their X and Y axes for easy comparison
    fig, axes = plt.subplots(2, 2, figsize=(12, 10), sharex=True, sharey=True)
    fig.suptitle('Pub/Sub Latency Distribution (Warm Starts Only)', fontsize=16)

    # Define the order and mapping of scenarios to subplots
    scenario_map = {
        "Small Message (0.1 rps)": axes[0, 0],
        "Small Message (5 rps)": axes[0, 1],
        "Large Message (0.1 rps)": axes[1, 0],
        "Large Message (5 rps)": axes[1, 1],
    }

    for name, ax in scenario_map.items():
        metrics = all_metrics.get(name)
        if not metrics or 'delay_get_requests_to_destination' not in metrics:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center')
            continue

        # Get latency data in milliseconds
        latencies_sec = metrics['delay_get_requests_to_destination']
        latencies_ms = np.array(latencies_sec) * 1000.0

        # Sort data for CDF plotting
        sorted_latencies = np.sort(latencies_ms)

        # Calculate Y-axis values for the CDF
        cdf_y = np.arange(1, len(sorted_latencies) + 1) / len(sorted_latencies)

        # Plot the CDF
        ax.plot(sorted_latencies, cdf_y, lw=2)

        # Format the subplot
        title = name.replace("Small Message", "10.0 KB").replace("Large Message", "1024.0 KB")
        ax.set_title(title, fontsize=12)
        ax.set_xscale('log')
        ax.grid(True, which="both", linestyle='--', linewidth=0.5)

    # Common labels for the entire figure
    fig.text(0.5, 0.04, 'Latency (milliseconds)', ha='center', va='center', fontsize=14)
    fig.text(0.06, 0.5, 'CDF', ha='center', va='center', rotation='vertical', fontsize=14)

    plt.tight_layout(rect=[0.08, 0.05, 1, 0.95])  # Adjust layout to make room for titles

    filename = "pubsub_latency_cdf.png"
    plt.savefig(filename)
    print(f"\nCDF plot of Pub/Sub latencies saved to {filename}")


def analyze_scenarios():
    """Main function to loop through scenarios, fetch, parse, and analyze logs."""
    try:
        logging_client = logging_v2.Client()
    except Exception:
        print("[ERROR] Could not authenticate with Google Cloud.")
        print("Please run 'gcloud auth application-default login' and try again.")
        sys.exit(1)

    all_scenario_metrics = {}  # MODIFIED: Store raw metrics for plotting

    for name, window in SCENARIOS.items():
        print(f"\n--- Analyzing Scenario: {name} ---")

        all_logs = {key: fetch_logs(logging_client, s_name, window['start_time'], window['end_time']) for key, s_name in
                    SERVICE_NAMES.items()}

        flat_logs = [log for logs in all_logs.values() for log in logs]
        if not flat_logs:
            print(f"  - [WARNING] No logs found for scenario '{name}'. Skipping.")
            continue

        # --- Build lookup maps for efficient correlation ---
        cold_start_instance_ids = set()
        instance_to_run_id = {}
        trace_to_request_start = {}

        for log in flat_logs:
            if "run.googleapis.com%2Fvarlog%2Fsystem" in log.get("logName", "") and "Starting new instance" in log.get(
                    "textPayload", ""):
                instance_id = log.get("labels", {}).get("instanceId")
                if instance_id:
                    cold_start_instance_ids.add(instance_id)

            if "run.googleapis.com%2Frequests" in log.get("logName", ""):
                trace = log.get("trace")
                payload = log.get("protoPayload") or log.get("httpRequest")
                if trace and payload and "latency" in payload:
                    trace_id = trace.split("/")[-1]
                    end_time = datetime.fromisoformat(log["timestamp"].replace("Z", "+00:00"))
                    latency_s = float(payload["latency"].rstrip("s"))
                    trace_to_request_start[trace_id] = end_time - timedelta(seconds=latency_s)

            parsed = parse_caribou_log(log)
            instance_id = log.get("labels", {}).get("instanceId")
            if parsed and instance_id:
                _, run_id, _ = parsed
                instance_to_run_id[instance_id] = run_id

        cold_start_run_ids = {instance_to_run_id[inst_id] for inst_id in cold_start_instance_ids if
                              inst_id in instance_to_run_id}
        print(f"  - Found {len(cold_start_run_ids)} runs that included a cold start. Excluding from analysis.")

        run_events = defaultdict(list)
        for service_key, logs in all_logs.items():
            for log in logs:
                parsed = parse_caribou_log(log)
                if not parsed: continue
                timestamp, run_id, message = parsed
                if run_id in cold_start_run_ids: continue

                trace_id = log.get("trace", "").split("/")[-1] if log.get("trace") else None
                run_events[run_id].append({'timestamp': timestamp, 'message': message, 'trace_id': trace_id})

        metrics = defaultdict(list)
        for run_id, events in run_events.items():
            events.sort(key=lambda x: x['timestamp'])

            run_timeline = {}
            invocations = {}

            for event in events:
                timestamp, message, trace_id = event['timestamp'], event['message'], event['trace_id']

                instance_match = INSTANCE_PATTERN.search(message)
                if not instance_match: continue
                instance_name = instance_match.group(1).split(':')[0]

                if message.startswith("INVOKED"):
                    run_timeline.setdefault(instance_name, {})['start_time'] = timestamp
                    if trace_id:
                        run_timeline[instance_name]['trace_id'] = trace_id
                elif message.startswith("EXECUTED"):
                    run_timeline.setdefault(instance_name, {})['end_time'] = timestamp
                elif message.startswith("INVOKING_SUCCESSOR"):
                    taint_match = TAINT_PATTERN.search(message)
                    successor_match = SUCCESSOR_PATTERN.search(message)
                    if taint_match and successor_match:
                        taint, successor = taint_match.group(1), successor_match.group(1).split(':')[0]
                        invocations[taint] = {'start_time': timestamp, 'from': instance_name, 'to': successor}

            # --- HYBRID LATENCY CALCULATION ---
            for taint, data in invocations.items():
                from_instance = data['from']
                to_instance = data['to']

                if to_instance in run_timeline and 'start_time' in run_timeline[to_instance]:
                    successor_invoked_time = run_timeline[to_instance]['start_time']
                    invocation_start_time = data['start_time']

                    precise_delay_calculated = False
                    if 'trace_id' in run_timeline[to_instance]:
                        successor_trace_id = run_timeline[to_instance]['trace_id']
                        if successor_trace_id in trace_to_request_start:
                            request_start_time = trace_to_request_start[successor_trace_id]
                            delay = (request_start_time - invocation_start_time).total_seconds()
                            if delay >= 0:
                                metrics[f'delay_{from_instance}_to_{to_instance}'].append(delay)
                                precise_delay_calculated = True
                                platform_overhead = (successor_invoked_time - request_start_time).total_seconds()
                                metrics[f'platform_overhead_{to_instance}'].append(platform_overhead)

                    if not precise_delay_calculated:
                        delay = (successor_invoked_time - invocation_start_time).total_seconds()
                        if delay >= 0:
                            metrics[f'delay_{from_instance}_to_{to_instance}'].append(delay)
                            metrics['imprecise_delays_counted'].append(1)

            critical_path = ['get_requests', 'destination']
            if all(k in run_timeline and 'start_time' in run_timeline[k] and 'end_time' in run_timeline[k] for k in
                   critical_path):
                e2e_time = (run_timeline['destination']['end_time'] - run_timeline['get_requests'][
                    'start_time']).total_seconds()
                metrics['e2e_time'].append(e2e_time)

                for inst in critical_path:
                    metrics[f'{inst}_exec_time'].append(
                        (run_timeline[inst]['end_time'] - run_timeline[inst]['start_time']).total_seconds())

        if not metrics['e2e_time']:
            print("  - [ERROR] Found no complete, correlated warm workflow runs in this scenario.")
            continue

        median_results = {f'median_{key}': np.median(val) for key, val in metrics.items() if val}

        print(f"Results for '{name}':")
        print(f"  - Found {len(metrics['e2e_time'])} complete warm runs.")
        if 'imprecise_delays_counted' in metrics:
            print(
                f"  - Note: {len(metrics['imprecise_delays_counted'])} Pub/Sub delays were calculated using the less precise fallback method.")
        print(f"  - Median End-to-End Time: {median_results.get('median_e2e_time', 0):.4f}s")
        print("  --- Breakdown ---")
        print(f"    - Median 'get_requests' execution: {median_results.get('median_get_requests_exec_time', 0):.4f}s")
        print(
            f"    - Median Pub/Sub Latency:          {median_results.get('median_delay_get_requests_to_destination', 0):.4f}s")
        print(f"    - Median 'destination' execution:  {median_results.get('median_destination_exec_time', 0):.4f}s")
        print(
            f"    - Median Platform Overhead at 'destination': {median_results.get('median_platform_overhead_destination', 0):.4f}s")

        all_scenario_metrics[name] = metrics  # MODIFIED: Store raw data for plotting

    if all_scenario_metrics:
        plot_results(all_scenario_metrics)  # MODIFIED: Pass raw data to plotter


if __name__ == "__main__":
    analyze_scenarios()