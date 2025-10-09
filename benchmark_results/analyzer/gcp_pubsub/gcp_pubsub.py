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

# MODIFIED: Updated with the service names for your pub/sub testing workflow
SERVICE_NAMES = {
    "get_requests": "pubs-ting-0-0-1-get-ests-gcp-us-ea1-97b98094d0a0",
    "destination": "pubs-ting-0-0-1-dest-tion-gcp-us-ea1-3c3c79569ac3",
}

# MODIFIED: Updated with the four scenarios from your client logs.
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
        "end_time": datetime(2025, 10, 8, 1, 13, 25, tzinfo=timezone.utc),
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
        # GCP logs often use a format like '2025-10-08 01:41:58,423060+0000'
        # We handle this by replacing the comma with a period for strptime
        if ',' in timestamp_str:
            timestamp_str = timestamp_str.replace(',', '.', 1)
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f%z")
        return timestamp, run_id, core_message
    except ValueError:
        return None


# MODIFIED: Rewritten plot function for the new two-stage workflow
def plot_results(results: Dict[str, Dict[str, float]]):
    """Generates a horizontal stacked bar chart visualizing the critical path for the pub/sub workflow."""
    labels = list(results.keys())

    get_requests_times = [results[label].get('median_get_requests_exec_time', 0) for label in labels]
    pubsub_delay = [results[label].get('median_delay_get_requests_to_destination', 0) for label in labels]
    destination_times = [results[label].get('median_destination_exec_time', 0) for label in labels]

    fig, ax = plt.subplots(figsize=(16, 8))

    lefts = np.zeros(len(labels))
    components = [
        (get_requests_times, 'get_requests execution'),
        (pubsub_delay, 'Pub/Sub Latency'),
        (destination_times, 'destination execution'),
    ]

    for data, label in components:
        bars = ax.barh(labels, data, height=0.5, left=lefts, label=label)
        for i, bar in enumerate(bars):
            width = bar.get_width()
            if width > 0.001: # Only label significant bars
                ax.text(lefts[i] + width / 2, bar.get_y() + bar.get_height() / 2,
                        f'{width:.4f}s', ha='center', va='center', color='white', weight='bold')
        lefts += np.array(data)

    ax.set_xlabel('Time (seconds)')
    ax.set_title('Pub/Sub Workflow Latency Breakdown (Warm Starts Only)')
    ax.legend(loc='lower right')
    ax.invert_yaxis()

    # Add total latency text
    for i, total in enumerate(lefts):
        ax.text(total + 0.01, i, f'Total: {total:.4f}s', ha='left', va='center', weight='bold')

    plt.tight_layout()
    filename = "pubsub_workflow_latency_breakdown.png"
    plt.savefig(filename)
    print(f"\nStacked bar chart of the critical path saved to {filename}")


def analyze_scenarios():
    """Main function to loop through scenarios, fetch, parse, and analyze logs."""
    try:
        logging_client = logging_v2.Client()
    except Exception:
        print("[ERROR] Could not authenticate with Google Cloud.")
        print("Please run 'gcloud auth application-default login' and try again.")
        sys.exit(1)

    all_scenario_results = {}

    for name, window in SCENARIOS.items():
        print(f"\n--- Analyzing Scenario: {name} ---")

        all_logs = {key: fetch_logs(logging_client, s_name, window['start_time'], window['end_time']) for key, s_name in
                    SERVICE_NAMES.items()}

        flat_logs = [log for logs in all_logs.values() for log in logs]

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
                # Use protoPayload for v2 logs, fall back to httpRequest for v1
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

                    # Fallback to less precise measurement if trace was missing
                    if not precise_delay_calculated:
                        delay = (successor_invoked_time - invocation_start_time).total_seconds()
                        if delay >= 0:
                            metrics[f'delay_{from_instance}_to_{to_instance}'].append(delay)
                            metrics['imprecise_delays_counted'].append(1)
            # --- END HYBRID SECTION ---

            # MODIFIED: Updated critical path and metric calculations
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

        # MODIFIED: Updated print statements for the new workflow
        print(f"Results for '{name}':")
        print(f"  - Found {len(metrics['e2e_time'])} complete warm runs.")
        if 'imprecise_delays_counted' in metrics:
            print(
                f"  - Note: {len(metrics['imprecise_delays_counted'])} Pub/Sub delays were calculated using the less precise fallback method.")
        print(f"  - Median End-to-End Time: {median_results.get('median_e2e_time', 0):.4f}s")
        print("  --- Breakdown ---")
        print(f"    - Median 'get_requests' execution: {median_results.get('median_get_requests_exec_time', 0):.4f}s")
        print(f"    - Median Pub/Sub Latency:          {median_results.get('median_delay_get_requests_to_destination', 0):.4f}s")
        print(f"    - Median 'destination' execution:  {median_results.get('median_destination_exec_time', 0):.4f}s")
        print(f"    - Median Platform Overhead at 'destination': {median_results.get('median_platform_overhead_destination', 0):.4f}s")


        all_scenario_results[name] = median_results

    if all_scenario_results:
        plot_results(all_scenario_results)


if __name__ == "__main__":
    analyze_scenarios()