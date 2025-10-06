import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

from google.cloud import logging_v2
import numpy as np
import matplotlib.pyplot as plt

# --- Configuration ---
GCP_PROJECT_ID = "caribou-460422"
SERVICE_1_NAME = "imag-sing-0-0-1-get-ests-gcp-us-ea1-92373516e9de"
SERVICE_2_NAME = "imag-sing-0-0-1-imag-ssor-gcp-us-ea1-ff3d4431cad6"

# Define the UTC time windows for each benchmark scenario
# NOTE: Timestamps from logs appear to be from 2025. This script assumes that year.
SCENARIOS = {
    "Low (0.1 rps)": {
        "start_time": datetime(2025, 9, 24, 21, 33, 20, tzinfo=timezone.utc),
        "end_time": datetime(2025, 9, 24, 21, 55, 30, tzinfo=timezone.utc),
    },
    "Medium (1 rps)": {
        "start_time": datetime(2025, 9, 24, 21, 14, 10, tzinfo=timezone.utc),
        "end_time": datetime(2025, 9, 24, 21, 30, 32, tzinfo=timezone.utc),
    },
    # "High (5 rps)": {
    #     "start_time": datetime(2025, 9, 24, 23, 37, 45, tzinfo=timezone.utc),
    #     "end_time": datetime(2025, 9, 24, 23, 52, 42, tzinfo=timezone.utc),
    # },
}
# -------------------

# --- Regular Expressions for Parsing ---
LOG_PATTERN = re.compile(r"TIME \((.*?)\) RUN_ID \((.*?)\) MESSAGE \((.*?)\) LOG_VERSION")
TOTAL_EXECUTION_TIME_PATTERN = re.compile(r"TOTAL_EXECUTION_TIME \((.*?)\) s")
TAINT_PATTERN = re.compile(r"TAINT \((.*?)\)")


# ---------------------------------------


def fetch_logs(client: logging_v2.Client, service_name: str, start_time: datetime, end_time: datetime) -> List[dict]:
    """Fetches structured logs for a specific Cloud Run service in a given time window."""
    time_start_str = start_time.isoformat()
    time_end_str = end_time.isoformat()

    # Broaden filter to get CARIBOU logs, request logs, and system logs for cold start detection
    filter_parts = [
        f'resource.type="cloud_run_revision"',
        f'resource.labels.service_name="{service_name}"',
        f'timestamp >= "{time_start_str}"',
        f'timestamp <= "{time_end_str}"',
        '(logName=~ "run.googleapis.com%2Frequests" OR logName =~ "run.googleapis.com%2Fvarlog%2Fsystem" OR jsonPayload.severity = "CARIBOU")'
    ]
    query = " AND ".join(filter_parts)

    print(
        f"Fetching logs for '{service_name}' from {start_time.strftime('%H:%M:%S')} to {end_time.strftime('%H:%M:%S')} UTC...")
    try:
        entries = client.list_entries(
            resource_names=[f"projects/{GCP_PROJECT_ID}"],
            filter_=query,
            page_size=1000,
        )
        # Convert pages of entries to a single list of dictionaries
        return [entry.to_api_repr() for entry in entries]
    except Exception as e:
        print(f"  [ERROR] Failed to retrieve logs for {service_name}. Error: {e}")
        return []


def parse_caribou_log(log_entry: dict) -> Optional[Tuple[datetime, str, str]]:
    """Parses a structured GCP log to extract the core Caribou log message."""
    payload = log_entry.get("jsonPayload", {})
    message = payload.get("message")
    if not message:
        return None

    match = LOG_PATTERN.search(message)
    if not match:
        return None

    timestamp_str, run_id, core_message = match.groups()
    try:
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f%z")
        return timestamp, run_id, core_message
    except ValueError:
        return None


def plot_results(results: Dict[str, Dict[str, float]]):
    """Generates a stacked bar chart from the analysis results."""
    labels = list(results.keys())
    get_ests_times = [results[label].get('avg_get_ests_time', 0) for label in labels]
    pubsub_delays = [results[label].get('avg_pubsub_delay', 0) for label in labels]
    imag_ssor_times = [results[label].get('avg_imag_ssor_time', 0) for label in labels]

    width = 0.5
    fig, ax = plt.subplots(figsize=(12, 8))

    # Use barh for horizontal bars
    p1 = ax.barh(labels, get_ests_times, width, label='Get Requests Execution Time')
    # Use 'left' to stack bars horizontally
    p2 = ax.barh(labels, pubsub_delays, width, left=get_ests_times, label='Pub/Sub Delay')
    p3 = ax.barh(labels, imag_ssor_times, width, left=[i + j for i, j in zip(get_ests_times, pubsub_delays)],
                 label='Image Processor Execution Time')

    ax.set_xlabel('Time (seconds)')
    ax.set_title('Workflow Latency Breakdown by Invocation Rate (Warm Starts Only)')
    ax.legend()
    ax.invert_yaxis()  # labels read top-to-bottom

    # Add text labels inside each bar segment
    for i in range(len(labels)):
        # Label for get-ests
        if get_ests_times[i] > 0:
            ax.text(get_ests_times[i] / 2, i, f'{get_ests_times[i]:.3f}s', ha='center', va='center', color='white',
                    weight='bold')
        # Label for Pub/Sub delay
        if pubsub_delays[i] > 0:
            ax.text(get_ests_times[i] + pubsub_delays[i] / 2, i, f'{pubsub_delays[i]:.3f}s', ha='center', va='center',
                    color='white', weight='bold')
        # Label for imag-ssor
        if imag_ssor_times[i] > 0:
            ax.text(get_ests_times[i] + pubsub_delays[i] + imag_ssor_times[i] / 2, i, f'{imag_ssor_times[i]:.3f}s',
                    ha='center', va='center', color='white', weight='bold')
        # Label for total time at the end of the bar
        total_height = get_ests_times[i] + pubsub_delays[i] + imag_ssor_times[i]
        ax.text(total_height + 0.05, i, f'Total: {total_height:.3f}s', ha='left', va='center')

    plt.tight_layout()
    filename = "v2_no5_workflow_latency_breakdown.png"
    plt.savefig(filename)
    print(f"\nStacked bar chart saved to {filename}")


def analyze_scenarios():
    """Main function to loop through scenarios, fetch, parse, and analyze logs."""
    try:
        logging_client = logging_v2.Client()
    except Exception as e:
        print("[ERROR] Could not authenticate with Google Cloud.")
        print("Please run 'gcloud auth application-default login' and try again.")
        sys.exit(1)

    all_scenario_results = {}

    for name, window in SCENARIOS.items():
        print(f"\n--- Analyzing Scenario: {name} ---")
        logs_service_1 = fetch_logs(logging_client, SERVICE_1_NAME, window['start_time'], window['end_time'])
        logs_service_2 = fetch_logs(logging_client, SERVICE_2_NAME, window['start_time'], window['end_time'])

        if not logs_service_1 and not logs_service_2:
            print("Could not find any logs in this time window. Skipping scenario.")
            continue

        # --- NEW, MORE ROBUST COLD START DETECTION ---
        all_logs = logs_service_1 + logs_service_2

        # 1. Find all instance IDs that had a cold start
        cold_start_instance_ids = set()
        for log in all_logs:
            if "run.googleapis.com%2Fvarlog%2Fsystem" in log.get("logName", ""):
                if "Starting new instance" in log.get("textPayload", ""):
                    instance_id = log.get("labels", {}).get("instanceId")
                    if instance_id:
                        cold_start_instance_ids.add(instance_id)

        # 2. Map instance IDs to run IDs
        instance_to_run_id = {}
        for log in all_logs:
            parsed = parse_caribou_log(log)
            instance_id = log.get("labels", {}).get("instanceId")
            if parsed and instance_id:
                _, run_id, _ = parsed
                instance_to_run_id[instance_id] = run_id

        # 3. Create the final set of cold start run IDs
        cold_start_run_ids = {instance_to_run_id[inst_id] for inst_id in cold_start_instance_ids if
                              inst_id in instance_to_run_id}

        print(f"  - Found {len(cold_start_run_ids)} runs that included a cold start. Excluding from analysis.")

        # --- Data Processing ---
        run_id_data = defaultdict(dict)
        invocation_start_times: Dict[str, datetime] = {}

        # Process logs from the first service (get-ests)
        for log in logs_service_1:
            parsed = parse_caribou_log(log)
            if not parsed: continue
            timestamp, run_id, message = parsed

            if run_id in cold_start_run_ids: continue

            if message.startswith("EXECUTED"):
                match = TOTAL_EXECUTION_TIME_PATTERN.search(message)
                if match:
                    run_id_data[run_id]['get_ests_time'] = float(match.group(1))

            if message.startswith("INVOKING_SUCCESSOR"):
                match = TAINT_PATTERN.search(message)
                if match:
                    invocation_start_times[match.group(1)] = timestamp

        # Process logs from the second service (imag-ssor)
        for log in logs_service_2:
            parsed = parse_caribou_log(log)
            if not parsed: continue
            timestamp, run_id, message = parsed

            if run_id in cold_start_run_ids: continue

            if message.startswith("EXECUTED"):
                match = TOTAL_EXECUTION_TIME_PATTERN.search(message)
                if match:
                    run_id_data[run_id]['imag_ssor_time'] = float(match.group(1))

            if message.startswith("INVOKED"):
                match = TAINT_PATTERN.search(message)
                if match and match.group(1) in invocation_start_times:
                    taint = match.group(1)
                    delay = (timestamp - invocation_start_times[taint]).total_seconds()
                    if delay >= 0:
                        run_id_data[run_id]['pubsub_delay'] = delay

        # --- Calculate Averages for this Scenario ---
        e2e_times, pubsub_delays, get_ests_times, imag_ssor_times = [], [], [], []
        for run_id, data in run_id_data.items():
            if 'get_ests_time' in data and 'imag_ssor_time' in data and 'pubsub_delay' in data:
                e2e_times.append(data['get_ests_time'] + data['imag_ssor_time'] + data['pubsub_delay'])
                pubsub_delays.append(data['pubsub_delay'])
                get_ests_times.append(data['get_ests_time'])
                imag_ssor_times.append(data['imag_ssor_time'])

        if not e2e_times:
            print("Found no complete, correlated warm workflow runs in this scenario.")
            continue

        avg_e2e = np.quantile(e2e_times, 0.25)
        avg_pubsub = np.quantile(pubsub_delays, 0.25)
        avg_get_ests = np.quantile(get_ests_times, 0.25)
        avg_imag_ssor = np.quantile(imag_ssor_times, 0.25)

        print(f"Results for '{name}':")
        print(f"  - Found {len(e2e_times)} complete runs.")
        print(f"  - 25th Percentile End-to-End Time: {avg_e2e:.4f}s")
        print(f"  - 25th Percentile Pub/Sub Delay:   {avg_pubsub:.4f}s")

        all_scenario_results[name] = {
            'avg_get_ests_time': avg_get_ests,
            'avg_pubsub_delay': avg_pubsub,
            'avg_imag_ssor_time': avg_imag_ssor,
        }

    # --- Plotting ---
    if all_scenario_results:
        plot_results(all_scenario_results)


if __name__ == "__main__":
    analyze_scenarios()

