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

# Define the service names for the Video Analytics workflow
SERVICE_NAMES = {
    "streaming": "vide-tics-0-0-1-stre-ming-gcp-us-ea1-e30129a4c248",
    "decode": "vide-tics-0-0-1-deco-code-gcp-us-ea1-3560a824f984",
    "recognition": "vide-tics-0-0-1-reco-tion-gcp-us-ea1-2d1c93992213",
    "consolidate": "vide-tics-0-0-1-cons-date-gcp-us-ea1-f9a1c9a47f95",
}

# Define the UTC time windows for each benchmark scenario
# Note: PDT is UTC-7. Timestamps are converted to UTC.
SCENARIOS = {
    "Low (0.1 rps)": {
        "start_time": datetime(2025, 9, 25, 22, 22, 0, tzinfo=timezone.utc),
        "end_time": datetime(2025, 9, 25, 22, 40, 0, tzinfo=timezone.utc),
    },
    "Medium (1 rps)": {
        "start_time": datetime(2025, 9, 25, 22, 53, 0, tzinfo=timezone.utc),
        "end_time": datetime(2025, 9, 25, 23, 10, 3, tzinfo=timezone.utc),
    },
    # "High (5 rps)": {
    #     "start_time": datetime(2025, 9, 26, 0, 42, 0, tzinfo=timezone.utc),
    #     "end_time": datetime(2025, 9, 26, 1, 0, 36, tzinfo=timezone.utc),
    # },
}
# -------------------

# --- Regular Expressions for Parsing ---
LOG_PATTERN = re.compile(r"TIME \((.*?)\) RUN_ID \((.*?)\) MESSAGE \((.*?)\) LOG_VERSION")
TAINT_PATTERN = re.compile(r"TAINT \((.*?)\)")
INSTANCE_PATTERN = re.compile(r"INSTANCE \((.*?)\)")
SUCCESSOR_PATTERN = re.compile(r"SUCCESSOR \((.*?)\)")


# ---------------------------------------


def fetch_logs(client: logging_v2.Client, service_name: str, start_time: datetime, end_time: datetime) -> List[dict]:
    """Fetches structured logs for a specific Cloud Run service in a given time window."""
    time_start_str = start_time.isoformat()
    time_end_str = end_time.isoformat()

    filter_parts = [
        f'resource.type="cloud_run_revision"',
        f'resource.labels.service_name="{service_name}"',
        f'timestamp >= "{time_start_str}"',
        f'timestamp <= "{time_end_str}"',
        'jsonPayload.severity="CARIBOU"'
    ]
    query = " AND ".join(filter_parts)

    print(f"Fetching logs for '{service_name}'...")
    try:
        entries = client.list_entries(resource_names=[f"projects/{GCP_PROJECT_ID}"], filter_=query, page_size=1000)
        return [entry.to_api_repr() for entry in entries]
    except Exception as e:
        print(f"  [ERROR] Failed to retrieve logs for {service_name}. Error: {e}")
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
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f%z")
        return timestamp, run_id, core_message
    except ValueError:
        return None


def plot_results(results: Dict[str, Dict[str, float]]):
    """Generates a horizontal stacked bar chart visualizing the critical path."""
    labels = list(results.keys())

    streaming_times = [results[label].get('median_streaming_exec_time', 0) for label in labels]
    delay1 = [results[label].get('median_delay_streaming_to_decode', 0) for label in labels]
    decode_times = [results[label].get('median_decode_exec_time', 0) for label in labels]
    delay2 = [results[label].get('median_delay_decode_to_recognition', 0) for label in labels]
    recognition_times = [results[label].get('median_recognition_fanout_duration', 0) for label in labels]
    delay3 = [results[label].get('median_delay_recognition_to_consolidate', 0) for label in labels]
    consolidate_times = [results[label].get('median_consolidate_exec_time', 0) for label in labels]

    fig, ax = plt.subplots(figsize=(18, 8))

    lefts = np.zeros(len(labels))
    components = [
        (streaming_times, 'streaming'),
        (delay1, 'Pub/Sub Delay 1'),
        (decode_times, 'decode'),
        (delay2, 'Pub/Sub Delay 2'),
        (recognition_times, 'recognition (fan-out)'),
        (delay3, 'Pub/Sub Delay 3'),
        (consolidate_times, 'consolidate'),
    ]

    for data, label in components:
        bars = ax.barh(labels, data, height=0.5, left=lefts, label=label)
        for i, bar in enumerate(bars):
            width = bar.get_width()
            if width > 0.1:  # Don't label very small segments
                ax.text(lefts[i] + width / 2, bar.get_y() + bar.get_height() / 2,
                        f'{width:.3f}s', ha='center', va='center', color='white', weight='bold', fontsize=10)
        lefts += np.array(data)

    ax.set_xlabel('Time (seconds)')
    ax.set_title('Median Critical Path Latency Breakdown for Video Analytics Workflow')
    ax.legend(loc='lower right')
    ax.invert_yaxis()

    for i, total in enumerate(lefts):
        ax.text(total + 0.1, i, f'Total Median: {total:.3f}s', ha='left', va='center', weight='bold')

    plt.tight_layout()
    filename = "video_workflow_latency_breakdown.png"
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

        run_events = defaultdict(list)
        for service_key, logs in all_logs.items():
            for log in logs:
                parsed = parse_caribou_log(log)
                if not parsed: continue
                timestamp, run_id, message = parsed
                run_events[run_id].append({'timestamp': timestamp, 'message': message, 'service': service_key})

        metrics = defaultdict(list)
        for run_id, events in run_events.items():
            events.sort(key=lambda x: x['timestamp'])

            run_timeline = {}
            invocations = {}

            for event in events:
                timestamp, message, service = event['timestamp'], event['message'], event['service']

                instance_match = INSTANCE_PATTERN.search(message)
                if not instance_match: continue
                instance_name = instance_match.group(1).split(':')[0]

                if message.startswith("INVOKED"):
                    run_timeline.setdefault(instance_name, {})['start_times'] = run_timeline.get(instance_name, {}).get(
                        'start_times', []) + [timestamp]
                elif message.startswith("EXECUTED"):
                    run_timeline.setdefault(instance_name, {})['end_times'] = run_timeline.get(instance_name, {}).get(
                        'end_times', []) + [timestamp]
                elif message.startswith("INVOKING_SUCCESSOR"):
                    taint_match = TAINT_PATTERN.search(message)
                    successor_match = SUCCESSOR_PATTERN.search(message)
                    if taint_match and successor_match:
                        taint, successor = taint_match.group(1), successor_match.group(1).split(':')[0]
                        invocations[taint] = {'start_time': timestamp, 'from': instance_name, 'to': successor}

            for taint, data in invocations.items():
                to_instance = data['to']
                if to_instance in run_timeline and 'start_times' in run_timeline[to_instance]:
                    # Find the earliest start time after the invocation start
                    for start_time in sorted(run_timeline[to_instance]['start_times']):
                        if start_time > data['start_time']:
                            delay = (start_time - data['start_time']).total_seconds()
                            if delay > 0:
                                run_timeline.setdefault(data['from'], {}).setdefault(f"delays_to_{to_instance}",
                                                                                     []).append(delay)
                                break

            critical_path = ['streaming', 'decode', 'recognition', 'consolidate']
            if all(k in run_timeline and 'start_times' in run_timeline[k] and 'end_times' in run_timeline[k] for k in
                   critical_path):
                e2e_time = (max(run_timeline['consolidate']['end_times']) - min(
                    run_timeline['streaming']['start_times'])).total_seconds()
                metrics['e2e_time'].append(e2e_time)

                metrics['streaming_exec_time'].append((max(run_timeline['streaming']['end_times']) - min(
                    run_timeline['streaming']['start_times'])).total_seconds())
                metrics['decode_exec_time'].append((max(run_timeline['decode']['end_times']) - min(
                    run_timeline['decode']['start_times'])).total_seconds())
                metrics['recognition_fanout_duration'].append((max(run_timeline['recognition']['end_times']) - min(
                    run_timeline['recognition']['start_times'])).total_seconds())
                metrics['consolidate_exec_time'].append((max(run_timeline['consolidate']['end_times']) - min(
                    run_timeline['consolidate']['start_times'])).total_seconds())

                if 'delays_to_decode' in run_timeline.get('streaming', {}): metrics['delay_streaming_to_decode'].extend(
                    run_timeline['streaming']['delays_to_decode'])
                if 'delays_to_recognition' in run_timeline.get('decode', {}): metrics[
                    'delay_decode_to_recognition'].extend(run_timeline['decode']['delays_to_recognition'])
                if 'delays_to_consolidate' in run_timeline.get('recognition', {}): metrics[
                    'delay_recognition_to_consolidate'].extend(run_timeline['recognition']['delays_to_consolidate'])

        if not metrics['e2e_time']:
            print("Found no complete, correlated workflow runs in this scenario.")
            continue

        median_results = {f'median_{key}': np.median(val) for key, val in metrics.items() if val}

        print(f"Results for '{name}':")
        print(f"  - Found {len(metrics['e2e_time'])} complete critical path runs.")
        print(f"  - Median End-to-End Time: {median_results.get('median_e2e_time', 0):.4f}s")

        all_scenario_results[name] = median_results

    if all_scenario_results:
        plot_results(all_scenario_results)


if __name__ == "__main__":
    analyze_scenarios()
