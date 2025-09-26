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

# Define the service names for the Text-2-Speech Censoring workflow
SERVICE_NAMES = {
    "get_input": "text-ring-0-0-1-get-nput-gcp-us-ea1-67144d27ac10",
    "text_2_speech": "text-ring-0-0-1-text-eech-gcp-us-ea1-6ab45e709401",
    "profanity": "text-ring-0-0-1-prof-nity-gcp-us-ea1-f5969189176b",
    "encoding": "text-ring-0-0-1-enco-ding-gcp-us-ea1-1e6e39dfc626",
    "censor": "text-ring-0-0-1-cens-nsor-gcp-us-ea1-1d11873b132c",
}

# Define the UTC time windows for each benchmark scenario
# Note: Assuming the year is 2025 based on previous logs.
SCENARIOS = {
    "Low (0.1 rps)": {
        "start_time": datetime(2025, 9, 25, 16, 23, 51, tzinfo=timezone.utc),
        "end_time": datetime(2025, 9, 25, 17, 00, 50, tzinfo=timezone.utc),
    },
    "Medium (1 rps)": {
        "start_time": datetime(2025, 9, 25, 14, 49, 30, tzinfo=timezone.utc),
        "end_time": datetime(2025, 9, 25, 15, 0, 3, tzinfo=timezone.utc),
    },
    # "High (5 rps)": {
    #     "start_time": datetime(2025, 9, 25, 0, 3, 00, tzinfo=timezone.utc),
    #     "end_time": datetime(2025, 9, 25, 0, 29, 36, tzinfo=timezone.utc),
    # },
}
# -------------------


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
        # Convert pages to a single list
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

    # Data for the critical path: get-nput -> text-eech -> enco-ding -> cens-nsor
    get_input_times = [results[label].get('avg_get_input_exec_time', 0) for label in labels]
    delay1 = [results[label].get('avg_delay_get_input_to_text_2_speech', 0) for label in labels]
    text_2_speech_times = [results[label].get('avg_text_2_speech_exec_time', 0) for label in labels]
    delay2 = [results[label].get('avg_delay_text_2_speech_to_encoding', 0) for label in labels]
    encoding_times = [results[label].get('avg_encoding_exec_time', 0) for label in labels]
    delay3 = [results[label].get('avg_delay_encoding_to_censor', 0) for label in labels]
    censor_times = [results[label].get('avg_censor_exec_time', 0) for label in labels]

    fig, ax = plt.subplots(figsize=(16, 8))

    lefts = np.zeros(len(labels))
    components = [
        (get_input_times, 'get_input'),
        (delay1, 'Pub/Sub Delay'),
        (text_2_speech_times, 'text_2_speech'),
        (delay2, 'Pub/Sub Delay'),
        (encoding_times, 'encoding'),
        (delay3, 'Pub/Sub Delay'),
        (censor_times, 'censor'),
    ]

    for data, label in components:
        bars = ax.barh(labels, data, height=0.5, left=lefts, label=label)
        for i, bar in enumerate(bars):
            width = bar.get_width()
            if width > 0.05:  # Don't label very small segments
                ax.text(lefts[i] + width / 2, bar.get_y() + bar.get_height() / 2,
                        f'{width:.3f}s', ha='center', va='center', color='white', weight='bold')
        lefts += np.array(data)

    ax.set_xlabel('Time (seconds)')
    ax.set_title('Critical Path Latency Breakdown by Invocation Rate')
    ax.legend(loc='lower right')
    ax.invert_yaxis()

    for i, total in enumerate(lefts):
        ax.text(total + 0.1, i, f'Total: {total:.3f}s', ha='left', va='center', weight='bold')

    plt.tight_layout()
    filename = "no5_t2s_workflow_latency_breakdown.png"
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

        # --- REFACTORED LOGIC ---
        # First Pass: Group all events by their run_id
        run_events = defaultdict(list)
        for logs in all_logs.values():
            for log in logs:
                parsed = parse_caribou_log(log)
                if not parsed: continue
                timestamp, run_id, message = parsed
                run_events[run_id].append({'timestamp': timestamp, 'message': message})

        # Second Pass: Process each complete run
        metrics = defaultdict(list)
        for run_id, events in run_events.items():
            # Sort events by timestamp to process them in order
            events.sort(key=lambda x: x['timestamp'])

            # Data structure to hold timing info for this specific run
            run_timeline = {}
            invocations = {}  # Taint -> {start_time, from_instance, to_instance}

            for event in events:
                timestamp = event['timestamp']
                message = event['message']

                instance_match = INSTANCE_PATTERN.search(message)
                if not instance_match: continue
                instance_name = instance_match.group(1).split(':')[0]

                if message.startswith("INVOKED"):
                    run_timeline.setdefault(instance_name, {})['start_time'] = timestamp
                elif message.startswith("EXECUTED"):
                    run_timeline.setdefault(instance_name, {})['end_time'] = timestamp
                elif message.startswith("INVOKING_SUCCESSOR"):
                    taint_match = TAINT_PATTERN.search(message)
                    successor_match = SUCCESSOR_PATTERN.search(message)
                    if taint_match and successor_match:
                        taint = taint_match.group(1)
                        successor = successor_match.group(1).split(':')[0]
                        invocations[taint] = {'start_time': timestamp, 'from': instance_name, 'to': successor}

            # Correlate invocations to find delays
            for taint, data in invocations.items():
                to_instance = data['to']
                if to_instance in run_timeline and 'start_time' in run_timeline[to_instance]:
                    delay = (run_timeline[to_instance]['start_time'] - data['start_time']).total_seconds()
                    if delay > 0:
                        run_timeline[data['from']][f"delay_to_{to_instance}"] = delay

            # Now, check if this run is complete and calculate its metrics
            critical_path = ['get_input', 'text_2_speech', 'encoding', 'censor']
            if all(k in run_timeline and 'start_time' in run_timeline[k] and 'end_time' in run_timeline[k] for k in
                   critical_path):
                # Calculate E2E latency
                e2e_time = (run_timeline['censor']['end_time'] - run_timeline['get_input'][
                    'start_time']).total_seconds()
                metrics['e2e_time'].append(e2e_time)

                # Calculate individual execution times
                for inst in critical_path:
                    metrics[f'{inst}_exec_time'].append(
                        (run_timeline[inst]['end_time'] - run_timeline[inst]['start_time']).total_seconds())
                if 'profanity' in run_timeline and 'end_time' in run_timeline['profanity']:
                    metrics['profanity_exec_time'].append((run_timeline['profanity']['end_time'] -
                                                           run_timeline['profanity']['start_time']).total_seconds())

                # Calculate delays from the timeline
                if 'delay_to_text_2_speech' in run_timeline.get('get_input', {}): metrics[
                    'delay_get_input_to_text_2_speech'].append(run_timeline['get_input']['delay_to_text_2_speech'])
                if 'delay_to_encoding' in run_timeline.get('text_2_speech', {}): metrics[
                    'delay_text_2_speech_to_encoding'].append(run_timeline['text_2_speech']['delay_to_encoding'])
                if 'delay_to_censor' in run_timeline.get('encoding', {}): metrics['delay_encoding_to_censor'].append(
                    run_timeline['encoding']['delay_to_censor'])

        if not metrics['e2e_time']:
            print("Found no complete, correlated workflow runs in this scenario.")
            continue

        avg_results = {f'avg_{key}': np.quantile(val, 0.25) for key, val in metrics.items()}

        print(f"Results for '{name}':")
        print(f"  - Found {len(metrics['e2e_time'])} complete critical path runs.")
        print(f"  - Average End-to-End Time: {avg_results.get('avg_e2e_time', 0):.4f}s")
        print(
            f"  - Average latency of parallel 'profanity' branch: {avg_results.get('avg_profanity_exec_time', 0):.4f}s")

        all_scenario_results[name] = avg_results

    # --- Plotting ---
    if all_scenario_results:
        plot_results(all_scenario_results)


if __name__ == "__main__":
    analyze_scenarios()

