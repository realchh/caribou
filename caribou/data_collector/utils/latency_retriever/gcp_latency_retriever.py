from google.cloud import monitoring_v3
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import statistics # For calculating median

# --- Configuration (Adapt these values) ---
PROJECT_ID = "caribou-460422"  # Replace with your Project ID
# This project is used for billing/quota for the API call

# Global metric for median RTT between zone pairs across all GCP projects
GLOBAL_ZONE_PAIR_RTT_METRIC = "networking.googleapis.com/all_gcp/vm_traffic/zone_pair_median_rtt"

# Define the time window for the query as the last 30 minutes for recent data
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(hours=1)

def get_zone_from_resource_name(resource_name: str) -> str:
    """Extracts zone name from a resource name like 'projects/PROJECT_NUM/locations/ZONE_NAME'"""
    try:
        return resource_name.split('/')[-1]
    except Exception:
        return "unknown_zone"

def get_region_from_zone(zone_name: str) -> str:
    """Extracts region from a zone name (e.g., 'us-central1-a' -> 'us-central1')."""
    if zone_name == "unknown_zone" or not zone_name:
        return "unknown_region"
    parts = zone_name.split('-')
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return zone_name # Or handle as an error/unknown

def get_global_inter_region_latency(project_id: str) -> list:
    """
    Fetches global zone-pair median RTT and aggregates to inter-region median RTT.

    Args:
        project_id: Your Google Cloud Project ID (for API quota/billing).

    Returns:
        A list of dictionaries, where each dictionary represents
        the aggregated median latency between two regions.
    """
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    # This will hold lists of median RTTs for zone pairs belonging to each region pair
    # Structure: {(source_region, dest_region): [rtt1, rtt2, ...]}
    region_pair_latencies_raw = defaultdict(list)
    processed_zone_pairs = set() # To avoid double counting if multiple points for same pair

    filter_str = f'metric.type = "{GLOBAL_ZONE_PAIR_RTT_METRIC}"'

    interval = monitoring_v3.types.TimeInterval()
    interval.start_time=start_time
    interval.end_time=end_time

    print(f"Querying metric: {GLOBAL_ZONE_PAIR_RTT_METRIC}")
    print(f"Time window: {start_time.isoformat()}Z to {end_time.isoformat()}Z")

    try:
        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.types.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        series_count = 0
        for series in results:
            series_count += 1
            # For `networking.googleapis.com/Location` as resource type, the resource labels often contain the location info.
            # For `networking.googleapis.com/all_gcp/vm_traffic/zone_pair_median_rtt`,
            # the metric labels themselves should define the pair.
            source_zone = series.resource.labels.get("location", "unknown_zone")
            dest_zone = series.metric.labels.get("remote_zone", "unknown_zone")

            # If the zones are unknown, print a warning and skip
            if source_zone == "unknown_zone" or dest_zone == "unknown_zone":
                print(f"Warning: Could not determine source/destination zone for series. Metric Labels: {series.metric.labels}, Resource Labels: {series.resource.labels}")
                continue

            source_region = get_region_from_zone(source_zone)
            dest_region = get_region_from_zone(dest_zone)

            if source_region == "unknown_region" or dest_region == "unknown_region" or source_region == dest_region:
                continue # Skip intra-region or unknown

            # Get the most recent point for this zone pair within the interval
            if series.points:
                point = series.points[0] # Assuming latest point if multiple, or that there's one point
                latency_ns = point.value.double_value
                latency_ms = latency_ns/1000000

                # Avoid processing the same zone pair multiple times if query returns overlapping data
                zone_pair_key = tuple(sorted((source_zone, dest_zone)))
                if zone_pair_key not in processed_zone_pairs:
                    region_pair_latencies_raw[(source_region, dest_region)].append(latency_ms)
                    processed_zone_pairs.add(zone_pair_key)

        print(f"Processed {series_count} time series entries.")

    except Exception as e:
        print(f"An error occurred: {e}")
        return []

    # Aggregate the collected zone-pair medians to get a region-pair median
    aggregated_region_latency_list = []
    for (src_reg, dst_reg), latencies in region_pair_latencies_raw.items():
        if latencies:
            median_latency_for_region_pair = statistics.median(latencies)
            aggregated_region_latency_list.append({
                "source_region": src_reg,
                "destination_region": dst_reg,
                "median_rtt_ms": round(median_latency_for_region_pair, 3),
                "contributing_zone_pairs": len(latencies)
            })

    print("latency list: \n", aggregated_region_latency_list)
    return aggregated_region_latency_list

if __name__ == "__main__":
    if PROJECT_ID == "***":
        print("Please update the PROJECT_ID variable with your Google Cloud Project ID.")
    else:
        print(f"Fetching global inter-region latency data (API access billed to project: {PROJECT_ID})")
        latencies = get_global_inter_region_latency(PROJECT_ID)

        if latencies:
            print(f"\n--- Aggregated Inter-Region Median Latencies ---")
            # Sort for consistent output
            latencies.sort(key=lambda x: (x["source_region"], x["destination_region"]))
            for entry in latencies:
                print(f"From {entry['source_region']} to {entry['destination_region']}: {entry['median_rtt_ms']} ms (from {entry['contributing_zone_pairs']} zone-pair medians)")
        else:
            print("No inter-region latency data retrieved or an error occurred. \n"
                  "Possible reasons:\n"
                  "- The metric name or labels used for parsing are incorrect (inspect raw 'series' object).\n"
                  "- No data available for the specified time window for this global metric.\n"
                  "- IAM permissions missing for the project to make Monitoring API calls."
                  )

    print(f"\nReminder: For project-specific VM-to-VM latency, use 'networking.googleapis.com/vm_flow/rtt' with resource type 'gce_instance'. That metric is a distribution.")
