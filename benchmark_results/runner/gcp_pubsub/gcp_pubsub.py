import subprocess
import time
import json
import sys
import caribou.endpoint.client as client

# --- Configuration ---
WORKFLOW_ID = "pubsub_testing-0.0.4"
ARGUMENT_PAYLOAD = '{"message_size": "large"}'
INTERVAL_SECONDS = 10
max_counter = 250
RUN_DURATION_MINUTES = max_counter * INTERVAL_SECONDS / 60
# -------------------

if __name__ == "__main__":
    start_time = time.time()
    end_time = start_time + RUN_DURATION_MINUTES * 60

    print(
        f"Starting workflow runner. Will run '{WORKFLOW_ID}' every {INTERVAL_SECONDS} seconds for {RUN_DURATION_MINUTES} minutes.")
    print("Press Ctrl+C to stop early.")

    caribou_client = client.Client(WORKFLOW_ID)

    counter = 0

    with open('http_gcp_pubsub_large_0_1_rps.txt', 'a') as f:
        while counter < max_counter:
            f.write(f"--- {time.ctime()} ---")
            f.write(f"Executing command: {ARGUMENT_PAYLOAD}")
            print(f"--- {time.ctime()} ---")
            print(f"Executing command: {ARGUMENT_PAYLOAD}")

            try:
                # Execute the command
                result = caribou_client.run(ARGUMENT_PAYLOAD)
                f.write("--- Command Successful ---")
                print("--- Command Successful ---")

            except FileNotFoundError:
                print("\n[ERROR] 'poetry' or 'caribou' command not found.")
                print("Please make sure you are in the correct project directory and have run 'poetry install'.")
                sys.exit(1)
            except Exception as e:
                print(f"\nAn unexpected error occurred: {e}")

            f.write(f"\nWaiting for {INTERVAL_SECONDS} seconds...")
            print(f"\nWaiting for {INTERVAL_SECONDS} seconds...")
            counter += 1
            time.sleep(INTERVAL_SECONDS)

        f.write("\nTime limit reached. Not starting another run.")
        print("\nTime limit reached. Not starting another run.")

        f.write(f"\nScript finished after {RUN_DURATION_MINUTES} minutes and {counter} iterations.")
        print(f"\nScript finished after {RUN_DURATION_MINUTES} minutes and {counter} iterations.")

