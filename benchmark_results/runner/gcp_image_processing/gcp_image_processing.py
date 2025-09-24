import subprocess
import time
import json
import sys
import caribou.endpoint.client as client

# --- Configuration ---
WORKFLOW_ID = "image_processing-0.0.1"
ARGUMENT_PAYLOAD = '{"image_name": "image_name.jpg", "desired_transformations": ["flip"]}'
INTERVAL_SECONDS = 1
RUN_DURATION_MINUTES = 10
# -------------------

if __name__ == "__main__":
    start_time = time.time()
    end_time = start_time + RUN_DURATION_MINUTES * 60

    print(
        f"Starting workflow runner. Will run '{WORKFLOW_ID}' every {INTERVAL_SECONDS} seconds for {RUN_DURATION_MINUTES} minutes.")
    print("Press Ctrl+C to stop early.")

    caribou_client = client.Client(WORKFLOW_ID)

    max_counter = RUN_DURATION_MINUTES * 60 / INTERVAL_SECONDS
    counter = 0

    while counter < max_counter:
        print(f"--- {time.ctime()} ---")
        print(f"Executing command: {ARGUMENT_PAYLOAD}")

        try:
            # Execute the command
            result = caribou_client.run(ARGUMENT_PAYLOAD)

            print("--- Command Successful ---")

        except FileNotFoundError:
            print("\n[ERROR] 'poetry' or 'caribou' command not found.")
            print("Please make sure you are in the correct project directory and have run 'poetry install'.")
            sys.exit(1)
        except Exception as e:
            print(f"\nAn unexpected error occurred: {e}")

        print(f"\nWaiting for {INTERVAL_SECONDS} seconds...")
        counter += 1
        time.sleep(INTERVAL_SECONDS)

    print("\nTime limit reached. Not starting another run.")

    print(f"\nScript finished after {RUN_DURATION_MINUTES} minutes and {counter} iterations.")

