from typing import Any

import json

import google
from dna_features_viewer import BiopythonTranslator
import matplotlib.pyplot as plt
import uuid
import os
import google.cloud.storage as gcs
from google.cloud.functions import context
import base64

# Change the following bucket name and region to match your setup
gcp_bucket_name = "caribou-dna-visualization-naufal"

# workflow = CaribouWorkflow(name="dna_visualization", version="0.0.1")


# @workflow.serverless_function(
#     name="visualize",
#     entry_point=True,
# )
def visualize(event: dict, context: google.cloud.functions.context) -> None:
    # temporary
    # if isinstance(event, str):
    #     event = json.loads(event)
    #
    # if "gen_file_name" in event:
    #     gen_file_name = event["gen_file_name"]
    # else:
    #     raise ValueError("No gen_file_name provided")
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    payload = json.loads(pubsub_message)

    gen_file_name = payload["gen_file_name"]
    if not gen_file_name:
        raise ValueError("No gen_file_name provided")

    req_id = uuid.uuid4()

    local_gen_filename = f"/tmp/genbank-{req_id}.gb"
    local_result_filename = f"/tmp/result-{req_id}.png"

    gcs_client = gcs.Client()
    bucket = gcs_client.bucket(gcp_bucket_name)

    source_blob_name = f"genbank/{gen_file_name}"
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(local_gen_filename)
    print(f"Downloaded object genbank/{gen_file_name} from bucket {gcp_bucket_name} to local file {local_gen_filename}")

    graphic_record = BiopythonTranslator().translate_record(local_gen_filename)
    ax, _ = graphic_record.plot(figure_width=10, strand_in_label_threshold=7)
    ax.figure.tight_layout()
    ax.figure.savefig(local_result_filename)

    # Close the figure to free up memory
    plt.close(ax.figure)

    """
    Required perms: storage.objects.create
    storage.objects.delete
        This permission is only required for uploads that overwrite an existing object.
    storage.objects.get
        This permission is only required if you plan on using the Google Cloud CLI to perform the tasks on this page.
    storage.objects.list
        This permission is only required if you plan on using the Google Cloud CLI to perform the tasks on this page.
        This permission is also required if you want to use the Google Cloud console to verify the objects you've uploaded.
    """
    destination_blob_name = f"result/{gen_file_name}.png"
    blob_to_upload = bucket.blob(destination_blob_name)
    blob_to_upload.upload_from_filename(local_result_filename)
    print(f"Uploaded {local_result_filename} to gs://{gcp_bucket_name}/{destination_blob_name}")

    os.remove(local_gen_filename)
    os.remove(local_result_filename)

    # return {"status": 200}