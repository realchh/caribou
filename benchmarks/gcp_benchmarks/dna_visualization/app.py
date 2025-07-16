from typing import Any

from caribou.deployment.client import CaribouWorkflow
from dna_features_viewer import BiopythonTranslator
import json
import matplotlib.pyplot as plt
import uuid
import os
import google.cloud.storage as gcs


# Change the following bucket name and region to match your setup
gcp_bucket_name = "caribou-dna-visualization"

workflow = CaribouWorkflow(name="dna_visualization", version="0.0.1")


@workflow.serverless_function(
    name="visualize",
    entry_point=True,
)
def visualize(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "gen_file_name" in event:
        gen_file_name = event["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")
    req_id = uuid.uuid4()

    local_gen_filename = f"/tmp/genbank-{req_id}.gb"
    local_result_filename = f"/tmp/result-{req_id}.png"

    gcs_client = gcs.Client()
    bucket = gcs_client.bucket(gcp_bucket_name)

    source_blob_name = f"genbank/{gen_file_name}"
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(local_gen_filename)

    graphic_record = BiopythonTranslator().translate_record(local_gen_filename)
    ax, _ = graphic_record.plot(figure_width=10, strand_in_label_threshold=7)
    ax.figure.tight_layout()
    ax.figure.savefig(local_result_filename)

    # Close the figure to free up memory
    plt.close(ax.figure)

    destination_blob_name = f"result/{gen_file_name}.png"
    blob_to_upload = bucket.blob(destination_blob_name)
    blob_to_upload.upload_from_filename(local_result_filename)

    os.remove(local_gen_filename)
    os.remove(local_result_filename)

    return {"status": 200}