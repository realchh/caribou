from typing import Any
import markdown
import base64
import boto3
import json
from tempfile import TemporaryDirectory
from caribou.deployment.client import CaribouWorkflow
from google.cloud import storage

gcs_bucket_name = "caribou-markdown-to-html"

workflow = CaribouWorkflow(name="markdown_to_html", version="0.0.1")

@workflow.serverless_function(
    name="markdown_to_html",
    entry_point=True,
)
def markdown_to_html(event: dict[str, Any]) -> dict[str, Any]:

    if isinstance(event, str):
        event = json.loads(event)

    if "filename" in event:
        filename = event["filename"]
    else:
        raise ValueError("No filename provided")

    client = storage.Client()
    
    with TemporaryDirectory() as tmp_dir:
        bucket = client.bucket(gcs_bucket_name)
        blob = bucket.blob(filename)
        blob.download_to_filename(f"{tmp_dir}/{filename}")

        with open(f"{tmp_dir}/{filename}", "r") as f:
            markdown_text = f.read()
        
        decoded_text = base64.b64decode(markdown_text).decode()
        html_text = markdown.markdown(decoded_text)

        with open(f"{tmp_dir}/{filename}.html", "w") as f:
            f.write(html_text)
        
        blob = bucket.blob(f"output/{filename}.html")
        blob.upload_from_filename(f"{tmp_dir}/{filename}.html")

    return {"status": 200}