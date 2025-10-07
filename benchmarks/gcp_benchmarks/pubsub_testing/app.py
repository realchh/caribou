from typing import Any

import json

from src.small import small_input
from src.large import large_input
from caribou.deployment.client import CaribouWorkflow
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Change the following bucket name and region to match your setup
workflow = CaribouWorkflow(name="pubsub_testing", version="0.0.1")

@workflow.serverless_function(
    name="get_requests",
    entry_point=True,
)
def get_requests(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "message_size" in event:
        message_size: str = event["message_size"]
    else:
        raise ValueError("No message size provided")

    if "small" in message_size.lower():
        message: str = small_input
    elif "large" in message_size.lower():
        message: str = large_input
    else:
        raise ValueError("No message size provided")

    payload = {
        "message_size": message_size,
        "message": message,
    }

    workflow.invoke_serverless_function(destination, payload)

    return {"status": 200}

@workflow.serverless_function(
    name="destination",
)
def destination(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "message_size" in event:
        message_size = event["message_size"]
    else:
        raise ValueError("No message size provided")

    if "message" in event:
        message = event["message"]
    else:
        raise ValueError("No message provided")

    print(f"  [INFO] event: {event}")

    return {"status": 200}