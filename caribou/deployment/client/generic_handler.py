import base64
import importlib
import json
import logging
from typing import Any, Dict, Optional, Union

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _parse_event(event: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Parse and normalize the event data."""
    if isinstance(event, str):
        try:
            # print(f"Parsing event string: {event}")
            return json.loads(event)
        except json.JSONDecodeError:
            return {"payload": event}
    return event


def _get_payload(event: Dict[str, Any]) -> Any:
    """Extract and parse the payload from the event."""
    payload = event.get("payload", {})
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload
    return payload


def _find_target_function(workflow: Any, target_name: Optional[str] = None) -> Any:
    """Find the target function in the workflow."""
    if target_name:
        for _, caribou_func in workflow.functions.items():
            if caribou_func.name == target_name:
                return caribou_func.wrapped_function

        raise ValueError(f"Function {target_name} not found in workflow")

    raise ValueError("Target function name not provided")


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    Generic Lambda handler that dynamically routes to the appropriate function based on the target in the payload.
    Handles both AWS (in direct invocations and SNS-triggered invocations) and GCP (Pub/Sub triggered invocations).

    The expected payload structure (after SNS or Pub/Sub unwrapping if needed):
    {
        "workflow_placement_decision": {...},
        "transmission_taint": "...",
        "number_of_hops_from_client_request": 1,
        "target": "function_name",  # Target is at root level
        "payload": {...}  # Payload is at root level
    }
    """
    try:
        # Check if event is a Flask Request object (for HTTP invocations)
        if hasattr(event, 'get_json'):
            # This is an HTTP request from Cloud Run
            # print("HTTP request detected")
            event = event.get_json()
            # print(f"Request JSON: {event}")
        if "Records" in event and len(event["Records"]) == 1 and "Sns" in event["Records"][0]:
            # Handle SNS-triggered invocations
            sns_message = event["Records"][0]["Sns"]["Message"]
            event = _parse_event(sns_message)
        elif "@type" in event and event["@type"] == "type.googleapis.com/google.pubsub.v1.PubsubMessage":
            # Handle Pub/Sub-triggered invocations
            data = base64.b64decode(event["data"]).decode("utf-8")
            # print(f"event before decoding {event}")
            event = _parse_event(data)
            # print(f"event after decoding {event}")
        else:
            # Handle direct invocations
            # print(f"Direct invocation event:")
            # print(f"Event type: {type(event)}")
            # print(f"Event keys: {event.keys() if isinstance(event, dict) else 'N/A'}")
            # print(f"Event content: {event}")
            event = _parse_event(event)
            # print(f"Parsed event: {event}")

        # Import and get workflow
        app = importlib.import_module("app")
        workflow = app.workflow

        # # Get payload and target function
        # payload = _get_payload(event)
        target_function_name = event.get("target") if isinstance(event, dict) else None
        target_function = _find_target_function(workflow, target_function_name)

        # Call the target function
        result = target_function(event)

        return {"statusCode": 200, "body": json.dumps(result)}

    except (ValueError, json.JSONDecodeError, ImportError) as e:
        logger.error("Error in generic handler: %s", str(e), exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def http_handler(request):
    """
    HTTP handler for Cloud Functions with --signature-type http.
    Wraps the Flask Request object and calls the lambda_handler.
    """
    try:
        # Extract JSON data from the request
        request_json = request.get_json(silent=True)
        
        if request_json is None:
            # Try to get data as text
            request_data = request.get_data(as_text=True)
            print(f"Request data as text: {request_data}")
            if request_data:
                try:
                    request_json = json.loads(request_data)
                except json.JSONDecodeError:
                    request_json = {"payload": request_data}
            else:
                request_json = {}
        
        print(f"HTTP handler received: {request_json}")
        
        # Call the lambda_handler with the parsed JSON
        result = lambda_handler(request_json, None)
        
        # Return the response
        if isinstance(result, dict) and "body" in result:
            return result["body"], result.get("statusCode", 200)
        return json.dumps(result), 200
        
    except Exception as e:
        logger.error("Error in HTTP handler: %s", str(e), exc_info=True)
        return json.dumps({"error": str(e)}), 500
