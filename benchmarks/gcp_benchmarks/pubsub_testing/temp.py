import base64

from benchmarks.gcp_benchmarks.pubsub_testing.src.large import large_input

large = "{'workflow_placement_decision': {'instances': {'get_requests:entry_point:0': {'instance_name': 'get_requests:entry_point:0', 'regions_and_providers': {'allowed_regions': [{'provider': 'gcp', 'region': 'us-east1'}, {'provider': 'gcp', 'region': 'us-west1'}, {'provider': 'gcp', 'region': 'us-west2'}, {'provider': 'gcp', 'region': 'northamerica-northeast1'}], 'disallowed_regions': None, 'providers': {'gcp': {'config': {'timeout': 120, 'memory': 1024, 'vcpu': 1.0, 'concurrency': 80}}}}, 'succeeding_instances': ['destination:get_requests_0_0:1'], 'preceding_instances': [], 'dependent_sync_predecessors': []}, 'destination:get_requests_0_0:1': {'instance_name': 'destination:get_requests_0_0:1', 'regions_and_providers': {'allowed_regions': [{'provider': 'gcp', 'region': 'us-east1'}, {'provider': 'gcp', 'region': 'us-west1'}, {'provider': 'gcp', 'region': 'us-west2'}, {'provider': 'gcp', 'region': 'northamerica-northeast1'}], 'disallowed_regions': None, 'providers': {'gcp': {'config': {'timeout': 120, 'memory': 1024, 'vcpu': 1.0, 'concurrency': 80}}}}, 'succeeding_instances': [], 'preceding_instances': ['get_requests:entry_point:0'], 'dependent_sync_predecessors': []}}, 'current_instance_name': 'destination:get_requests_0_0:1', 'workflow_placement': {'home_deployment': {'get_requests:entry_point:0': {'identifier': 'projects/caribou-460422/topics/pubs-ting-0-0-1-get-ests-gcp-us-ea1-97b98094d0a0_messaging_topic', 'provider_region': {'provider': 'gcp', 'region': 'us-east1'}, 'function_identifier': 'https://pubs-ting-0-0-1-get-ests-gcp-us-ea1-97b98094d0a0-es6wskpkmq-ue.a.run.app'}, 'destination:get_requests_0_0:1': {'identifier': 'projects/caribou-460422/topics/pubs-ting-0-0-1-dest-tion-gcp-us-ea1-3c3c79569ac3_messaging_topic', 'provider_region': {'provider': 'gcp', 'region': 'us-east1'}, 'function_identifier': 'https://pubs-ting-0-0-1-dest-tion-gcp-us-ea1-3c3c79569ac3-es6wskpkmq-ue.a.run.app'}}}, 'time_key': 'N/A', 'send_to_home_region': False, 'run_id': 'e28a457288cd42ffa7bff5ae8a0805b8', 'data_size': 1.751817762851715e-06, 'consumed_read_capacity': 1.0}, 'transmission_taint': 'c582b1fb7ed6481f931573ead4aeba47', 'number_of_hops_from_client_request': 1, 'target': 'destination', 'payload': {'message_size': 'small', 'message': "


if __name__ == "__main__":
    large = large + large_input + '}"}}'
    data = large.encode("utf-8")
    print(len(data))
    encoded = base64.b64encode(data)
    len = len(encoded)
    print(len)