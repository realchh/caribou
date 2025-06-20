import os

from caribou.common.constants import GLOBAL_GCP_SYSTEM_REGION, GLOBAL_SYSTEM_REGION, INTEGRATION_TEST_SYSTEM_REGION
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
from caribou.common.provider import Provider
from caribou.common.utils import str_to_bool


class Endpoints:  # pylint: disable=too-many-instance-attributes
    def __init__(self) -> None:
        integration_test_on = str_to_bool(os.environ.get("INTEGRATIONTEST_ON", "False"))
        self._provider = os.environ.get("CARIBOU_DEFAULT_PROVIDER", Provider.AWS.value)

        if integration_test_on:
            self._provider = Provider.INTEGRATION_TEST_PROVIDER.value
            global_system_region = INTEGRATION_TEST_SYSTEM_REGION
        else:
            if self._provider == Provider.GCP.value:
                global_system_region = GLOBAL_GCP_SYSTEM_REGION
            elif self._provider == Provider.AWS.value:
                global_system_region = GLOBAL_SYSTEM_REGION
            else:
                raise ValueError(f"Unknown provider {self._provider}")

        # TODO (#56): Implement retrieval of deployer server and update checker regions
        self._deployment_server_region = global_system_region
        self._deployment_resources_client: RemoteClient | None = None

        self._deployment_optimizaion_monitor_region = global_system_region
        self._deployment_manager_client: RemoteClient | None = None

        self._deployment_algorithm_workflow_placement_decision_region = global_system_region
        self._deployment_algorithm_workflow_placement_decision_client: RemoteClient | None = None

        self._data_collector_region = global_system_region
        self._data_collector_client: RemoteClient | None = None

        self._data_store_region = global_system_region
        self._data_store_client: RemoteClient | None = None

        self._framework_cli_remote_client = RemoteClientFactory.get_framework_cli_remote_client(GLOBAL_SYSTEM_REGION)

    def get_deployment_resources_client(self) -> RemoteClient:
        if self._deployment_resources_client is None:
            self._deployment_resources_client = RemoteClientFactory.get_remote_client(
                self._provider, self._deployment_server_region
            )
        return self._deployment_resources_client

    def get_deployment_manager_client(self) -> RemoteClient:
        if self._deployment_manager_client is None:
            self._deployment_manager_client = RemoteClientFactory.get_remote_client(
                self._provider, self._deployment_optimizaion_monitor_region
            )
        return self._deployment_manager_client

    def get_deployment_algorithm_workflow_placement_decision_client(self) -> RemoteClient:
        if self._deployment_algorithm_workflow_placement_decision_client is None:
            self._deployment_algorithm_workflow_placement_decision_client = RemoteClientFactory.get_remote_client(
                self._provider, self._deployment_algorithm_workflow_placement_decision_region
            )
        return self._deployment_algorithm_workflow_placement_decision_client

    def get_data_collector_client(self) -> RemoteClient:
        if self._data_collector_client is None:
            self._data_collector_client = RemoteClientFactory.get_remote_client(
                self._provider, self._data_collector_region
            )
        return self._data_collector_client

    def get_datastore_client(self) -> RemoteClient:
        if self._data_store_client is None:
            self._data_store_client = RemoteClientFactory.get_remote_client(self._provider, self._data_store_region)
        return self._data_store_client

    def get_framework_cli_remote_client(self) -> AWSRemoteClient:
        return self._framework_cli_remote_client
