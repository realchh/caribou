from datetime import datetime
from typing import Any, Optional

from caribou.common.models.remote_client.remote_client import RemoteClient


class MockRemoteClient(RemoteClient):  # pylint: disable=too-many-public-methods
    def create_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        cpu: float | None = None,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        pass

    def resource_exists(self, resource):
        pass

    def get_current_provider_region(self) -> str:
        pass

    def create_role(self, role_name, policy, trust_policy):
        pass

    def update_role(self, role_name, policy, trust_policy):
        pass

    def send_message_to_messaging_service(self, identifier, message):
        pass

    def get_predecessor_data(self, current_instance_name, workflow_instance_id, consistent_read: bool = True):
        pass

    def upload_predecessor_data_at_sync_node(self, function_name, workflow_instance_id, message):
        pass

    def set_value_in_table(self, table_name, key, value, convert_to_bytes: bool = False):
        pass

    def get_value_from_table(self, table_name, key, consistent_read: bool = True):
        pass

    def upload_resource(self, key, resource):
        pass

    def download_resource(self, key):
        pass

    def update_function(
        self,
        function_name,
        role_identifier,
        zip_contents,
        runtime,
        handler,
        environment_variables,
        timeout,
        memory_size,
        cpu: float | None = None,
        additional_docker_commands: Optional[list[str]] = None,
    ):
        pass

    def get_key_present_in_table(self, table_name: str, key: str, consistent_read: bool = True) -> bool:
        pass

    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> list[bool]:
        pass

    def get_all_values_from_table(self, table_name: str) -> dict:
        pass

    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        pass

    def get_keys(self, table_name: str) -> list[str]:
        pass

    def remove_value_from_table(self, table_name: str, key: str) -> None:
        pass

    def create_sync_tables(self) -> None:
        pass

    def get_logs_since(self, function_instance: str, since: datetime) -> list[str]:
        pass

    def remove_key(self, table_name: str, key: str) -> None:
        pass

    def remove_function(self, function_name: str) -> None:
        pass

    def remove_role(self, role_name: str) -> None:
        pass

    def remove_messaging_topic(self, topic_identifier: str) -> None:
        pass

    def get_topic_identifier(self, topic_name: str) -> str:
        pass

    def remove_resource(self, key: str) -> None:
        pass

    def update_value_in_table(self, table_name: str, key: str, value: str, convert_to_bytes: bool = False) -> None:
        pass

    def get_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        pass

    def get_insights_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        pass

    def query_metric(
        self,
        revision_name: str,
        metric_type: str,
        start: datetime,
        end: datetime,
        aligner: str | None = None,
    ) -> float | None:
        pass

    def deploy_remote_cli(
        self,
        function_name: str,
        handler: str,
        role_arn: str,
        timeout: int,
        memory_size: int,
        ephemeral_storage: int,
        zip_contents: bytes,
        tmpdirname: str,
        env_vars: dict,
    ) -> None:
        pass

    def get_timer_rule_schedule_expression(self, rule_name: str) -> Optional[str]:
        pass

    def remove_timer_rule(self, lambda_function_name: str, rule_name: str) -> None:
        pass

    def create_timer_rule(
        self, lambda_function_name: str, schedule_expression: str, rule_name: str, event_payload: str
    ) -> None:
        pass

    def invoke_remote_framework_internal_action(self, action_type: str, action_events: dict[str, Any]) -> None:
        pass

    def invoke_remote_framework_with_payload(
        self, payload: dict[str, Any], invocation_type: str = "RequestResponse"
    ) -> None:
        pass

    def event_bridge_permission_exists(self, lambda_function_name: str, statement_id: str) -> bool:
        pass
