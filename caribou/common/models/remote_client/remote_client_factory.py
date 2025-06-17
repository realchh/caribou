from __future__ import annotations

from typing import TYPE_CHECKING

from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.provider import Provider

if TYPE_CHECKING:
    from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
    from caribou.common.models.remote_client.gcp_remote_client import GCPRemoteClient


class RemoteClientFactory:
    @staticmethod
    def get_remote_client(provider: str, region: str) -> RemoteClient:
        try:
            provider_enum = Provider(provider)
        except ValueError as e:
            raise RuntimeError(f"Unknown provider {provider}") from e
        if provider_enum == Provider.AWS:
            from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient

            return AWSRemoteClient(region)
        if provider_enum == Provider.GCP:
            from caribou.common.models.remote_client.gcp_remote_client import GCPRemoteClient

            return GCPRemoteClient(region=region)
        if provider_enum in [Provider.TEST_PROVIDER1, Provider.TEST_PROVIDER2]:
            from caribou.common.models.remote_client.mock_remote_client import MockRemoteClient

            return MockRemoteClient()
        if provider_enum == Provider.INTEGRATION_TEST_PROVIDER:
            from caribou.common.models.remote_client.integration_test_remote_client import IntegrationTestRemoteClient

            return IntegrationTestRemoteClient()
        raise RuntimeError(f"Unknown provider {provider}")

    @staticmethod
    def get_framework_cli_remote_client(region: str) -> AWSRemoteClient:
        from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient

        return AWSRemoteClient(region)
