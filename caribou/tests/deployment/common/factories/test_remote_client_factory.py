import unittest
from unittest.mock import patch, MagicMock

from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.common.models.remote_client.gcp_remote_client import GCPRemoteClient
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory


class TestRemoteClientFactory(unittest.TestCase):
    def test_get_remote_client_aws(self):
        # Arrange
        provider = "aws"
        region = "region1"

        # Act
        remote_client = RemoteClientFactory.get_remote_client(provider, region)

        # Assert
        self.assertIsInstance(remote_client, AWSRemoteClient)

    def test_get_remote_client_unknown(self):
        # Arrange
        provider = "unknown"
        region = "region1"

        # Act & Assert
        with self.assertRaises(RuntimeError):
            RemoteClientFactory.get_remote_client(provider, region)

    @patch("caribou.common.models.remote_client.gcp_remote_client.GCPRemoteClient")
    def test_get_remote_client_gcp(self, mock_gcp_remote_client_class):
        # Arrange
        provider = "gcp"
        region = "region1"

        mock_instance = MagicMock(spec=GCPRemoteClient)
        mock_gcp_remote_client_class.return_value = mock_instance

        # Act & Assert
        remote_client = RemoteClientFactory.get_remote_client(provider, region)
        self.assertEqual(remote_client, mock_instance)
        mock_gcp_remote_client_class.assert_called_once_with(region=region)


if __name__ == "__main__":
    unittest.main()
