import os
import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from caribou.common import constants
from caribou.common.provider import Provider
from google.api_core import exceptions as google_api_exceptions

from caribou.common.teardown.teardown_tables import (
    remove_aws_table,
    remove_bucket,
    teardown_framework_tables,
    teardown_framework_buckets,
    remove_sync_tables_all_regions,
    remove_gcp_collection,
)


class TestTeardownTables(unittest.TestCase):
    @patch("boto3.client")
    def test_remove_aws_table_exists(self, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb

        remove_aws_table(mock_dynamodb, "test_table")

        mock_dynamodb.describe_table.assert_called_once_with(TableName="test_table")
        mock_dynamodb.delete_table.assert_called_once_with(TableName="test_table")

    @patch("boto3.client")
    def test_remove_aws_table_not_exists(self, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_dynamodb.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "describe_table"
        )

        remove_aws_table(mock_dynamodb, "test_table")

        mock_dynamodb.describe_table.assert_called_once_with(TableName="test_table")
        mock_dynamodb.delete_table.assert_not_called()

    @patch("boto3.client")
    def test_remove_aws_table_other_error(self, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_dynamodb.describe_table.side_effect = ClientError(
            {"Error": {"Code": "SomeOtherException"}}, "describe_table"
        )

        with self.assertRaises(ClientError):
            remove_aws_table(mock_dynamodb, "test_table")

    @patch("boto3.client")
    @patch("boto3.resource")
    def test_remove_bucket_exists(self, mock_boto_resource, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3_resource = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_boto_resource.return_value = mock_s3_resource

        remove_bucket(mock_s3, mock_s3_resource, "test_bucket")

        mock_s3.head_bucket.assert_called_once_with(Bucket="test_bucket")
        mock_s3_resource.Bucket.assert_called_once_with("test_bucket")
        mock_s3_resource.Bucket().objects.all().delete.assert_called_once()
        mock_s3.delete_bucket.assert_called_once_with(Bucket="test_bucket")

    @patch("boto3.client")
    @patch("boto3.resource")
    def test_remove_bucket_not_exists(self, mock_boto_resource, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3_resource = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_boto_resource.return_value = mock_s3_resource
        mock_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "head_bucket")

        remove_bucket(mock_s3, mock_s3_resource, "test_bucket")

        mock_s3.head_bucket.assert_called_once_with(Bucket="test_bucket")
        mock_s3_resource.Bucket.assert_not_called()
        mock_s3.delete_bucket.assert_not_called()

    @patch("boto3.client")
    @patch("boto3.resource")
    def test_remove_bucket_other_error(self, mock_boto_resource, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3_resource = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_boto_resource.return_value = mock_s3_resource
        mock_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "SomeOtherException"}}, "head_bucket")

        with self.assertRaises(ClientError):
            remove_bucket(mock_s3, mock_s3_resource, "test_bucket")

    @patch("boto3.client")
    @patch("caribou.common.teardown.teardown_tables.constants")
    def test_teardown_framework_tables(self, mock_constants, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_constants.GLOBAL_SYSTEM_REGION = "us-west-2"
        mock_constants.SYNC_MESSAGES_TABLE = "sync_messages"
        mock_constants.SYNC_PREDECESSOR_COUNTER_TABLE = "sync_predecessor_counter"
        mock_constants.TEST_TABLE = "test_table"

        teardown_framework_tables(Provider.AWS.value)

        mock_dynamodb.describe_table.assert_called_once_with(TableName="test_table")
        mock_dynamodb.delete_table.assert_called_once_with(TableName="test_table")

    @patch("boto3.client")
    @patch("boto3.resource")
    @patch("caribou.common.teardown.teardown_tables.constants")
    def test_teardown_framework_buckets(self, mock_constants, mock_boto_resource, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3_resource = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_boto_resource.return_value = mock_s3_resource
        mock_constants.GLOBAL_SYSTEM_REGION = "us-west-2"
        mock_constants.TEST_BUCKET = "test_bucket"

        teardown_framework_buckets(Provider.AWS.value)

        mock_s3.head_bucket.assert_called_once_with(Bucket="test_bucket")
        mock_s3_resource.Bucket.assert_called_once_with("test_bucket")
        mock_s3_resource.Bucket().objects.all().delete.assert_called_once()
        mock_s3.delete_bucket.assert_called_once_with(Bucket="test_bucket")

    @patch("boto3.client")
    @patch("caribou.common.teardown.teardown_tables.constants")
    @patch("caribou.common.teardown.teardown_tables.Endpoints")
    def test_remove_sync_tables_all_regions(self, mock_endpoints, mock_constants, mock_boto_client):
        mock_dynamodb = MagicMock()
        mock_boto_client.return_value = mock_dynamodb
        mock_constants.GLOBAL_SYSTEM_REGION = "us-west-2"
        mock_constants.SYNC_MESSAGES_TABLE = "sync_messages"
        mock_constants.SYNC_PREDECESSOR_COUNTER_TABLE = "sync_predecessor_counter"
        mock_constants.AVAILABLE_REGIONS_TABLE = "available_regions"
        mock_endpoints().get_data_collector_client().get_all_values_from_table.return_value = {
            "aws:us-east-1": {},
            "aws:us-west-1": {},
        }

        remove_sync_tables_all_regions(Provider.AWS.value)

        self.assertEqual(mock_boto_client.call_count, 3)
        mock_dynamodb.describe_table.assert_any_call(TableName="sync_messages")
        mock_dynamodb.describe_table.assert_any_call(TableName="sync_predecessor_counter")
        mock_dynamodb.delete_table.assert_any_call(TableName="sync_messages")
        mock_dynamodb.delete_table.assert_any_call(TableName="sync_predecessor_counter")

    @patch("google.cloud.firestore.Client")
    def test_remove_gcp_collection(self, mock_firestore_client):
        """Tests the recursive deletion of documents in a Firestore collection."""
        mock_client_instance = mock_firestore_client.return_value
        mock_collection_ref = MagicMock()
        mock_batch = MagicMock()

        # Simulate two batches of documents to test recursion
        doc_ref1, doc_ref2 = MagicMock(), MagicMock()
        first_batch = [doc_ref1]
        second_batch = [doc_ref2]

        mock_client_instance.collection.return_value = mock_collection_ref
        mock_client_instance.batch.return_value = mock_batch
        # First call to stream() returns one doc, second call returns another, third returns empty
        mock_collection_ref.limit.return_value.stream.side_effect = [first_batch, second_batch, []]

        remove_gcp_collection(mock_client_instance, "test_collection")

        # Assert it was called recursively
        self.assertEqual(mock_client_instance.collection.call_count, 3)
        # Assert documents were deleted in batches
        self.assertEqual(mock_batch.delete.call_count, 2)
        self.assertEqual(mock_batch.commit.call_count, 2)

    @patch("google.cloud.firestore.Client")
    def test_remove_gcp_collection_not_found(self, mock_firestore_client):
        """Tests that no error is raised if the collection doesn't exist."""
        mock_client_instance = mock_firestore_client.return_value
        mock_client_instance.collection.side_effect = google_api_exceptions.NotFound("Collection not found")

        # This should run without raising an exception
        remove_gcp_collection(mock_client_instance, "non_existent_collection")
        mock_client_instance.collection.assert_called_once_with("non_existent_collection")

    # --- Tests for Top-Level Wrapper Functions ---

    @patch("caribou.common.teardown.teardown_tables.teardown_aws_framework_tables")
    @patch("caribou.common.teardown.teardown_tables.teardown_gcp_framework_tables")
    def test_teardown_framework_tables_dispatches_to_aws(self, mock_gcp_teardown, mock_aws_teardown):
        """Verify the main teardown function calls the AWS logic when provider is AWS."""
        with patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "aws"}):
            teardown_framework_tables(Provider.AWS.value)
            mock_aws_teardown.assert_called_once()
            mock_gcp_teardown.assert_not_called()

    @patch("caribou.common.teardown.teardown_tables.teardown_aws_framework_tables")
    @patch("caribou.common.teardown.teardown_tables.teardown_gcp_framework_tables")
    def test_teardown_framework_tables_dispatches_to_gcp(self, mock_gcp_teardown, mock_aws_teardown):
        """Verify the main teardown function calls the GCP logic when provider is GCP."""
        with patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp", "GCP_PROJECT_ID": "test-project"}):
            teardown_framework_tables(Provider.GCP.value)
            mock_gcp_teardown.assert_called_once()
            mock_aws_teardown.assert_not_called()

    @patch("caribou.common.teardown.teardown_tables.remove_sync_tables_aws_all_regions")
    @patch("caribou.common.teardown.teardown_tables.remove_sync_tables_gcp_all_regions")
    def test_remove_sync_tables_dispatches_correctly(self, mock_gcp_sync, mock_aws_sync):
        """Verify the sync table removal function dispatches correctly."""
        with patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "aws"}):
            remove_sync_tables_all_regions(Provider.AWS.value)
            mock_aws_sync.assert_called_once()
            mock_gcp_sync.assert_not_called()

        mock_aws_sync.reset_mock()
        mock_gcp_sync.reset_mock()

        with patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"}):
            remove_sync_tables_all_regions(Provider.GCP.value)
            mock_aws_sync.assert_not_called()
            mock_gcp_sync.assert_called_once()


if __name__ == "__main__":
    unittest.main()
