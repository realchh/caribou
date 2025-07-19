import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, UTC
import json

from google.api_core import exceptions as google_api_exceptions
from google.cloud import run_v2

from caribou.common.models.remote_client.gcp_remote_client import GCPRemoteClient
from caribou.deployment.common.deploy.models.resource import Resource
from caribou.common.constants import SYNC_MESSAGES_TABLE, SYNC_PREDECESSOR_COUNTER_TABLE


class TestGCPRemoteClient(unittest.TestCase):
    @patch("google.auth.default")
    @patch(
        "caribou.common.models.remote_client.gcp_remote_client.service_account.Credentials.from_service_account_file"
    )
    def setUp(self, mock_service_account, mock_default_auth):
        self.project_id = "test-project"
        self.region = "us-central1"

        # Mock credentials
        mock_creds = MagicMock()
        mock_creds.project_id = self.project_id
        mock_default_auth.return_value = (mock_creds, self.project_id)

        # Initialize the client
        self.gcp_client = GCPRemoteClient(project_id=self.project_id, region=self.region)

        # Mock all the service clients
        self.gcp_client._run_client = MagicMock()
        self.gcp_client._storage_client = MagicMock()
        self.gcp_client._firestore_client = MagicMock()
        self.gcp_client._firestore_admin_client = MagicMock()
        self.gcp_client._pubsub_publisher_client = MagicMock()
        self.gcp_client._pubsub_subscriber_client = MagicMock()
        self.gcp_client._eventarc_client = MagicMock()
        self.gcp_client._artifact_registry_client = MagicMock()
        self.gcp_client._iam_admin_client = MagicMock()
        self.gcp_client._resource_manager_client = MagicMock()
        self.gcp_client._logging_client = MagicMock()
        self.gcp_client._monitoring_client = MagicMock()
        self.gcp_client._scheduling_client = MagicMock()

        self.gcp_client.FUNCTION_CREATE_ATTEMPTS = 2
        self.gcp_client.DELAY_TIME = 0

    def test_get_current_provider_region(self):
        result = self.gcp_client.get_current_provider_region()
        self.assertEqual(result, f"gcp_{self.region}")

    def test_get_service_account_existing(self):
        # Test getting an existing service account
        service_account_name = "test-sa"
        expected_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"

        mock_sa = MagicMock()
        mock_sa.email = expected_email
        self.gcp_client._iam_admin_client.get_service_account.return_value = mock_sa
        self.gcp_client._iam_admin_client.create_service_account.side_effect = google_api_exceptions.AlreadyExists(
            "Already exists"
        )

        result = self.gcp_client.get_service_account(service_account_name)

        self.assertEqual(result, expected_email)

    def test_get_service_account_create_new(self):
        # Test creating a new service account
        service_account_name = "test-sa"
        expected_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"

        mock_sa = MagicMock()
        mock_sa.email = expected_email
        self.gcp_client._iam_admin_client.create_service_account.return_value = mock_sa

        result = self.gcp_client.get_service_account(service_account_name)

        self.gcp_client._iam_admin_client.create_service_account.assert_called_once_with(
            name=f"projects/{self.project_id}", account_id=service_account_name
        )
        self.assertEqual(result, expected_email)

    def test_get_cloud_run_service_exists(self):
        service_name = "test-service"

        # Create a mock service object
        mock_service = MagicMock(spec=run_v2.Service)
        mock_service.name = f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        mock_service.uri = "https://test-service.run.app"
        mock_service.update_time = MagicMock()
        mock_service.create_time = MagicMock()

        # Mock container
        mock_container = MagicMock()
        mock_container.image = "gcr.io/test/image:latest"
        mock_container.env = [MagicMock(name="TEST_VAR", value="test_value")]
        mock_container.resources.limits = {"memory": "512Mi", "cpu": "1"}

        # Mock template
        mock_template = MagicMock()
        mock_template.containers = [mock_container]
        mock_template.service_account = "test-sa@test-project.iam.gserviceaccount.com"
        mock_template.timeout = "300s"
        mock_template.scaling.min_instance_count = 0
        mock_template.scaling.max_instance_count = 100

        mock_service.template = mock_template

        self.gcp_client._run_client.service_path.return_value = (
            f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        )
        self.gcp_client._run_client.get_service.return_value = mock_service

        result = self.gcp_client.get_cloud_run_service(service_name)

        self.assertIsNotNone(result)
        self.assertEqual(result["ServiceName"], service_name)
        self.assertEqual(result["ImageUri"], "gcr.io/test/image:latest")

    def test_get_cloud_run_service_not_found(self):
        service_name = "non-existent-service"

        self.gcp_client._run_client.service_path.return_value = (
            f"projects/{self.project_id}/locations/{self.region}/services/{service_name}"
        )
        self.gcp_client._run_client.get_service.side_effect = google_api_exceptions.NotFound("Not found")

        result = self.gcp_client.get_cloud_run_service(service_name)

        self.assertIsNone(result)

    def test_set_predecessor_reached(self):
        predecessor_name = "pred1"
        sync_node_name = "sync1"
        workflow_instance_id = "workflow1"
        direct_call = True

        # Mock Firestore transaction
        mock_tx = MagicMock()
        mock_document = MagicMock()
        mock_snap = MagicMock()
        mock_snap.to_dict.return_value = {}
        mock_document.get.return_value = mock_snap

        self.gcp_client._firestore_client.collection.return_value.document.return_value = mock_document
        self.gcp_client._firestore_client.transaction.return_value = mock_tx

        # Mock the transactional decorator
        with patch("caribou.common.models.remote_client.gcp_remote_client.firestore.transactional", lambda f: f):
            result = self.gcp_client.set_predecessor_reached(
                predecessor_name, sync_node_name, workflow_instance_id, direct_call
            )

        self.assertEqual(len(result), 3)
        self.assertIsInstance(result[0], list)
        self.assertIsInstance(result[1], float)
        self.assertIsInstance(result[2], float)

    def test_create_sync_tables(self):
        mock_collection = MagicMock()
        mock_document = MagicMock()
        mock_collection.document.return_value = mock_document
        self.gcp_client._firestore_client.collection.return_value = mock_collection

        # Mock admin client methods
        mock_field = MagicMock()
        mock_field.ttl_config = None
        self.gcp_client._firestore_admin_client.get_field.return_value = mock_field

        with patch.object(self.gcp_client, "_ensure_firestore_database_exists"):
            self.gcp_client.create_sync_tables()

        # Verify collections were created
        expected_calls = [unittest.mock.call(SYNC_MESSAGES_TABLE), unittest.mock.call(SYNC_PREDECESSOR_COUNTER_TABLE)]
        for call in expected_calls:
            self.assertIn(call, self.gcp_client._firestore_client.collection.call_args_list)

    @patch("subprocess.run")
    @patch("tempfile.TemporaryDirectory")
    def test_create_function(self, mock_tempdir, mock_subprocess):
        function_name = "test-function"
        role_identifier = "test-sa@test-project.iam.gserviceaccount.com"
        zip_contents = b"test zip contents"
        runtime = "python312"
        handler = "main.handler"
        environment_variables = {"TEST_VAR": "test_value"}
        timeout = 300
        memory_size = 512

        # Mock temporary directory context manager
        mock_temp_context = MagicMock()
        mock_temp_context.__enter__.return_value = "/tmp/test"
        mock_temp_context.__exit__.return_value = None
        mock_tempdir.return_value = mock_temp_context

        # Mock deployed image URI (empty to trigger new build)
        self.gcp_client._get_deployed_image_uri = MagicMock(return_value="")

        # Mock Docker and upload methods
        self.gcp_client._build_docker_image = MagicMock()
        self.gcp_client._upload_image_to_artifact_registry = MagicMock(return_value="gcr.io/test/image:latest")
        self.gcp_client._store_deployed_image_uri = MagicMock()
        self.gcp_client._create_cloud_run_service = MagicMock(return_value="https://test-function.run.app")

        # Mock file operations
        with patch("builtins.open", unittest.mock.mock_open()):
            with patch("zipfile.ZipFile") as mock_zipfile:
                mock_zip_instance = MagicMock()
                mock_zipfile.return_value.__enter__.return_value = mock_zip_instance

                result = self.gcp_client.create_function(
                    function_name,
                    role_identifier,
                    zip_contents,
                    runtime,
                    handler,
                    environment_variables,
                    timeout,
                    memory_size,
                )

        self.assertEqual(result, "https://test-function.run.app")
        self.gcp_client._build_docker_image.assert_called_once()
        self.gcp_client._upload_image_to_artifact_registry.assert_called_once()

    def test_create_role(self):
        role_name = "test-role"
        policy = json.dumps({"roles": ["roles/storage.objectViewer", "roles/pubsub.publisher"]})
        trust_policy = {"test": "policy"}

        # Mock service account creation
        service_account_email = f"{role_name}@{self.project_id}.iam.gserviceaccount.com"
        self.gcp_client.get_service_account = MagicMock(return_value=service_account_email)

        # Mock project policy with proper bindings structure
        mock_policy = MagicMock()
        mock_binding = MagicMock()
        mock_binding.role = "existing_role"
        mock_binding.members = []

        # Create a mock bindings object that behaves like the GCP API
        class MockBindings:
            def __init__(self):
                self.bindings = [mock_binding]

            def __iter__(self):
                return iter(self.bindings)

            def add(self, role, members):
                new_binding = MagicMock()
                new_binding.role = role
                new_binding.members = members
                self.bindings.append(new_binding)

        mock_policy.bindings = MockBindings()

        self.gcp_client._resource_manager_client.get_iam_policy.return_value = mock_policy

        # Mock project details
        mock_project = MagicMock()
        mock_project.name = f"projects/123456"
        self.gcp_client._resource_manager_client.get_project.return_value = mock_project

        # Mock service account policy
        mock_sa_policy = MagicMock()
        mock_sa_policy.bindings = []
        self.gcp_client._iam_admin_client.get_iam_policy.return_value = mock_sa_policy

        result = self.gcp_client.create_role(role_name, policy, trust_policy)

        self.assertEqual(result, service_account_email)
        self.gcp_client.get_service_account.assert_called_once_with(name=role_name)

        self.gcp_client.get_service_account.assert_called_once_with(name=role_name)

    def test_create_pubsub_topic(self):
        topic_name = "test-topic"
        expected_topic_path = f"projects/{self.project_id}/topics/{topic_name}"

        mock_response = MagicMock()
        mock_response.name = expected_topic_path

        self.gcp_client._pubsub_publisher_client.topic_path.return_value = expected_topic_path
        self.gcp_client._pubsub_publisher_client.create_topic.return_value = mock_response

        # Mock project details for IAM policy
        mock_project = MagicMock()
        mock_project.name = f"projects/123456"
        self.gcp_client._resource_manager_client.get_project.return_value = mock_project

        # Mock IAM policy
        mock_policy = MagicMock()
        mock_policy.bindings = []
        self.gcp_client._pubsub_publisher_client.get_iam_policy.return_value = mock_policy

        result = self.gcp_client.create_pubsub_topic(topic_name)

        self.assertEqual(result, expected_topic_path)
        self.gcp_client._pubsub_publisher_client.create_topic.assert_called_once_with(name=expected_topic_path)

    def test_send_message_to_messaging_service(self):
        topic_identifier = "projects/test-project/topics/test-topic"
        message = "test message"

        mock_future = MagicMock()
        mock_future.result.return_value = "message-id"
        self.gcp_client._pubsub_publisher_client.publish.return_value = mock_future

        with patch("builtins.print"):  # Mock print to avoid output
            self.gcp_client.send_message_to_messaging_service(topic_identifier, message)

        self.gcp_client._pubsub_publisher_client.publish.assert_called_once()

    def test_set_value_in_table(self):
        table_name = "test-table"
        key = "test-key"
        value = "test-value"

        mock_document = MagicMock()
        self.gcp_client._firestore_client.collection.return_value.document.return_value = mock_document

        self.gcp_client.set_value_in_table(table_name, key, value)

        mock_document.set.assert_called_once_with({"value": value})

    def test_get_value_from_table(self):
        table_name = "test-table"
        key = "test-key"
        expected_value = "test-value"

        mock_snap = MagicMock()
        mock_snap.exists = True
        mock_snap.to_dict.return_value = {"value": expected_value}

        mock_document = MagicMock()
        mock_document.get.return_value = mock_snap
        self.gcp_client._firestore_client.collection.return_value.document.return_value = mock_document

        result, consumed_capacity = self.gcp_client.get_value_from_table(table_name, key)

        self.assertEqual(result, expected_value)
        self.assertEqual(consumed_capacity, 1.0)

    def test_upload_resource(self):
        key = "test-resource-key"
        resource = b"test resource data"

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        self.gcp_client._storage_client.bucket.return_value = mock_bucket

        self.gcp_client.upload_resource(key, resource)

        mock_blob.upload_from_string.assert_called_once_with(resource)

    def test_download_resource(self):
        key = "test-resource-key"
        expected_data = b"test resource data"

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = expected_data
        mock_bucket.blob.return_value = mock_blob
        self.gcp_client._storage_client.bucket.return_value = mock_bucket

        result = self.gcp_client.download_resource(key)

        self.assertEqual(result, expected_data)

    def test_query_metric(self):
        revision_name = "test-revision"
        metric_type = "run.googleapis.com/container/cpu/utilizations"
        start = datetime.now(UTC) - timedelta(hours=1)
        end = datetime.now(UTC)

        # Mock metric data
        mock_point = MagicMock()
        mock_point.value.double_value = 0.5

        mock_count_point = MagicMock()
        mock_count_point.value.int64_value = 2

        mock_series = MagicMock()
        mock_series.points = [mock_point]

        mock_count_series = MagicMock()
        mock_count_series.points = [mock_count_point]

        self.gcp_client._monitoring_client.list_time_series.side_effect = [
            [mock_series],  # First call for the metric
            [mock_count_series],  # Second call for instance count
        ]

        result = self.gcp_client.query_metric(revision_name, metric_type, start, end, "ALIGN_MAX")

        self.assertEqual(result, 0.25)  # 0.5 / 2

    def test_resource_exists_service_account(self):
        resource = Resource(name="test-sa", resource_type="service_account")

        self.gcp_client.service_account_exists = MagicMock(return_value=True)

        result = self.gcp_client.resource_exists(resource)

        self.assertTrue(result)
        self.gcp_client.service_account_exists.assert_called_once_with(resource)

    def test_resource_exists_cloud_run_service(self):
        resource = Resource(name="test-service", resource_type="cloud_run_service")

        self.gcp_client.cloud_run_service_exists = MagicMock(return_value=True)

        result = self.gcp_client.resource_exists(resource)

        self.assertTrue(result)
        self.gcp_client.cloud_run_service_exists.assert_called_once_with(resource)

    @patch("requests.post")
    @patch("google.oauth2.id_token.fetch_id_token")
    def test_invoke_remote_framework_with_payload(self, mock_fetch_token, mock_post):
        payload = {"action": "test", "data": "test_data"}

        # Mock service details
        mock_service = MagicMock()
        mock_service.uri = "https://remote-cli.run.app"
        self.gcp_client._run_client.service_path.return_value = (
            "projects/test/locations/us-central1/services/remote-cli"
        )
        self.gcp_client._run_client.get_service.return_value = mock_service

        # Mock auth
        mock_fetch_token.return_value = "test-token"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        self.gcp_client.invoke_remote_framework_with_payload(payload)

        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://remote-cli.run.app")
        self.assertEqual(kwargs["json"], payload)
        self.assertIn("Authorization", kwargs["headers"])

    def test_get_logs_between(self):
        function_instance = "test-function"
        start = datetime.now(UTC) - timedelta(hours=1)
        end = datetime.now(UTC)

        mock_entry = MagicMock()
        mock_entry.to_api_repr.return_value = {"test": "log"}

        self.gcp_client._logging_client.list_entries.return_value = [mock_entry]

        result = self.gcp_client.get_logs_between(function_instance, start, end)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], '{"test": "log"}')


if __name__ == "__main__":
    unittest.main()
