import json
import logging
import os
import random
import subprocess
import tempfile
import time
import zipfile
from datetime import UTC, datetime, timedelta
from time import sleep
from typing import Any, Optional

from google.api_core import exceptions as google_api_exceptions
from google.api_core.client_options import ClientOptions
from google.auth import default as google_auth_default
from google.cloud import (
    artifactregistry_v1,
    eventarc_v1,
    firestore,
    firestore_admin_v1,
    logging_v2,
    monitoring_v3,
    pubsub_v1,
    resourcemanager_v3,
    run_v2,
    scheduler_v1,
    storage,
)
from google.cloud.firestore_v1 import DocumentReference
from google.cloud.iam_admin_v1 import IAMClient
from google.iam.v1 import policy_pb2
from google.oauth2 import id_token, service_account  # pylint: disable=unused-import
from google.protobuf import field_mask_pb2, timestamp_pb2
from google.pubsub_v1 import DeadLetterPolicy, PushConfig, RetryPolicy

from caribou.common.constants import (
    BUFFER_GCP_METRICS_GRACE_PERIOD,
    CARIBOU_WORKFLOW_IMAGES_TABLE,
    DEPLOYMENT_RESOURCES_BUCKET,
    FIRESTORE_TTL_FIELD_NAME,
    GCP_LOG_SYNCER_DEFAULT_DELAY,
    GLOBAL_GCP_SYSTEM_REGION,
    REMOTE_CARIBOU_CLI_GCP_FUNCTION_NAME,
    SYNC_MESSAGES_TABLE,
    SYNC_PREDECESSOR_COUNTER_TABLE,
    SYNC_TABLE_TTL,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.utils import (
    compress_json_str,
    decompress_json_str,
    get_country_abbreviation,
    get_region_abbreviation,
)
from caribou.deployment.common.deploy.models.resource import Resource

logger = logging.getLogger(__name__)


# pylint: disable=too-many-lines
# pylint: disable=too-many-instance-attributes
class GCPRemoteClient(RemoteClient):  # pylint: disable=too-many-public-methods
    FUNCTION_CREATE_ATTEMPTS = 30
    DELAY_TIME = 5

    def __init__(
        self,
        project_id: str | None = None,
        region: str | None = GLOBAL_GCP_SYSTEM_REGION,
        credentials_path: str | None = None,
    ) -> None:
        if credentials_path:
            self._credentials = service_account.Credentials.from_service_account_file(
                credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            default_project_id = self._credentials.project_id
        else:
            self._credentials, default_project_id = google_auth_default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )

        self._project_id = project_id or default_project_id
        if not self._project_id:
            raise ValueError("GCP Project ID not found.")

        self._region = region
        if not self._region:
            raise ValueError("GCP region must be provided.")

        self._workflow_image_cache: dict[str, dict[str, str]] = {}
        self._deployment_resource_bucket: str = os.environ.get(
            "CARIBOU_OVERRIDE_DEPLOYMENT_RESOURCES_BUCKET", DEPLOYMENT_RESOURCES_BUCKET
        )

        self._client_cache: dict[str, Any] = {}
        self._last_request_time = 0.0

    # pylint: disable=too-many-branches
    def _client(self, service_name: str) -> Any:
        """Lazily initialize and cache GCP service clients."""
        if service_name not in self._client_cache:
            if service_name == "run":
                client_options = (
                    ClientOptions(api_endpoint=f"{self._region}-run.googleapis.com") if self._region else None
                )
                self._client_cache[service_name] = run_v2.ServicesClient(
                    credentials=self._credentials, client_options=client_options
                )
            elif service_name == "storage":
                self._client_cache[service_name] = storage.Client(
                    project=self._project_id, credentials=self._credentials
                )
            elif service_name == "firestore":
                self._client_cache[service_name] = firestore.Client(credentials=self._credentials)
            elif service_name == "firestore_admin":
                self._client_cache[service_name] = firestore_admin_v1.FirestoreAdminClient(
                    credentials=self._credentials
                )
            elif service_name == "pubsub_publisher":
                batch_settings = pubsub_v1.types.BatchSettings(
                    max_messages=1,  # Max 100 messages per batch
                    max_bytes=10000000,  # Max 10 MB per batch
                    max_latency=0.01,  # Max 0.01s (10ms) to wait before sending
                )
                self._client_cache[service_name] = pubsub_v1.PublisherClient(
                    credentials=self._credentials, batch_settings=batch_settings
                )
            elif service_name == "pubsub_subscriber":
                self._client_cache[service_name] = pubsub_v1.SubscriberClient(credentials=self._credentials)
            elif service_name == "eventarc":
                self._client_cache[service_name] = eventarc_v1.EventarcClient(credentials=self._credentials)
            elif service_name == "artifact_registry":
                self._client_cache[service_name] = artifactregistry_v1.ArtifactRegistryClient(
                    credentials=self._credentials
                )
            elif service_name == "iam_admin":
                self._client_cache[service_name] = IAMClient(credentials=self._credentials)
            elif service_name == "resource_manager":
                self._client_cache[service_name] = resourcemanager_v3.ProjectsClient(credentials=self._credentials)
            elif service_name == "logging":
                self._client_cache[service_name] = logging_v2.Client(credentials=self._credentials)
            elif service_name == "monitoring":
                self._client_cache[service_name] = monitoring_v3.MetricServiceClient(credentials=self._credentials)
            elif service_name == "scheduling":
                self._client_cache[service_name] = scheduler_v1.CloudSchedulerClient(credentials=self._credentials)
            else:
                raise ValueError(f"Unknown service name: {service_name}")

        return self._client_cache[service_name]

    @property
    def _run_client(self) -> run_v2.ServicesClient:
        return self._client("run")

    @property
    def _storage_client(self) -> storage.Client:
        return self._client("storage")

    @property
    def _firestore_client(self) -> firestore.Client:
        return self._client("firestore")

    @property
    def _firestore_admin_client(self) -> firestore_admin_v1.FirestoreAdminClient:
        return self._client("firestore_admin")

    @property
    def _pubsub_publisher_client(self) -> pubsub_v1.PublisherClient:
        return self._client("pubsub_publisher")

    @property
    def _pubsub_subscriber_client(self) -> pubsub_v1.SubscriberClient:
        return self._client("pubsub_subscriber")

    @property
    def _eventarc_client(self) -> eventarc_v1.EventarcClient:
        return self._client("eventarc")

    @property
    def _artifact_registry_client(self) -> artifactregistry_v1.ArtifactRegistryClient:
        return self._client("artifact_registry")

    @property
    def _iam_admin_client(self) -> IAMClient:
        return self._client("iam_admin")

    @property
    def _resource_manager_client(self) -> resourcemanager_v3.ProjectsClient:
        return self._client("resource_manager")

    @property
    def _logging_client(self) -> logging_v2.Client:
        return self._client("logging")

    @property
    def _monitoring_client(self) -> monitoring_v3.MetricServiceClient:
        return self._client("monitoring")

    @property
    def _scheduling_client(self) -> scheduler_v1.CloudSchedulerClient:
        return self._client("scheduling")

    def get_current_provider_region(self) -> str:
        return f"gcp_{self._region}"

    def get_service_account(self, name: str) -> str:
        sa_email = f"{name}@{self._project_id}.iam.gserviceaccount.com"
        full_account_name = f"projects/{self._project_id}/serviceAccounts/{sa_email}"
        try:
            service_account_object = self._iam_admin_client.get_service_account(name=full_account_name)
            return service_account_object.email
        except google_api_exceptions.NotFound:
            service_account_object = self._iam_admin_client.create_service_account(
                name=f"projects/{self._project_id}",
                account_id=name,
            )
            sleep(2)
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Failed to create service account: {e}") from e

        return service_account_object.email

    def get_cloud_run_service(self, service_name: str) -> dict[str, Any] | None:
        """
        Retrieves a Cloud Run service and formats its configuration as a dictionary.
        """
        service_object = self.get_cloud_run_service_object(service_name)
        if not service_object:
            return None

        return self._format_service_dict(service_object)

    def _format_service_dict(self, service: run_v2.Service) -> dict[str, Any]:
        """
        A helper method to translate a Cloud Run service dictionary into a format
        that resembles the AWS Lambda Configuration dictionary.
        """
        container = service.template.containers[0]
        template = service.template

        env_vars = {env.name: env.value for env in container.env}

        formatted_config = {
            "ServiceName": service.name.split("/")[-1],
            "ServiceArn": service.name,
            "ServiceUri": service.uri,
            "ImageUri": container.image,
            "Role": template.service_account,
            "MemorySize": container.resources.limits.get("memory"),
            "CpuLimit": container.resources.limits.get("cpu"),
            "Timeout": template.timeout,
            "Environment": {"Variables": env_vars},
            "LastModified": service.update_time,
            "CreateTime": service.create_time,
            "Scaling": {
                "MinInstances": template.scaling.min_instance_count,
                "MaxInstances": template.scaling.max_instance_count,
            },
        }
        return formatted_config

    def get_cloud_run_service_object(self, service_name: str) -> run_v2.Service | None:
        """
        Retrieves a Cloud Run service by its name.
        Returns the Service object or None if not found.
        """
        full_service_name = self._run_client.service_path(
            project=self._project_id, location=self._region, service=service_name
        )

        try:
            service_object = self._run_client.get_service(name=full_service_name)
            return service_object
        except google_api_exceptions.NotFound:
            return None

    def resource_exists(self, resource: Resource) -> bool:
        if resource.resource_type in ("service_account", "iam_role"):
            return self.service_account_exists(resource)
        if resource.resource_type in ("cloud_run_service", "function"):
            return self.cloud_run_service_exists(resource)
        if resource.resource_type in ("artifact_registry_repository", "ecr_repository"):
            return self.artifact_registry_repository_exists(resource)
        if resource.resource_type in ("pubsub_topic", "messaging_topic"):
            return False
        raise RuntimeError(f"Unknown resource type {resource.resource_type}")

    def service_account_exists(self, resource: Resource) -> bool:
        try:
            response = self.get_service_account(resource.name)
            return response is not None
        except google_api_exceptions.GoogleAPICallError:
            return False

    def cloud_run_service_exists(self, resource: Resource) -> bool:
        return self.get_cloud_run_service(resource.name) is not None

    # pylint: disable=too-many-statements
    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> tuple[list[bool], float, float]:
        client = self._firestore_client
        document = client.collection(SYNC_PREDECESSOR_COUNTER_TABLE).document(workflow_instance_id)

        # Add retry logic with exponential backoff
        max_retries = 15
        base_delay = 0.1
        max_delay = 5.0

        for attempt in range(max_retries):
            try:
                tx = client.transaction()

                @firestore.transactional
                def _transaction(
                    tx: firestore.Transaction, document: DocumentReference
                ) -> tuple[list[bool], dict[str, Any]]:
                    try:
                        snap = document.get(transaction=tx)
                        data = snap.to_dict() or {}

                    except google_api_exceptions.GoogleAPICallError as e:
                        logger.error("TRANSACTION: Failed to read document: %s", e)
                        raise

                    sync_map: dict[str, bool] = data.get(sync_node_name, {})

                    if not direct_call:
                        if predecessor_name not in sync_map:
                            sync_map[predecessor_name] = direct_call
                    else:
                        sync_map[predecessor_name] = direct_call

                    data[sync_node_name] = sync_map
                    data[FIRESTORE_TTL_FIELD_NAME] = datetime.now(UTC) + timedelta(seconds=SYNC_TABLE_TTL)

                    tx.set(document, data, merge=True)
                    return list(sync_map.values()), data

                bool_list, final_doc = _transaction(tx, document)

                # Serialize and calculate response size
                final_doc_copy = final_doc.copy()
                if "expires_at" in final_doc_copy:
                    final_doc_copy["expires_at"] = str(final_doc_copy["expires_at"])
                if FIRESTORE_TTL_FIELD_NAME in final_doc_copy:
                    final_doc_copy[FIRESTORE_TTL_FIELD_NAME] = str(final_doc_copy[FIRESTORE_TTL_FIELD_NAME])

                consumed_write_capacity = 1.0
                response_size = len(json.dumps(final_doc_copy).encode("utf-8")) / (1024**3)

                return bool_list, response_size, consumed_write_capacity

            except google_api_exceptions.Aborted as e:
                # Transaction was aborted due to contention, retry with backoff
                if attempt < max_retries - 1:
                    delay = min(base_delay * (2**attempt), max_delay)
                    jitter = random.uniform(0, delay * 0.1)
                    total_delay = delay + jitter
                    logger.warning(
                        "Transaction aborted on attempt %s, retrying in %.2fs: %s", attempt + 1, total_delay, e
                    )
                    time.sleep(total_delay)
                    continue

                logger.error("Transaction failed after %d attempts due to contention: %s", max_retries, e)
                raise RuntimeError(f"Failed to set predecessor reached after {max_retries} attempts: {e}") from e

            except google_api_exceptions.GoogleAPICallError as e:
                logger.error(
                    "Firestore transaction failed for sync node %s on attempt %d: %s", sync_node_name, attempt + 1, e
                )
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                    time.sleep(delay)
                    continue

                raise RuntimeError(f"Failed to set predecessor reached after {max_retries} attempts: {e}") from e

            except ValueError as e:
                logger.error(
                    "Firestore transaction failed for sync node %s on attempt %d: %s", sync_node_name, attempt + 1, e
                )
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                    time.sleep(delay)
                    continue

                raise RuntimeError(f"Failed to set predecessor reached after {max_retries} attempts: {e}") from e

        # Should never reach here, but return empty state if all retries failed
        raise RuntimeError(f"Failed to set predecessor reached after {max_retries} attempts: Unknown error")

    def create_sync_tables(self) -> None:
        # Check if table exists
        client = self._firestore_client
        admin_client = self._firestore_admin_client
        database_path = f"projects/{self._project_id}/databases/(default)"

        self._ensure_firestore_database_exists(database_name="(default)")

        for table in [SYNC_MESSAGES_TABLE, SYNC_PREDECESSOR_COUNTER_TABLE]:
            client.collection(table).document("_sentinel").set({"created_at": firestore.SERVER_TIMESTAMP}, merge=True)
            collection_group = f"{database_path}/collectionGroups/{table}"
            ttl_field_path = f"{collection_group}/fields/{FIRESTORE_TTL_FIELD_NAME}"

            field = admin_client.get_field(name=ttl_field_path)
            if not field.ttl_config:
                ttl_field = firestore_admin_v1.Field(
                    {"name": ttl_field_path, "ttl_config": firestore_admin_v1.Field.TtlConfig()}
                )
                request = firestore_admin_v1.UpdateFieldRequest(
                    {
                        "field": ttl_field,
                        "update_mask": field_mask_pb2.FieldMask(  # pylint: disable=maybe-no-member
                            paths=["ttl_config"]
                        ),
                    }
                )
                admin_client.update_field(request=request)

    def _ensure_firestore_database_exists(self, database_name: str) -> bool:
        client = self._firestore_admin_client
        db_path = f"projects/{self._project_id}/databases/{database_name}"
        try:
            client.get_database(name=db_path)
            return True
        except google_api_exceptions.NotFound:
            pass

        database = firestore_admin_v1.types.Database(
            name=db_path,
            type_=firestore_admin_v1.types.Database.DatabaseType.FIRESTORE_NATIVE,
            location_id=self._region,
        )

        try:
            response = client.create_database(
                parent=f"projects/{self._project_id}", database_id=database_name, database=database
            )
            response.result()
            return True
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Failed to create database: {e}") from e

    def upload_predecessor_data_at_sync_node(
        self, function_name: str, workflow_instance_id: str, message: str
    ) -> float:
        client = self._firestore_client
        document = client.collection(SYNC_MESSAGES_TABLE).document(f"{workflow_instance_id}:{function_name}")

        # ArrayUnion is atomic, so no transaction needed - this reduces contention significantly
        max_retries = 15
        base_delay = 0.1
        max_delay = 5.0

        for attempt in range(max_retries):
            try:
                transaction = client.transaction()

                @firestore.transactional
                def _transactional_update(tx: firestore.Transaction, doc_ref: DocumentReference) -> None:
                    snap = doc_ref.get(transaction=tx)

                    if snap.exists:
                        # Document exists, just update the message array
                        tx.update(doc_ref, {"message": firestore.ArrayUnion([message])})
                    else:
                        ttl_time = datetime.now(UTC) + timedelta(seconds=SYNC_TABLE_TTL)

                        # Document does not exist, create it with the message and TTL
                        tx.set(
                            doc_ref,
                            {
                                "message": [message],
                                FIRESTORE_TTL_FIELD_NAME: ttl_time,
                            },
                        )

                _transactional_update(transaction, document)

                return 1.0

            except google_api_exceptions.GoogleAPICallError as e:
                logger.error(
                    "Firestore update failed for sync node %s on attempt %d: %s", function_name, attempt + 1, e
                )
                if attempt < max_retries - 1:
                    delay = min(base_delay * (2**attempt), max_delay)
                    jitter = random.uniform(0, delay * 0.1)
                    time.sleep(delay + jitter)
                    continue

                raise RuntimeError(f"Failed to upload predecessor data after {max_retries} attempts: {e}") from e

            except ValueError as e:
                logger.error(
                    "Firestore transaction failed for sync node %s on attempt %d: %s", function_name, attempt + 1, e
                )
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                    time.sleep(delay)
                    continue

                raise RuntimeError(f"Failed to set predecessor reached after {max_retries} attempts: {e}") from e

        raise RuntimeError("Failed to upload predecessor data: unknown error")

    def get_predecessor_data(
        self,
        current_instance_name: str,
        workflow_instance_id: str,
        consistent_read: bool = True,  # pylint: disable=unused-argument
    ) -> tuple[list[str], float]:
        client = self._firestore_client
        document_id = f"{workflow_instance_id}:{current_instance_name}"
        snap = client.collection(SYNC_MESSAGES_TABLE).document(document_id).get()

        if not snap.exists:
            return [], 0.0

        data = snap.to_dict() or {}
        messages: list[str] = data.get("message", [])
        consumed_read_capacity = 1.0
        return messages, consumed_read_capacity

    def create_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: Optional[bytes],
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        cpu: float | None = None,
        concurrency: int | None = None,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        image_uri: str
        deployed_image_uri = self._get_deployed_image_uri(function_name)
        if len(deployed_image_uri) > 0:
            image_uri = self._copy_image_to_region(deployed_image_uri)
        else:
            if zip_contents is None:
                raise RuntimeError("No deployed image AND No deployment package provided for function creation")

            with tempfile.TemporaryDirectory() as tmpdirname:
                # Step 1: Unzip the ZIP file
                zip_path = os.path.join(tmpdirname, "code.zip")
                with open(zip_path, "wb") as f_zip:
                    f_zip.write(zip_contents)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

                # Step 2: Create a Dockerfile in the temporary directory
                dockerfile_content = self._generate_dockerfile(handler, additional_docker_commands)
                with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
                    f_dockerfile.write(dockerfile_content)

                # Step 3: Build the Docker Image
                image_name = f"{function_name.lower()}:latest"
                self._build_docker_image(tmpdirname, image_name)

                # Step 4: Upload the Image to Artifact Registry
                image_uri = self._upload_image_to_artifact_registry(image_name)
                self._store_deployed_image_uri(function_name, image_uri)

        final_cpu = self._get_gcp_cpu_config(cpu, memory_size)

        if concurrency is None:
            raise RuntimeError("No concurrency specified")

        service_url = self._create_cloud_run_service(
            service_name=function_name,
            image_uri=image_uri,
            env=environment_variables,
            cpu=final_cpu,
            memory_mib=memory_size,
            timeout_s=timeout,
            service_account_email=role_identifier,
            max_concurrency=concurrency,
        )

        return service_url

    def _create_cloud_run_service(
        self,
        service_name: str,
        image_uri: str,
        env: dict[str, str],
        cpu: float,
        memory_mib: int,
        timeout_s: int,
        service_account_email: str,
        max_concurrency: int,
    ) -> str:
        client = self._run_client
        parent = f"projects/{self._project_id}/locations/{self._region}"
        full_name = f"{parent}/services/{service_name}"

        container = run_v2.Container()
        container.image = image_uri
        container.env = [run_v2.EnvVar({"name": k, "value": v}) for k, v in env.items()]
        container.resources = run_v2.ResourceRequirements(limits={"cpu": str(cpu), "memory": f"{memory_mib}Mi"})

        max_instances = self._calculate_safe_max_instances(memory_mib)

        template = run_v2.RevisionTemplate(
            {
                "max_instance_request_concurrency": max_concurrency,
                "containers": [container],
                "timeout": f"{timeout_s}s",
                "service_account": service_account_email,
                "scaling": run_v2.RevisionScaling({"min_instance_count": 0, "max_instance_count": max_instances}),
            }
        )

        svc = run_v2.Service()
        svc.template = template

        try:
            op = client.create_service(parent=parent, service=svc, service_id=service_name)
        except google_api_exceptions.AlreadyExists:
            existing = client.get_service(name=full_name)
            existing.template = template
            mask = field_mask_pb2.FieldMask(paths=["template"])  # pylint: disable=maybe-no-member
            op = client.update_service(service=existing, update_mask=mask)

        op.result()
        return client.get_service(name=full_name).uri

    def _calculate_safe_max_instances(self, memory_mib: int) -> int:
        """
        Calculate safe max instances based on memory allocation and regional quota.

        Args:
            memory_mib: Memory in MiB per instance

        Returns:
            Safe maximum number of instances
        """
        # Convert MiB to bytes
        memory_bytes_per_instance = memory_mib * 1024 * 1024

        # GCP default regional memory quota is ~400GB (429,496,729,600 bytes)
        # Use 90% of quota to leave safety margin
        regional_quota_bytes = 429_496_729_600 * 0.9

        # Calculate max instances based on quota
        max_instances_by_quota = int(regional_quota_bytes / memory_bytes_per_instance)

        max_instances = max(1, min(100, max_instances_by_quota))

        return max_instances

    def _store_deployed_image_uri(self, function_name: str, image_name: str) -> None:
        workflow_instance_id = "-".join(function_name.split("-")[0:5])
        function_name_simple = function_name[len(workflow_instance_id) + 1 :]
        function_name_simple = "-".join(function_name_simple.split("-")[0:3])

        if workflow_instance_id not in self._workflow_image_cache:
            self._workflow_image_cache[workflow_instance_id] = {}

        self._workflow_image_cache[workflow_instance_id].update({function_name_simple: image_name})

        client = self._firestore_client
        document = client.collection(CARIBOU_WORKFLOW_IMAGES_TABLE).document(workflow_instance_id)
        document.set({function_name_simple: image_name}, merge=True)

    def _copy_image_to_region(self, deployed_image_uri: str) -> str:
        parts = deployed_image_uri.split("/")
        original_image_name = parts[-1]
        original_region = "-".join(original_image_name.split("-")[8:10])

        new_region = self._region

        if new_region is None:
            raise RuntimeError("No remote client region specified. This should be impossible")

        new_region_country = new_region.split("-")[0]
        new_region_country = get_country_abbreviation(new_region_country)
        new_region_region = new_region.split("-")[1]
        new_region_region = get_region_abbreviation(new_region_region)

        new_region = f"{new_region_country}-{new_region_region}"
        new_image_name = original_image_name.replace(original_region, new_region)

        repo_id = "caribou"  # Base artifact registry repo to hold the docker images used for deployment
        self._ensure_repository(repo_id)

        host = f"{self._region}-docker.pkg.dev"
        image_path = f"{host}/{self._project_id}/{repo_id}"
        new_image_uri = f"{image_path}/{new_image_name}"

        original_ecr_registry = f"{original_region}-docker.pkg.dev"

        # Use /tmp directory which is writable in Cloud Run
        with tempfile.TemporaryDirectory(dir="/tmp") as temp_dir:
            print(f"Using crane to copy image from {original_ecr_registry} to {host}")
            try:
                subprocess.run(
                    ["gcrane", "cp", deployed_image_uri, new_image_uri],
                    cwd=temp_dir,
                    check=True,
                )
                logger.info("Docker image %s copied successfully.", new_image_uri)
            except subprocess.CalledProcessError as e:
                logger.error("Failed to copy Docker image %s. Error: %s", new_image_uri, e)
            return new_image_uri

    def _get_deployed_image_uri(self, function_name: str) -> str:
        workflow_instance_id = "-".join(function_name.split("-")[0:5])
        function_name_simple = function_name[len(workflow_instance_id) + 1 :]
        function_name_simple = "-".join(function_name_simple.split("-")[0:3])
        if workflow_instance_id not in self._workflow_image_cache:
            self._workflow_image_cache[workflow_instance_id] = {}

        cached = self._workflow_image_cache[workflow_instance_id].get(function_name_simple)

        if cached:
            return cached

        client = self._firestore_client

        snap = client.collection(CARIBOU_WORKFLOW_IMAGES_TABLE).document(workflow_instance_id).get()

        if not snap.exists:
            return ""

        document_dict = snap.to_dict() or {}
        image_uri = document_dict.get(function_name_simple, "")
        self._workflow_image_cache.setdefault(workflow_instance_id, {})[function_name_simple] = image_uri

        return image_uri

    def _generate_dockerfile(self, handler: str, additional_docker_commands: Optional[list[str]]) -> str:
        run_command = ""
        if additional_docker_commands and len(additional_docker_commands) > 0:
            run_command += " && ".join(additional_docker_commands)
        if len(run_command) > 0:
            run_command = f"RUN {run_command}"

        source_file = handler.split(".")[0] + ".py"
        target_function = handler.split(".")[-1]

        return f"""
        FROM {self._region}-docker.pkg.dev/serverless-runtimes/google-22/runtimes/python312
        WORKDIR /app        
        ENV PYTHONPATH /app
        ENV CARIBOU_DEFAULT_PROVIDER gcp
        COPY requirements.txt ./
        USER root
        {run_command}
        RUN apt-get update && \
            apt-get install -y --no-install-recommends \
                libsqlite3-0 \
            && rm -rf /var/lib/apt/lists/*
        RUN pip3 install --no-cache-dir -r requirements.txt
        COPY app.py ./
        COPY src ./src
        COPY caribou ./caribou
        CMD ["functions-framework", \
        "--source", "{source_file}", \
        "--target", "{target_function}", \
        "--signature-type", "event"]
        """

    def _build_docker_image(self, context_path: str, image_name: str) -> None:
        try:
            subprocess.run(["docker", "build", "--platform", "linux/amd64", "-t", image_name, context_path], check=True)
            logger.info("Docker image %s built successfully.", image_name)
        except subprocess.CalledProcessError as e:
            # This will catch errors from the subprocess and logger.info a message.
            logger.error("Failed to build Docker image %s. Error: %s", image_name, e)

    def _ensure_repository(self, repository_name: str) -> str:
        """
        Returns the full resource name of the repository. If the repository does not exist, it will be created.
        """
        repository_name = repository_name.lower()
        client = self._artifact_registry_client
        full_repo = client.repository_path(
            project=self._project_id,
            location=self._region,
            repository=repository_name,
        )

        try:
            client.get_repository(name=full_repo)
        except google_api_exceptions.NotFound:
            repository = artifactregistry_v1.Repository()
            repository.format_ = artifactregistry_v1.Repository.Format.DOCKER
            repository.description = "Caribou build artifacts"
            client.create_repository(
                parent=f"projects/{self._project_id}/locations/{self._region}",
                repository_id=repository_name,
                repository=repository,
            )
            sleep(5)

        return full_repo

    def _upload_image_to_artifact_registry(self, image_name: str) -> str:
        repo_id = "caribou"  # Base artifact registry repo to hold the docker images used for deployment
        self._ensure_repository(repo_id)

        host = f"{self._region}-docker.pkg.dev"
        image_path = f"{host}/{self._project_id}/{repo_id}"

        name, tag = (image_name.split(":", 1) + ["latest"])[:2]
        remote = f"{image_path}/{name}:{tag}"

        subprocess.run(
            ["gcloud", "auth", "configure-docker", host, "--quiet"],
            check=True,
        )

        subprocess.run(["docker", "tag", image_name, remote], check=True)
        subprocess.run(["docker", "push", remote], check=True)

        logging.info("Pushed image %s", remote)
        return remote

    def update_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: Optional[bytes],
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        cpu: float | None = None,
        concurrency: int | None = None,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        image_uri: str
        deployed_image_uri = self._get_deployed_image_uri(function_name)
        if len(deployed_image_uri) > 0:
            image_uri = self._copy_image_to_region(deployed_image_uri)
        else:
            if zip_contents is None:
                raise RuntimeError("No deployed image AND No deployment package provided for function update")

            # Process the ZIP contents to build and upload a Docker image,
            # then update the function code with the image URI
            with tempfile.TemporaryDirectory() as tmpdirname:
                zip_path = os.path.join(tmpdirname, "code.zip")
                with open(zip_path, "wb") as f_zip:
                    f_zip.write(zip_contents)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

                dockerfile_content = self._generate_dockerfile(handler, additional_docker_commands)
                with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
                    f_dockerfile.write(dockerfile_content)

                image_name = f"{function_name.lower()}:latest"
                self._build_docker_image(tmpdirname, image_name)
                image_uri = self._upload_image_to_artifact_registry(image_name)
                self._store_deployed_image_uri(function_name, image_uri)

        final_cpu = self._get_gcp_cpu_config(cpu, memory_size)

        if concurrency is None:
            raise RuntimeError("No concurrency specified")

        service_url = self._create_cloud_run_service(
            service_name=function_name,
            image_uri=image_uri,
            env=environment_variables,
            cpu=final_cpu,
            memory_mib=memory_size,
            timeout_s=timeout,
            service_account_email=role_identifier,
            max_concurrency=concurrency,
        )

        return service_url

    def remove_function(self, function_name: str) -> None:
        client = self._run_client
        function_name = function_name.replace("_", "-")
        full_path = f"projects/{self._project_id}/locations/{self._region}/services/{function_name}"
        try:
            response = client.delete_service(name=full_path)
            response.result()
        except google_api_exceptions.NotFound as e:
            logger.info("Function %s not found (maybe the function is already deleted): %s", function_name, e)

    def create_role(self, role_name: str, policy: str, trust_policy: dict | None = None) -> str:
        policy_list = json.loads(policy)
        if "roles" not in policy_list:
            raise ValueError("Policy must contain 'roles'")

        roles = policy_list["roles"]

        client = self._iam_admin_client
        project_client = self._resource_manager_client

        service_account_email = f"{role_name}@{self._project_id}.iam.gserviceaccount.com"
        service_account_name = f"projects/{self._project_id}/serviceAccounts/{service_account_email}"

        sa = self.get_service_account(name=role_name)

        project_policy = project_client.get_iam_policy(resource=f"projects/{self._project_id}")

        policy_member = f"serviceAccount:{service_account_email}"
        existing_roles = {binding.role for binding in project_policy.bindings if policy_member in binding.members}
        for role in roles:
            if role in existing_roles:
                continue

            binding_found = False
            for binding in project_policy.bindings:
                if binding.role == role:
                    binding_found = True
                    binding.members.append(policy_member)

            if not binding_found:
                project_policy.bindings.add(role=role, members=[policy_member])

        project_client.set_iam_policy(request={"resource": f"projects/{self._project_id}", "policy": project_policy})

        service_account_policy = client.get_iam_policy(resource=service_account_name)

        project_number = self._resource_manager_client.get_project(name=f"projects/{self._project_id}").name.split("/")[
            1
        ]
        pubsub_service_account = f"serviceAccount:service-{project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"

        service_account_policy.bindings.append(
            policy_pb2.Binding(  # pylint: disable=maybe-no-member
                role="roles/iam.serviceAccountTokenCreator", members=[pubsub_service_account]
            )
        )

        client.set_iam_policy(request={"resource": service_account_name, "policy": service_account_policy})
        return sa

    def update_role(self, role_name: str, policy: str, trust_policy: dict | None = None) -> str:
        policy_list = json.loads(policy)
        if "roles" not in policy_list:
            raise ValueError("Policy must contain 'roles'")

        roles = policy_list["roles"]

        project_client = self._resource_manager_client

        service_account_email = f"{role_name}@{self._project_id}.iam.gserviceaccount.com"

        sa = self.get_service_account(name=role_name)

        project_policy = project_client.get_iam_policy(resource=f"projects/{self._project_id}")

        policy_member = f"serviceAccount:{service_account_email}"

        existing_roles = {
            binding.role: binding for binding in project_policy.bindings if policy_member in binding.members
        }

        for role in set(roles) - existing_roles.keys():
            b = next((bind for bind in project_policy.bindings if bind.role == bind), None)
            if b:
                b.members.append(policy_member)
            else:
                project_policy.bindings.add(role=role, members=[policy_member])

        for obsolete in existing_roles.keys() - set(roles):
            bind = existing_roles[obsolete]
            bind.members.remove(policy_member)
            if not bind.members:
                project_policy.bindings.remove(bind)

        project_client.set_iam_policy(request={"resource": f"projects/{self._project_id}", "policy": project_policy})
        return sa

    def remove_role(self, role_name: str) -> None:
        client = self._resource_manager_client

        service_account_email = f"{role_name}@{self._project_id}.iam.gserviceaccount.com"
        policy_member = f"serviceAccount:{service_account_email}"
        service_account_name = f"projects/{self._project_id}/serviceAccounts/{service_account_email}"

        try:
            project_policy = client.get_iam_policy(resource=f"projects/{self._project_id}")
            policy_changed = False

            for i in range(len(project_policy.bindings) - 1, -1, -1):
                binding = project_policy.bindings[i]

                if policy_member in binding.members:
                    binding.members.remove(policy_member)
                    policy_changed = True

                if not binding.members:
                    del project_policy.bindings[i]

            if policy_changed:
                client.set_iam_policy(request={"resource": f"projects/{self._project_id}", "policy": project_policy})

        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Could not delete member from IAM Role {e}") from e

        time.sleep(3)  # wait until gcp updates the roles

        try:
            iam_client = self._iam_admin_client
            iam_client.delete_service_account(name=service_account_name)
        except google_api_exceptions.NotFound as e:
            raise RuntimeError(f"Service account {service_account_email} not found") from e
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Failed to delete service account {service_account_email} from IAM Role {e}") from e

    def create_pubsub_topic(self, topic_name: str) -> str:
        client = self._pubsub_publisher_client
        # If topic exists, the following will return the existing topic
        topic_path = client.topic_path(self._project_id, topic_name)
        try:
            response = client.create_topic(name=topic_path)
        except google_api_exceptions.AlreadyExists:
            return topic_path

        resource_manager_client = self._resource_manager_client
        project_number = resource_manager_client.get_project(name=f"projects/{self._project_id}").name.split("/")[1]
        publisher_service_email = f"{project_number}-compute@developer.gserviceaccount.com"
        publisher_member = f"serviceAccount:{publisher_service_email}"
        publisher_role = "roles/pubsub.publisher"

        try:
            policy = client.get_iam_policy(request={"resource": topic_path})

            binding_to_modify = None
            for binding in policy.bindings:
                if binding.role == publisher_role:
                    binding_to_modify = binding
                    break

            if binding_to_modify:
                if publisher_member in binding_to_modify.members:
                    return response.name  # binding already exists

                binding_to_modify.members.append(publisher_member)

            else:
                policy.bindings.append(
                    policy_pb2.Binding(  # pylint: disable=maybe-no-member
                        role=publisher_role, members=[publisher_member]
                    )
                )

            client.set_iam_policy(request={"resource": topic_path, "policy": policy})
        except google_api_exceptions.NotFound as e:
            logger.exception("Topic %s not found. Cannot set IAM policy: %s", topic_name, e)
        except google_api_exceptions.GoogleAPICallError as e:
            logger.exception("Failed to set IAM policy for topic %s: %s", topic_path, e)

        return response.name

    def create_pubsub_subscription(
        self, topic: str, subscription_name: str, push_endpoint: str, service_account_name: str, timeout: int
    ) -> str:
        client = self._pubsub_subscriber_client

        push_config = PushConfig(
            push_endpoint=push_endpoint,
            oidc_token=PushConfig.OidcToken(service_account_email=service_account_name, audience=push_endpoint),
        )

        subscription_path = client.subscription_path(project=self._project_id, subscription=subscription_name)

        # timeout of pub/sub ack has a minimum of 10 seconds and maximum of 600 seconds
        # https://cloud.google.com/pubsub/docs/subscription-properties#ack_deadline
        timeout = max(min(timeout, 600), 10)

        dead_letter_topic_id = f"{subscription_name}-dl"
        dead_letter_topic_path = self.create_pubsub_topic(dead_letter_topic_id)

        dead_letter_policy = DeadLetterPolicy(
            dead_letter_topic=dead_letter_topic_path,
            max_delivery_attempts=5,
        )

        # make it so that pub/sub will retry with an exponential backoff (avoiding retry spam)
        retry_policy = RetryPolicy()

        try:
            response = client.create_subscription(
                {
                    "name": subscription_path,
                    "topic": topic,
                    "push_config": push_config,
                    "ack_deadline_seconds": timeout,
                    "retry_policy": retry_policy,
                    "dead_letter_policy": dead_letter_policy,
                }
            )
        except google_api_exceptions.AlreadyExists:
            return subscription_path
        return response.name

    def add_pubsub_permission_for_cloud_run(self, cloud_run_service_name: str, service_account_name: str) -> None:
        client = self._run_client

        full_name = client.service_path(self._project_id, self._region, cloud_run_service_name)
        policy = client.get_iam_policy(request={"resource": full_name})

        invoker_member = f"serviceAccount:{service_account_name}"

        for binding in policy.bindings:
            if binding.role == "roles/run.invoker" and invoker_member in binding.members:
                return

        new_binding = policy_pb2.Binding(  # pylint: disable=maybe-no-member
            role="roles/run.invoker", members=[invoker_member]
        )
        policy.bindings.append(new_binding)
        client.set_iam_policy(request={"resource": full_name, "policy": policy})

    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        client = self._pubsub_publisher_client
        print("publishing")
        # compressed json (also change caribou/deployment/client/caribou_workflow.py:1270 to toggle compression)
        # response = client.publish(topic=identifier, data=compress_json_str(message))
        response = client.publish(topic=identifier, data=message.encode("utf-8"))
        # for some reason it needs this line so the message gets sent to pub/sub
        print(response.result())

    def set_value_in_table(self, table_name: str, key: str, value: str, convert_to_bytes: bool = False) -> None:
        client = self._firestore_client
        doc = client.collection(table_name).document(key)
        if convert_to_bytes:
            doc.set({"value": compress_json_str(value)})
        else:
            doc.set({"value": value})

    def update_value_in_table(self, table_name: str, key: str, value: str, convert_to_bytes: bool = False) -> None:
        client = self._firestore_client
        doc = client.collection(table_name).document(key)
        try:
            if convert_to_bytes:
                doc.update({"value": compress_json_str(value)})
            else:
                doc.update({"value": value})
        except google_api_exceptions.NotFound:
            if convert_to_bytes:
                doc.set({"value": compress_json_str(value)})
            else:
                doc.set({"value": value})
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Could not update value in table {table_name} for key {key}: {e}") from e

    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        client = self._firestore_client
        doc_ref = client.collection(table_name).document(key)

        update_data: dict[str, Any] = {}
        for column, type_, value in column_type_value:
            if type_ == "S":
                update_data[column] = value
            else:
                update_data[column] = compress_json_str(value)

        doc_ref.set(update_data, merge=True)

    def get_value_from_table(self, table_name: str, key: str, consistent_read: bool = True) -> tuple[str, float]:
        client = self._firestore_client
        doc = client.collection(table_name).document(key).get()

        consumed_read_capacity = 1.0

        if not doc.exists:
            return "", consumed_read_capacity

        value = doc.to_dict().get("value")

        if isinstance(value, bytes):
            return decompress_json_str(value), consumed_read_capacity

        if isinstance(value, str):
            return value, consumed_read_capacity

        return "", consumed_read_capacity

    def remove_value_from_table(self, table_name: str, key: str) -> None:
        client = self._firestore_client
        client.collection(table_name).document(key).delete()

    def get_all_values_from_table(self, table_name: str) -> dict[str, Any]:
        client = self._firestore_client
        docs = client.collection(table_name).stream()

        result: dict[str, Any] = {}
        for doc in docs:
            doc_dict = doc.to_dict()
            value = doc_dict.get("value")

            if isinstance(value, bytes):
                result[doc.id] = decompress_json_str(value)
            elif isinstance(value, str):
                result[doc.id] = value

        return result

    def get_key_present_in_table(self, table_name: str, key: str, consistent_read: bool = True) -> bool:
        client = self._firestore_client
        doc = client.collection(table_name).document(key).get()
        return doc.exists

    def get_keys(self, table_name: str) -> list[str]:
        client = self._firestore_client
        collection = client.collection(table_name)
        documents = collection.list_documents()
        return [document.id for document in documents]

    def remove_key(self, table_name: str, key: str) -> None:
        client = self._firestore_client
        document = client.collection(table_name).document(key)

        try:
            document.delete()
        except google_api_exceptions.NotFound as e:
            logger.info("key %s not found: %s", key, e)
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Could not delete key {key} from table {table_name}: {e}") from e

    def upload_resource(self, key: str, resource: bytes) -> None:
        client = self._storage_client
        bucket = client.bucket(self._deployment_resource_bucket)
        blob = bucket.blob(key)
        try:
            blob.upload_from_string(resource)
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(
                f"Error uploading resource {key}, does the bucket {self._deployment_resource_bucket} exist and do you have permission to access it: {str(e)}"  # pylint: disable=line-too-long
            ) from e

    def download_resource(self, key: str) -> bytes:
        client = self._storage_client
        bucket = client.bucket(self._deployment_resource_bucket)
        blob = bucket.blob(key)
        try:
            return blob.download_as_bytes()
        except google_api_exceptions.NotFound as e:
            raise RuntimeError(
                f"Key {key} not found at the bucket {self._deployment_resource_bucket}. Is the resource deployed? {str(e)}"  # pylint: disable=line-too-long
            ) from e
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(
                f"Error uploading resource {key}, does the bucket {self._deployment_resource_bucket} exist and do you have permission to access it: {str(e)}"  # pylint: disable=line-too-long
            ) from e

    def remove_resource(self, key: str) -> None:
        client = self._storage_client
        bucket = client.bucket(self._deployment_resource_bucket)
        blob = bucket.blob(key)

        try:
            blob.delete()
        except google_api_exceptions.NotFound as e:
            logger.info("key %s not found: %s", key, e)
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Could not delete resource {key} from database: {e}") from e

    def _log_filter(self, service_name: str, start: datetime | None = None, end: datetime | None = None) -> list[str]:
        max_retries = 5

        if start:
            time_start = start.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        else:
            time_start = None

        if end:
            time_end = end.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        else:
            time_end = None

        resource_filter = f'resource.labels.service_name="{service_name}"'

        query = (
            f'resource.type="cloud_run_revision" {resource_filter} AND '
            f'(logName = "projects/{self._project_id}/logs/run.googleapis.com%2Frequests" '
            f'OR jsonPayload.severity = "CARIBOU")'
        )

        if time_start:
            query += f' AND timestamp>="{time_start}"'

        if time_end:
            query += f' AND timestamp<="{time_end}"'

        client = self._logging_client

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                result = []
                entry_count = 0

                # Apply rate limiting before each request except for the first one
                if attempt > 0:
                    self._wait_for_rate_limit()

                entries = client.list_entries(
                    filter_=query,
                    order_by=logging_v2.DESCENDING,
                    page_size=1000,
                )

                for entry in entries:
                    json_result = json.dumps(entry.to_api_repr())
                    result.append(json_result)
                    entry_count += 1

                return result

            except Exception as e:  # pylint: disable=broad-except
                last_exception = e

                if not self._should_retry(e):
                    logging.error("Non-retryable error for service %s: %s", service_name, e)
                    raise

                if attempt < max_retries:
                    retry_delay = self._calculate_retry_delay(attempt)
                    logging.warning(
                        "Retry %d/%d for service %s after %.2f s delay. Error: %s",
                        attempt + 1,
                        max_retries,
                        service_name,
                        retry_delay,
                        e,
                    )
                    time.sleep(retry_delay)
                else:
                    logging.error("Max retries exceeded for service %s: %s", service_name, e)

        if last_exception:
            raise last_exception

        raise RuntimeError(f"Failed  to retrieve logs for {service_name} after {max_retries} retries")

    def _wait_for_rate_limit(self) -> None:
        """Ensure we don't exceed rate limits by adding delay between requests."""
        current_time = time.time()
        min_request_interval = GCP_LOG_SYNCER_DEFAULT_DELAY
        # from: https://cloud.google.com/logging/quotas#api-limits
        time_since_last = current_time - self._last_request_time

        if time_since_last < min_request_interval:
            sleep_time = min_request_interval - time_since_last
            logging.info("Rate limiting: sleeping for %.2f seconds", sleep_time)
            time.sleep(sleep_time)

        self._last_request_time = time.time()

    def _should_retry(self, exception: Exception) -> bool:
        """Determine if we should retry based on the exception."""
        if isinstance(exception, google_api_exceptions.ResourceExhausted):
            # Check if it's a quota/rate limit error
            if "quota" in str(exception).lower() or "rate_limit" in str(exception).lower():
                return True
        return isinstance(exception, (google_api_exceptions.ServiceUnavailable, google_api_exceptions.DeadlineExceeded))

    def _calculate_retry_delay(self, attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
        """Calculate exponential backoff delay with jitter."""
        delay = min(base_delay * (2**attempt), max_delay)
        # Add jitter to prevent thundering herd
        jitter = random.uniform(0.1, 0.5) * delay
        return delay + jitter

    def get_logs_since(self, function_instance: str, since: datetime) -> list[str]:
        return self._log_filter(function_instance, start=since)

    def get_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        return self._log_filter(function_instance, start=start, end=end)

    def get_insights_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        return self._log_filter(function_instance, start=start, end=end)

    def query_metric(
        self,
        revision_name: str,
        metric_type: str,
        start: datetime,
        end: datetime,
        aligner: str | None = None,
    ) -> float | None:
        project_name = f"projects/{self._project_id}"

        filter_string = f'metric.type="{metric_type}" AND ' f'resource.labels.revision_name="{revision_name}"'

        count_filter_string = (
            'metric.type="run.googleapis.com/container/instance_count" AND '
            f'resource.labels.revision_name="{revision_name}"'
        )

        start = start - timedelta(minutes=BUFFER_GCP_METRICS_GRACE_PERIOD)
        start_timestamp = timestamp_pb2.Timestamp()  # pylint: disable=maybe-no-member
        start_timestamp.FromDatetime(dt=start)
        end_timestamp = timestamp_pb2.Timestamp()  # pylint: disable=maybe-no-member
        end_timestamp.FromDatetime(dt=end)

        interval = monitoring_v3.types.TimeInterval(start_time=start_timestamp, end_time=end_timestamp)

        request: dict[str, Any] = {
            "name": project_name,
            "filter": filter_string,
            "interval": interval,
            "view": monitoring_v3.types.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        count_request: dict[str, Any] = {
            "name": project_name,
            "filter": count_filter_string,
            "interval": interval,
            "view": monitoring_v3.types.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        if aligner:
            align = monitoring_v3.types.Aggregation.Aligner.ALIGN_NONE
            if aligner == "ALIGN_MAX":
                align = monitoring_v3.types.Aggregation.Aligner.ALIGN_MAX
            elif aligner == "ALIGN_SUM":
                align = monitoring_v3.types.Aggregation.Aligner.ALIGN_SUM
            elif aligner == "ALIGN_PERCENTILE_99":
                align = monitoring_v3.types.Aggregation.Aligner.ALIGN_PERCENTILE_99

            request["aggregation"] = monitoring_v3.types.Aggregation(
                alignment_period={"seconds": 60},
                per_series_aligner=align,  # type: ignore
            )

        count_request["aggregation"] = monitoring_v3.types.Aggregation(
            alignment_period={"seconds": 60},
            per_series_aligner=monitoring_v3.types.Aggregation.Aligner.ALIGN_MAX,  # type: ignore
        )

        try:
            result = self._monitoring_client.list_time_series(request=request)
            count_result = self._monitoring_client.list_time_series(request=count_request)

            for series, count_series in zip(result, count_result):
                if not series.points:
                    return None

                max_value = 0.0
                for point, container_count in zip(series.points, count_series.points):
                    value = point.value.double_value / max(container_count.value.int64_value, 1)
                    max_value = max(max_value, value)

                return max_value

        except google_api_exceptions.GoogleAPICallError as e:
            logger.warning("Failed to query metric '%s': %s", metric_type, e)

        return None

    def remove_messaging_topic(self, topic_identifier: str) -> None:
        publisher_client = self._pubsub_publisher_client
        subscriber_client = self._pubsub_subscriber_client

        # Get all subscriptions for the topic
        for subscription in subscriber_client.list_subscriptions(request={"project": f"projects/{self._project_id}"}):
            if subscription.topic == topic_identifier:
                try:
                    subscription_details = subscriber_client.get_subscription(subscription=subscription.name)
                    subscriber_client.delete_subscription(subscription=subscription.name)

                    if (
                        subscription_details.dead_letter_policy
                        and subscription_details.dead_letter_policy.dead_letter_topic
                    ):
                        try:
                            publisher_client.delete_topic(
                                topic=subscription_details.dead_letter_policy.dead_letter_topic
                            )
                        except google_api_exceptions.NotFound:
                            pass

                except google_api_exceptions.NotFound:
                    pass

        publisher_client.delete_topic(topic=topic_identifier)

    def get_topic_identifier(self, topic_name: str) -> str:
        publisher_client = self._pubsub_publisher_client
        topic_path = publisher_client.topic_path(self._project_id, topic_name)

        try:
            publisher_client.get_topic(topic=topic_path)
            return topic_path
        except google_api_exceptions.NotFound as e:
            raise RuntimeError(f"Topic {topic_name} not found") from e

    def remove_artifact_registry_repository(self, package_name: str) -> None:
        repo_id = "caribou"

        package_name = package_name.lower()
        package_name = package_name.replace("_", "-")

        full_package_name = self._artifact_registry_client.package_path(
            project=self._project_id, location=self._region, repository=repo_id, package=package_name
        )
        self._artifact_registry_client.delete_package(name=full_package_name)

    def artifact_registry_repository_exists(self, resource: Resource) -> bool:
        repo_id = "caribou"
        package_name = resource.name.lower()
        full_package_name = self._artifact_registry_client.package_path(
            project=self._project_id, location=self._region, repository=repo_id, package=package_name
        )

        try:
            self._artifact_registry_client.get_package(name=full_package_name)
            return True
        except google_api_exceptions.NotFound:
            return False

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
        cpu: int | None = None,
    ) -> None:
        # Step 1: Unzip the ZIP file
        zip_path = os.path.join(tmpdirname, "code.zip")
        with open(zip_path, "wb") as f_zip:
            f_zip.write(zip_contents)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdirname)

        # Step 2: Create a Dockerfile in the temporary directory
        dockerfile_content = self._generate_framework_dockerfile(handler, env_vars)
        with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
            f_dockerfile.write(dockerfile_content)

        # Step 3: Build the Docker Image
        image_name = f"{function_name.lower()}:latest"
        self._build_docker_image(tmpdirname, image_name)

        # Step 4: Upload the Image to Artifact Registry
        image_uri = self._upload_image_to_artifact_registry(image_name)

        # Step 5: Create the Cloud Run Service
        self._create_framework_cloud_run_function(
            function_name, image_uri, role_arn, timeout, memory_size, cpu, ephemeral_storage
        )

    def _generate_framework_dockerfile(self, handler: str, env_vars: dict) -> str:
        # Create ENV statements for each environment variable
        env_statements = "\n".join([f'ENV {key}="{value}"' for key, value in env_vars.items()])

        source_file = handler.split(".")[0] + ".py"
        target_function = handler.split(".")[-1]

        return f"""
        # Stage 1: Base image with Python 3.12 slim for installing Go
        FROM python:3.12-slim AS builder

        # Install essential packages for downloading and compiling Go
        RUN apt-get update && apt-get install -y curl tar gcc
        
        RUN apt-get update && \
            apt-get install -y --no-install-recommends \
                libsqlite3-0 \
            && rm -rf /var/lib/apt/lists/*

        # Download and extract Go 1.22.6
        RUN curl -LO https://go.dev/dl/go1.22.6.linux-amd64.tar.gz \
            && tar -C /usr/local -xzf go1.22.6.linux-amd64.tar.gz \
            && rm go1.22.6.linux-amd64.tar.gz

        # Set environment variables for Go
        ENV PATH="/usr/local/go/bin:$PATH"

        # Download and install the crane tool
        RUN curl -sL "https://github.com/google/go-containerregistry/releases/download/v0.20.2/go-containerregistry_Linux_x86_64.tar.gz" > go-containerregistry.tar.gz
        RUN tar -zxvf go-containerregistry.tar.gz -C /usr/local/bin/ gcrane

        COPY caribou-go ./caribou-go

        # Compile Go application
        RUN cd caribou-go && \
            chmod +x build_caribou_no_tests.sh && \
            ./build_caribou_no_tests.sh

        # Stage 2: Build the final image based on GCP python 3.12 runtime
        FROM {self._region}-docker.pkg.dev/serverless-runtimes/google-22/runtimes/python312

        # Copy the compiled Go application from the builder stage
        COPY --from=builder caribou-go caribou-go

        # Copy Go and GCrane binaries from the builder stage
        COPY --from=builder /usr/local/go /usr/local/go
        COPY --from=builder /usr/local/bin/gcrane /usr/local/bin/gcrane

        # Set up PATH and GOROOT environment variables
        ENV PATH="/usr/local/go/bin:/usr/local/bin:$PATH"
        ENV GOROOT=/usr/local/go

        # Install Poetry via pip
        USER root
        RUN apt-get update && apt-get install -y --no-install-recommends \
            build-essential \
            && pip3 install poetry \
            && rm -rf /var/lib/apt/lists/*

        # Copy Python dependency management files
        COPY pyproject.toml poetry.lock ./
        COPY README.md ./ 
        
        COPY caribou ./caribou
        
        # Configure Poetry settings and install dependencies
        RUN poetry config virtualenvs.create false
        RUN poetry install --only main

        # Declare environment variables
        {env_statements}
        ENV CARIBOU_DEFAULT_PROVIDER gcp

        # Copy application code
        COPY app.py ./

        # Command to run the application
        CMD ["functions-framework", \
        "--source", "{source_file}", \
        "--target", "{target_function}", \
        "--signature-type", "cloudevent"]
        """

    def _create_framework_cloud_run_function(
        self,
        function_name: str,
        image_uri: str,
        service_account_email: str,
        timeout: int,
        memory_size: int,
        cpu: float | None = None,
        ephemeral_storage: int = 0,
        env: dict[str, str] | None = None,
    ) -> str:
        # Ephemeral storage uses file system in memory
        # https://cloud.google.com/run/docs/container-contract#filesystem
        memory_and_storage = memory_size + ephemeral_storage
        if memory_and_storage > 32768:
            raise ValueError(
                f"Total amount of memory ({memory_size} MB) + ephemeral storage ({ephemeral_storage} MB)"
                f" exceeds 32,768 MB (current size = {memory_and_storage} MB)"
            )

        final_cpu = self._get_gcp_cpu_config(cpu, memory_and_storage)

        url = self._create_cloud_run_service(
            service_name=function_name,
            image_uri=image_uri,
            env=env or {},
            cpu=final_cpu,
            memory_mib=memory_and_storage,
            timeout_s=timeout if timeout >= 1 else 0,
            service_account_email=service_account_email,
            max_concurrency=80,
        )

        topic_path = self.create_pubsub_topic(f"{function_name}-topic")
        subscription_name = f"{function_name}-subscription"
        self.create_pubsub_subscription(topic_path, subscription_name, url, service_account_email, timeout)

        self.add_pubsub_permission_for_cloud_run(function_name, service_account_email)

        print(f"Caribou Lambda Framework remote cli function {function_name}" f" created successfully, with url: {url}")
        return url

    def get_timer_rule_schedule_expression(self, rule_name: str) -> Optional[str]:
        """Retrieves the schedule expression of a Cloud Scheduler job."""
        job_path = self._scheduling_client.job_path(self._project_id, self._region, rule_name)
        try:
            job = self._scheduling_client.get_job(name=job_path)
            return job.schedule
        except google_api_exceptions.NotFound:
            logger.info("Timer rule %s not found", rule_name)
            return None
        except google_api_exceptions.GoogleAPICallError as e:
            logger.info("Could not get timer rule %s: %s", rule_name, e)
            return None

    def remove_timer_rule(self, lambda_function_name: str, rule_name: str) -> None:
        """Remove the Cloud Scheduler job."""
        job_path = self._scheduling_client.job_path(self._project_id, self._region, rule_name)
        try:
            self._scheduling_client.delete_job(name=job_path)
            logger.info("Cloud Scheduler job %s deleted successfully", rule_name)
        except google_api_exceptions.NotFound:
            logger.info("Cloud Scheduler job %s not found. Maybe the rule is already deleted.", rule_name)
        except google_api_exceptions.GoogleAPICallError as e:
            logger.error("Error deleting the Cloud Scheduler job %s: %s", rule_name, e)

    def create_timer_rule(
        self, lambda_function_name: str, schedule_expression: str, rule_name: str, event_payload: str
    ) -> None:
        """
        Creates a Cloud Scheduler job that publishes a message to a Pub/Sub topic.
        This topic is assumed to be the one that triggers the target Cloud Run service.
        """
        # Get the remote CLI pub/sub topic
        client = self._pubsub_publisher_client
        topic_name = self.get_remote_cli_topic_name()
        topic_path = client.topic_path(self._project_id, topic_name)
        try:
            client.get_topic(topic=topic_path)
        except google_api_exceptions.NotFound as e:
            raise RuntimeError(f"Pub/sub topic {topic_name} for timer rule not found") from e

        job_path = self._scheduling_client.job_path(self._project_id, self._region, rule_name)

        pubsub_target = scheduler_v1.types.PubsubTarget(topic_name=topic_path, data=event_payload.encode("utf-8"))

        job = scheduler_v1.Job(
            {"name": job_path, "schedule": schedule_expression, "time_zone": "Etc/UTC", "pubsub_target": pubsub_target}
        )

        try:
            self._scheduling_client.create_job(parent=f"projects/{self._project_id}/locations/{self._region}", job=job)
            logger.info("Timer rule %s created successfully", rule_name)
        except google_api_exceptions.AlreadyExists:
            logger.info("Timer rule %s already exists, updating", rule_name)
            self._scheduling_client.update_job(job=job)
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Error creating timer rule {rule_name}: {e}") from e

    def get_remote_cli_topic_name(self) -> str:
        """Get the topic name for remote CLI commands"""
        remote_cli_name = REMOTE_CARIBOU_CLI_GCP_FUNCTION_NAME
        return f"{remote_cli_name}-topic"

    def invoke_remote_framework_internal_action(self, action_type: str, action_events: dict[str, Any]) -> None:
        payload = {
            "action": "internal_action",
            "type": action_type,
            "event": action_events,
        }

        self.invoke_remote_framework_with_payload(payload)

    def invoke_remote_framework_with_payload(
        self, payload: dict[str, Any], invocation_type: str = "RequestResponse"
    ) -> None:
        """
        Invokes the remote framework CLI (a Cloud Run service) via a pub/sub request.
        """
        # Get the remote cli url
        topic_name = self.get_remote_cli_topic_name()
        topic_path = self._pubsub_publisher_client.topic_path(self._project_id, topic_name)

        try:
            self._pubsub_publisher_client.get_topic(topic=topic_path)

            message_data = json.dumps(payload).encode("utf-8")
            result = self._pubsub_publisher_client.publish(topic_path, message_data)

            message_id = result.result()
            logger.info("Successfully invoked remote CLI. Message id: %s", message_id)

        except google_api_exceptions.NotFound as e:
            raise RuntimeError(f"Topic {topic_name} not found: e") from e
        except google_api_exceptions.GoogleAPICallError as e:
            logger.error("Pub/Sub invocation failed: %s", e)

    def event_bridge_permission_exists(self, lambda_function_name: str, statement_id: str) -> bool:
        # This method should not be reached, but it is here to satisfy the interface.
        raise NotImplementedError()

    def _get_gcp_cpu_config(self, cpu: float | None, memory: int) -> float:
        # CPU and Memory limits from:
        # https://cloud.google.com/run/docs/configuring/services/memory-limits#cpu-minimum
        # https://cloud.google.com/run/docs/configuring/services/cpu
        min_cpu_required = 1.0
        if memory > 4096:  # More than 4GB
            min_cpu_required = 2.0
        if memory > 8192:  # More than 8GB
            min_cpu_required = 4.0
        if memory > 16384:  # More than 16GB
            min_cpu_required = 6.0
        if memory > 24576:  # More than 24GB
            min_cpu_required = 8.0

        # Use the user-provided CPU value if it's higher than the minimum required.
        # Otherwise, use the calculated minimum. This prevents invalid combinations.
        final_cpu = max(cpu or 1.0, min_cpu_required)

        return final_cpu
