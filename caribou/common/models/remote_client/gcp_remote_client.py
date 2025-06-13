import json
import logging
import os
import subprocess
import tempfile
import time
import zipfile
from datetime import UTC, datetime, timedelta
from typing import Any, Optional

from google.api_core import exceptions as google_api_exceptions
from google.api_core.client_options import ClientOptions
from google.auth import default as google_auth_default
from google.cloud import (
    artifactregistry_v1,
    eventarc_v1,
    firestore,
    firestore_admin_v1,
    iam_admin_v1,
    logging_v2,
    pubsub_v1,
    resourcemanager_v3,
    run_v2,
    scheduler_v1,
    storage,
)
from google.cloud.iam_admin_v1 import IAMClient
from google.cloud.iam_admin_v1 import types as iam_admin_types
from google.iam.v1 import iam_policy_pb2, policy_pb2
from google.oauth2 import service_account
from google.protobuf import field_mask_pb2
from google.protobuf.field_mask_pb2 import FieldMask
from google.protobuf.json_format import MessageToDict

from caribou.common.constants import (
    CARIBOU_WORKFLOW_IMAGES_TABLE,
    DEPLOYMENT_RESOURCES_BUCKET,
    FIRESTORE_TTL_FIELD_NAME,
    GLOBAL_SYSTEM_REGION,
    REMOTE_CARIBOU_CLI_FUNCTION_NAME,
    SYNC_MESSAGES_TABLE,
    SYNC_PREDECESSOR_COUNTER_TABLE,
    SYNC_TABLE_TTL,
    SYNC_TABLE_TTL_ATTRIBUTE_NAME,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.utils import compress_json_str, decompress_json_str
from caribou.deployment.common.deploy.models.resource import Resource

logger = logging.getLogger(__name__)


# pylint: disable=too-many-lines
# pylint: disable=too-many-instance-attributes
class GCPRemoteClient(RemoteClient):  # pylint: disable=too-many-public-methods
    FUNCTION_CREATE_ATTEMPTS = 30
    DELAY_TIME = 5

    def __init__(
        self, project_id: str | None = None, region: str | None = None, credentials_path: str | None = None
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

        client_options = ClientOptions(api_endpoint=f"{self._region}-run.googleapis.com") if self._region else None
        self._run_client = run_v2.ServicesClient(credentials=self._credentials, client_options=client_options)
        self._storage_client = storage.Client(project=self._project_id, credentials=self._credentials)
        self._firestore_client = firestore.Client(credentials=self._credentials)
        self._firestore_admin_client = firestore_admin_v1.FirestoreAdminClient(credentials=self._credentials)
        self._pubsub_publisher_client = pubsub_v1.PublisherClient(credentials=self._credentials)
        self._pubsub_subscriber_client = pubsub_v1.SubscriberClient(credentials=self._credentials)
        self._eventarc_client = eventarc_v1.EventarcClient(credentials=self._credentials)
        self._artifact_registry_client = artifactregistry_v1.ArtifactRegistryClient(credentials=self._credentials)
        self._iam_admin_client = IAMClient(credentials=self._credentials)
        self._resource_manager_client = resourcemanager_v3.ProjectsClient(credentials=self._credentials)
        self._logging_client = logging_v2.Client(credentials=self._credentials)

        self._workflow_image_cache: dict[str, dict[str, str]] = {}
        # Allow for override of the deployment resources bucket (Due to S3 bucket name restrictions)
        self._deployment_resource_bucket: str = os.environ.get(
            "CARIBOU_OVERRIDE_DEPLOYMENT_RESOURCES_BUCKET", DEPLOYMENT_RESOURCES_BUCKET
        )

    def get_current_provider_region(self) -> str:
        return f"gcp_{self._region}"

    def get_service_account(self, name: str) -> str:
        full_account_name = f"projects/{self._project_id}/serviceAccounts/{name}"

        service_account_object = self._iam_admin_client.get_service_account(name=full_account_name)
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
        if resource.resource_type == "service_account":
            return self.service_account_exists(resource)
        if resource.resource_type == "cloud_run_service":
            return self.cloud_run_service_exists(resource)
        if resource.resource_type == "artifact_registry_repository":
            return self.artifact_registry_repository_exists(resource)
        if resource.resource_type == "pubsub_topic":
            return False
        raise RuntimeError(f"Unknown resource type {resource.resource_type}")

    def service_account_exists(self, resource: Resource) -> bool:
        return self.get_service_account(resource.name) is not None

    def cloud_run_service_exists(self, resource: Resource) -> bool:
        return self.get_cloud_run_service(resource.name) is not None

    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> tuple[list[bool], float, float]:
        client = self._firestore_client
        document = client.collection(SYNC_PREDECESSOR_COUNTER_TABLE).document(workflow_instance_id)
        tx = client.transaction()

        @firestore.transactional
        def _transaction(tx: firestore.Transaction):
            snap = document.get(transaction=tx)
            data = snap.to_dict()
            if not data:
                data = {}

            sync_map: dict[str, bool] = data.get(sync_node_name, {})

            if predecessor_name not in sync_map or direct_call:
                sync_map[predecessor_name] = direct_call

            data[sync_node_name] = sync_map
            data[FIRESTORE_TTL_FIELD_NAME] = datetime.now(UTC) + timedelta(seconds=SYNC_TABLE_TTL)
            # print(str(data[FIRESTORE_TTL_FIELD_NAME]))
            tx.set(document, data)
            return list(sync_map.values()), data

        bool_list, final_doc = _transaction(tx)
        final_doc["expires_at"] = str(final_doc["expires_at"])
        consumed_write_capacity = 0.0
        response_size = len(json.dumps(final_doc).encode()) / (1024**3)

        return bool_list, response_size, consumed_write_capacity

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
                    {"field": ttl_field, "update_mask": field_mask_pb2.FieldMask(paths=["ttl_config"])}
                )
                admin_client.update_field(request=request)

            print(f"table {table} created on database (default)")

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
        document = client.collection(SYNC_MESSAGES_TABLE).document(f"{function_name}:{workflow_instance_id}")

        ttl_time = datetime.now(UTC) + timedelta(seconds=SYNC_TABLE_TTL)

        tx = client.transaction()

        @firestore.transactional
        def _transaction(tx: firestore.Transaction):
            tx.set(
                document,
                {
                    "message": firestore.ArrayUnion([message]),
                    FIRESTORE_TTL_FIELD_NAME: ttl_time,
                },
                merge=True,
            )

        _transaction(tx)

        return 0.0

    def get_predecessor_data(
        self,
        current_instance_name: str,
        workflow_instance_id: str,
        consistent_read: bool = True,  # pylint: disable=unused-argument
    ) -> tuple[list[str], float]:
        client = self._firestore_client
        document_id = f"{current_instance_name}:{workflow_instance_id}"
        snap = client.collection(SYNC_MESSAGES_TABLE).document(document_id).get()

        if not snap.exists:
            return [], 0.0

        data = snap.to_dict() or {}
        messages: list[str] = data.get("message", [])
        consumed_read_capacity = 0.0
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
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        image_uri: str
        deployed_image_uri = self._get_deployed_image_uri(function_name)
        if deployed_image_uri:
            image_uri = deployed_image_uri
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

        service_account_email = self.get_service_account(role_identifier)

        service_url = self._create_cloud_run_service(
            service_name=function_name,
            image_uri=image_uri,
            env=environment_variables,
            cpu=1.0,
            memory_mib=memory_size,
            timeout_s=timeout,
            service_account_email=service_account_email,
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
    ) -> str:
        client = self._run_client
        parent = f"projects/{self._project_id}/locations/{self._region}"
        full_name = f"{parent}/services/{service_name}"

        container = run_v2.Container()
        container.image = image_uri
        container.env = [run_v2.EnvVar({"name": k, "value": v}) for k, v in env.items()]
        container.resources = run_v2.ResourceRequirements(limits={"cpu": str(cpu), "memory": f"{memory_mib}Mi"})

        template = run_v2.RevisionTemplate(
            {
                "max_instance_request_concurrency": 80,
                "containers": [container],
                "timeout_seconds": timeout_s,
                "service_account": service_account_email,
                "scaling": run_v2.RevisionScaling({"min_instance_count": 0, "max_instance_count": 100}),
            }
        )

        traffic_target = run_v2.TrafficTarget()
        traffic_target.percent = 100
        traffic_target.latest_revision = True

        svc = run_v2.Service()

        svc.name = full_name
        svc.template = template
        svc.traffic = [traffic_target]

        try:
            op = client.create_service(parent=parent, service=svc, service_id=service_name)
            logger.info("Cloud Run service %s created successfully.", service_name)
        except google_api_exceptions.AlreadyExists:
            op = client.update_service(service=svc)

        op.result()
        return client.get_service(name=full_name).uri

    def _store_deployed_image_uri(self, function_name: str, image_name: str) -> None:
        workflow_instance_id = "-".join(function_name.split("-")[0:2])

        function_name_simple = function_name[len(workflow_instance_id) + 1 :].rsplit("_", 1)[0]

        if workflow_instance_id not in self._workflow_image_cache:
            self._workflow_image_cache[workflow_instance_id] = {}

        self._workflow_image_cache[workflow_instance_id].update({function_name_simple: image_name})

        client = self._firestore_client
        document = client.collection(CARIBOU_WORKFLOW_IMAGES_TABLE).document(workflow_instance_id)
        document.set({function_name_simple: image_name}, merge=True)

    def _get_deployed_image_uri(self, function_name: str) -> str:
        workflow_instance_id = "-".join(function_name.split("-")[0:2])

        function_name_simple = function_name[len(workflow_instance_id) + 1 :].rsplit("_", 1)[0]

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
        return f"""
        FROM {self._region}-docker.pkg.dev/serverless-runtimes/google-22/runtimes/python312
        COPY requirements.txt ./
        {run_command}
        USER root
        RUN pip3 install --no-cache-dir -r requirements.txt
        USER app
        ENV ENABLE_PROFILER="true"
        COPY app.py ./
        COPY src ./src
        COPY caribou ./caribou
        CMD ["{handler}"]
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
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        deployed_image_uri = self._get_deployed_image_uri(function_name)
        if deployed_image_uri:
            image_uri = deployed_image_uri
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

        service_account_email = self.get_service_account(role_identifier)

        service_url = self._create_cloud_run_service(
            service_name=function_name,
            image_uri=image_uri,
            env=environment_variables,
            cpu=1.0,
            memory_mib=memory_size,
            timeout_s=timeout,
            service_account_email=service_account_email,
        )

        return service_url

    def remove_function(self, function_name: str) -> None:
        client = self._run_client
        full_path = f"projects/{self._project_id}/locations/{self._region}/services/{function_name}"
        try:
            response = client.delete_service(name=full_path)
            response.result()
        except google_api_exceptions.NotFound as e:
            print(f"Function {function_name} not found (maybe the function is already deleted): {e}")

    def create_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        policy_list = json.loads(policy)
        if "gcp" not in policy_list or "roles" not in policy_list["gcp"]:
            raise ValueError("Policy must contain 'gcp' and 'roles'")

        roles = policy_list["gcp"]["roles"]

        client = self._iam_admin_client
        project_client = self._resource_manager_client

        service_account_email = f"{role_name}@{self._project_id}.iam.gserviceaccount.com"
        service_account_name = f"projects/{self._project_id}/serviceAccounts/{service_account_email}"

        try:
            client.get_service_account(name=service_account_name)
        except google_api_exceptions.NotFound:
            client.create_service_account(
                name=f"projects/{self._project_id}",
                account_id=role_name,
                service_account=iam_admin_v1.ServiceAccount({"display_name": role_name}),
            )
            time.sleep(3)

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
        return self.get_service_account(service_account_email)

    def update_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        policy_list = json.loads(policy)
        if "gcp" not in policy_list or "roles" not in policy_list["gcp"]:
            raise ValueError("Policy must contain 'gcp' and 'roles'")

        roles = policy_list["gcp"]["roles"]

        client = self._iam_admin_client
        project_client = self._resource_manager_client

        service_account_email = f"{role_name}@{self._project_id}.iam.gserviceaccount.com"
        service_account_name = f"projects/{self._project_id}/serviceAccounts/{service_account_email}"

        try:
            client.get_service_account(name=service_account_name)
        except google_api_exceptions.NotFound:
            return self.create_role(role_name, policy, trust_policy)

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
        return self.get_service_account(service_account_email)

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
        except google_api_exceptions.NotFound:
            raise RuntimeError(f"Service account {service_account_email} not found")
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
        return response["name"]

    def subscribe_pubsub_topic(self, topic_path: str, subscription_name) -> None:
        client = self._pubsub_subscriber_client
        response = client.create_subscription(name=subscription_name, topic=topic_path)
        return response["name"]

    def add_pubsub_permission_for_cloud_run(self, service_name: str) -> None:
        client = self._run_client
        full_name = client.service_path(self._project_id, self._region, service_name)
        policy = client.get_iam_policy(request={"resource": full_name})
        invoker_member = f"serviceAccount:service-{self._project_id}@gcp-sa-pubsub.iam.gserviceaccount.com"

        for binding in policy.bindings:
            if binding.role == "roles/run.invoker" and invoker_member in binding.members:
                return

        policy.bindings.append({"role": "roles/run.invoker", "members": [invoker_member]})
        client.set_iam_policy(request={"resource": full_name, "policy": policy})

    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        client = self._pubsub_publisher_client
        topic_path = client.topic_path(self._project_id, identifier)
        client.publish(topic=topic_path, data=compress_json_str(message))

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
        if convert_to_bytes:
            doc.update({"value": compress_json_str(value)})
        else:
            doc.update({"value": value})

    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        client = self._firestore_client
        doc_ref = client.collection(table_name).document(key)

        update_data = {}
        for column, type_, value in column_type_value:
            if type_ == "S":
                update_data[column] = value
            else:
                update_data[column] = compress_json_str(value)

        doc_ref.set(update_data, merge=True)

    # TODO: GCP capacity = 1/64 RCU
    def get_value_from_table(self, table_name: str, key: str, consistent_read: bool = True) -> tuple[str, float]:
        client = self._firestore_client
        doc = client.collection(table_name).document(key).get()

        consumed_read_capacity = 0.0

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
            print(f"Key {key} not found: {e}")
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
            print(f"Key {key} not found: {e}")
        except google_api_exceptions.GoogleAPICallError as e:
            raise RuntimeError(f"Could not delete resource {key} from database: {e}") from e

    def _log_filter(
            self,
            service_name: str,
            start: datetime | None = None,
            end: datetime | None = None,
            *,
            insights: bool = False
    ) -> list[str]:
        if start:
            time_start = start.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        else:
            time_start = None

        if end:
            time_end = end.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        else:
            time_end = None

        resource_filter = (
            'resource.type="cloud_run_revision" '
            f'resource.labels.service_name="{service_name}"'
        )

        if insights:
            resource_filter = (
                'resource.type="cloud_run_revision" '
                'jsonPayload.@type="type.googleapis.com/google.cloud.run.v1.Revision"'
            )

        query = (
            f'{resource_filter} '
            f'timestamp>="{time_start}" ' if start else ''
            f'timestamp<="{time_end}"' if end else ''
        )

        client = self._logging_client
        entries = client.list_entries(filter_=query, order_by=logging_v2.DESCENDING)

        return [entry.payload.get("message", str(entry.payload)) for entry in entries]

    def get_logs_since(self, function_instance: str, since: datetime) -> list[str]:
        return self._log_filter(function_instance, start=since)

    def get_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        return self._log_filter(function_instance, start=start, end=end)

    def get_insights_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        return self._log_filter(function_instance, start=start, end=end, insights=True)

    def remove_messaging_topic(self, topic_identifier: str) -> None:
        publisher_client = self._pubsub_publisher_client
        subscriber_client = self._pubsub_subscriber_client

        # Get all subscriptions for the topic
        for subscription in subscriber_client.list_subscriptions(request={"project": f"projects/{self._project_id}"}):
            if subscription.topic == topic_identifier:
                try:
                    subscriber_client.delete_subscription(subscription=subscription.name)
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
            raise RuntimeError(f"Topic {topic_name} not found")

    def remove_artifact_registry_repository(self, repository_name: str) -> None:
        repository_name = repository_name.lower()
        full_repo_name = self._artifact_registry_client.repository_path(
            project=self._project_id, location=self._region, repository=repository_name
        )
        self._artifact_registry_client.delete_repository(name=full_repo_name)

    def artifact_registry_repository_exists(self, resource: Resource) -> bool:
        repository_name = resource.name.lower()
        full_repo_name = self._artifact_registry_client.repository_path(
            project=self._project_id, location=self._region, repository=repository_name
        )

        try:
            self._artifact_registry_client.get_repository(name=full_repo_name)
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

        # Step 4: Upload the Image to ECR
        image_uri = self._upload_image_to_artifact_registry(image_name)
        self._create_framework_lambda_function(
            function_name, image_uri, role_arn, timeout, memory_size, ephemeral_storage
        )

    # TODO: comment out for now, too complicated
    def _generate_framework_dockerfile(self, handler: str, env_vars: dict) -> str:
        # Create ENV statements for each environment variable
        env_statements = "\n".join([f'ENV {key}="{value}"' for key, value in env_vars.items()])

        return f"""
        # Stage 1: Base image with Python 3.12 slim for installing Go
        FROM python:3.12-slim AS builder

        # Install essential packages for downloading and compiling Go
        RUN apt-get update && apt-get install -y curl tar gcc

        # Download and extract Go 1.22.6
        RUN curl -LO https://go.dev/dl/go1.22.6.linux-amd64.tar.gz \
            && tar -C /usr/local -xzf go1.22.6.linux-amd64.tar.gz \
            && rm go1.22.6.linux-amd64.tar.gz

        # Set environment variables for Go
        ENV PATH="/usr/local/go/bin:$PATH"

        # Download and install the crane tool
        RUN curl -sL "https://github.com/google/go-containerregistry/releases/download/v0.20.2/go-containerregistry_Linux_x86_64.tar.gz" > go-containerregistry.tar.gz
        RUN tar -zxvf go-containerregistry.tar.gz -C /usr/local/bin/ crane

        COPY caribou-go ./caribou-go

        # Compile Go application
        RUN cd caribou-go && \
            chmod +x build_caribou_no_tests.sh && \
            ./build_caribou_no_tests.sh

        # Stage 2: Build the final image based on GCP python 3.12 runtime
        FROM {self._region}-docker.pkg.dev/serverless-runtimes/google-22/runtimes/python312

        # Copy the compiled Go application from the builder stage
        COPY --from=builder caribou-go caribou-go

        # Copy Go and Crane binaries from the builder stage
        COPY --from=builder /usr/local/go /usr/local/go
        COPY --from=builder /usr/local/bin/crane /usr/local/bin/crane

        # Set up PATH and GOROOT environment variables
        ENV PATH="/usr/local/go/bin:/usr/local/bin:$PATH"
        ENV GOROOT=/usr/local/go

        # Install Poetry via pip
        RUN pip3 install poetry

        # Copy Python dependency management files
        COPY pyproject.toml poetry.lock ./
        COPY README.md ./ 
        
        COPY caribou ./caribou
        
        # Configure Poetry settings and install dependencies
        RUN poetry config virtualenvs.create false
        RUN poetry install --only main

        # Declare environment variables
        {env_statements}

        # Copy application code
        COPY app.py ./

        # Command to run the application
        CMD ["{handler}"]
        """

    def _create_framework_lambda_function(
        self, function_name: str, image_uri: str, role: str, timeout: int, memory_size: int, ephemeral_storage_size: int
    ) -> str:
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "Role": role,
            "Code": {"ImageUri": image_uri},
            "PackageType": "Image",
            "Timeout": timeout,
            "MemorySize": memory_size,
            "EphemeralStorage": {"Size": ephemeral_storage_size},
        }
        if timeout >= 1:
            kwargs["Timeout"] = timeout
        arn = self._create_cloud_run_service(
            service_name=function_name,
            image_uri=image_uri,

        )

        print(f"Caribou Lambda Framework remote cli function {function_name}" f" created successfully, with ARN: {arn}")

        return arn

    def get_timer_rule_schedule_expression(self, rule_name: str) -> Optional[str]:
        """Retrieve the schedule expression of a timer rule if it exist."""
        try:
            events_client = self._client("events")

            # Describe the rule using the EventBridge client
            rule_details = events_client.describe_rule(Name=rule_name)

            # Return the rule schedule expression
            return rule_details.get("ScheduleExpression")
        except ClientError as e:
            # Check if its ResourceNotFoundException, which means the rule doesn't exist
            # We don't need to do anything in this case
            if not e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"Error removing the EventBridge rule {rule_name}: {e}")

            return None

    def remove_timer_rule(self, lambda_function_name: str, rule_name: str) -> None:
        """Remove the EventBridge rule and its associated targets."""
        try:
            events_client = self._client("events")

            # Remove the targets from the rule
            events_client.remove_targets(Rule=rule_name, Ids=[f"{lambda_function_name}-target"])

            # Delete the rule itself
            events_client.delete_rule(
                Name=rule_name, Force=True  # Ensures the rule is deleted even if it's still in use
            )
        except ClientError as e:
            # Check if its ResourceNotFoundException, which means the rule doesn't exist
            # We don't need to do anything in this case
            if not e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"Error removing the EventBridge rule {rule_name}: {e}")

    def event_bridge_permission_exists(self, lambda_function_name: str, statement_id: str) -> bool:
        """Check if a specific permission exists in the Lambda function's policy based on the StatementId."""
        try:
            lambda_client = self._client("lambda")

            # Get the current policy for the Lambda function
            policy_response = lambda_client.get_policy(FunctionName=lambda_function_name)

            # Parse the policy JSON
            policy_statements = json.loads(policy_response["Policy"])["Statement"]

            # Check if a permission with the given StatementId exists
            for statement in policy_statements:
                if statement["Sid"] == statement_id:
                    return True

            return False  # If no matching StatementId is found, return False
        except ClientError as e:
            if not e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"Error in asserting if permission exists {lambda_function_name} - {statement_id}: {e}")

            return False

    def create_timer_rule(
        self, lambda_function_name: str, schedule_expression: str, rule_name: str, event_payload: str
    ) -> None:
        # Initialize the EventBridge and Lambda clients
        events_client = self._client("events")
        lambda_client = self._client("lambda")

        # Create a rule with the specified schedule expression
        response = events_client.put_rule(Name=rule_name, ScheduleExpression=schedule_expression, State="ENABLED")
        rule_arn = response["RuleArn"]

        # Add permission for EventBridge to invoke the Lambda function
        statement_id = f"{rule_name}-invoke-lambda"
        if not self.event_bridge_permission_exists(lambda_function_name, statement_id):
            lambda_client.add_permission(
                FunctionName=lambda_function_name,
                StatementId=statement_id,
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=rule_arn,
            )

        # Get the ARN of the Lambda function
        lambda_arn = self.get_cloud_run_service(lambda_function_name)["FunctionArn"]

        # Attach the Lambda function to the rule
        events_client.put_targets(
            Rule=rule_name,
            Targets=[{"Id": f"{lambda_function_name}-target", "Arn": lambda_arn, "Input": event_payload}],
        )

    def invoke_remote_framework_internal_action(self, action_type: str, action_events: dict[str, Any]) -> None:
        payload = {
            "action": "internal_action",
            "type": action_type,
            "event": action_events,
        }

        self.invoke_remote_framework_with_payload(payload, invocation_type="Event")

    def invoke_remote_framework_with_payload(self, payload: dict[str, Any], invocation_type: str = "Event") -> None:
        # Get the boto3 lambda client
        lambda_client = self._client("lambda")
        remote_framework_cli_name = REMOTE_CARIBOU_CLI_FUNCTION_NAME

        # Invoke the lambda function with the payload
        lambda_client.invoke(
            FunctionName=remote_framework_cli_name, InvocationType=invocation_type, Payload=json.dumps(payload)
        )


if __name__ == "__main__":
    gcp_remote_client = GCPRemoteClient(project_id="caribou-460422", region="us-east1")
    # print(gcp_remote_client.get_service_account(name="809845967121-compute@developer.gserviceaccount.com"))
    iam_policy = """
    {
      "gcp": {
        "roles": [
          "roles/logging.logWriter",
          "roles/pubsub.publisher"
        ]
      }
    }
    """

    iam_policy_2 = """
    {
      "gcp": {
        "roles": [
          "roles/logging.logWriter"
        ]
      }
    }
    """
    # gcp_remote_client.create_sync_tables()
    # db = firestore.Client()
    # doc = db.collection("sync_messages_table").document("dummy")
    # doc.set({"data":"ping", "expires_at": datetime.utcnow()})
    # sa = gcp_remote_client.create_role("caribou-runtime", iam_policy, {})
    # sa = gcp_remote_client.update_role("caribou-runtime", iam_policy_2, {})
    # gcp_remote_client.remove_role("caribou-runtime")
    # sa = gcp_remote_client.get_cloud_run_service(service_name="visualize")
    # print("SA:", sa)
    # os.environ["FIRESTORE_EMULATOR_HOST"] = "localhost:8787"
    # gcp_remote_client.set_predecessor_reached("A", "sync1", "wf-X", False)
    # gcp_remote_client.set_predecessor_reached("B", "sync1", "wf-X", True)
    #
    # db = firestore.Client(project="demo-proj")
    # doc = db.collection("caribou_sync_counters").document("wf-X").get()
    # print(doc.to_dict())
    # gcp_remote_client.upload_predecessor_data_at_sync_node("fnA", "wf-1", "hello")
    # gcp_remote_client.upload_predecessor_data_at_sync_node("fnA", "wf-1", "world")
    #
    # doc = gcp_remote_client._firestore_client.collection(SYNC_MESSAGES_TABLE) \
    #     .document("fnA:wf-1").get().to_dict()

    # msgs, cap = gcp_remote_client.get_predecessor_data("fnA", "wf-1")
    # print(msgs)
    # print(cap)
