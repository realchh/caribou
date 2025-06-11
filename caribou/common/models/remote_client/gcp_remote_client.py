# TODO: implement automatic deployment to GCP
import json
import logging
import os
import subprocess
import tempfile
import time
import zipfile
from datetime import datetime
from typing import Any, Optional

from google.api_core import exceptions as google_api_exceptions
from google.api_core.client_options import ClientOptions
from google.auth import default as google_auth_default
from google.cloud import (
    artifactregistry_v1,
    eventarc_v1,
    firestore,
    iam_admin_v1,
    logging_v2,
    pubsub_v1,
    run_v2,
    scheduler_v1,
    storage,
    resourcemanager_v3
)
from google.cloud.iam_admin_v1 import IAMClient
from google.cloud.iam_admin_v1 import types as iam_admin_types
from google.iam.v1 import iam_policy_pb2, policy_pb2
from google.oauth2 import service_account
from google.protobuf.json_format import MessageToDict

from caribou.common.constants import (
    CARIBOU_WORKFLOW_IMAGES_TABLE,
    DEPLOYMENT_RESOURCES_BUCKET,
    DEPLOYMENT_RESOURCES_TABLE,
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
        self._pubsub_publisher_client = pubsub_v1.PublisherClient(credentials=self._credentials)
        self._pubsub_subscriber_client = pubsub_v1.SubscriberClient(credentials=self._credentials)
        self._eventarc_client = eventarc_v1.EventarcClient(credentials=self._credentials)
        self._artifact_registry_client = artifactregistry_v1.ArtifactRegistryClient(credentials=self._credentials)
        self._iam_admin_client = IAMClient(credentials=self._credentials)
        self._resource_manager_client = resourcemanager_v3.ProjectsClient(credentials=self._credentials)

        self._client_cache: dict[str, Any] = {}
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
        Retrieves a Cloud Run service and formats its configuration as a dictionary
        that closely mirrors the original AWS Lambda configuration structure.
        """
        service_object = self.get_cloud_run_service_object(service_name)
        if not service_object:
            return None

        # The MessageToDict function converts the protobuf object into a Python dict.
        # This gives us a raw, complete dictionary representation of the service.
        service_dict = MessageToDict(service_object._pb)

        # Now, let's format this raw dictionary into the desired structure.
        return self._format_service_dict(service_dict)

    def _format_service_dict(self, service_dict: dict) -> dict[str, Any]:
        """
        A helper method to translate a Cloud Run service dictionary into a format
        that resembles the AWS Lambda Configuration dictionary.
        """
        # Get the container configuration, defaulting to an empty dict if not present
        container = service_dict.get("template", {}).get("containers", [{}])[0]
        template = service_dict.get("template", {})

        # Extract environment variables into a simple {key: value} dict
        env_vars = {env["name"]: env.get("value", "") for env in container.get("env", [])}

        # The formatted dictionary
        formatted_config = {
            # GCP Equivalent Fields
            "ServiceName": service_dict.get("name", "").split("/")[-1],
            "ServiceArn": service_dict.get("name"),  # The full resource name is the closest to an ARN
            "ServiceUri": service_dict.get("uri"),
            "ImageUri": container.get("image"),
            "Role": template.get("serviceAccount"),
            "MemorySize": container.get("resources", {}).get("limits", {}).get("memory"),
            "CpuLimit": container.get("resources", {}).get("limits", {}).get("cpu"),
            "Timeout": template.get("timeout"),
            "Environment": {"Variables": env_vars},
            "LastModified": service_dict.get("updateTime"),
            "CreateTime": service_dict.get("createTime"),
            "Scaling": {
                "MinInstances": template.get("scaling", {}).get("minInstanceCount"),
                "MaxInstances": template.get("scaling", {}).get("maxInstanceCount"),
            },
            # Original AWS keys for reference (commented out or set to None)
            # "FunctionName": service_dict.get("name", "").split("/")[-1],
            # "FunctionArn": service_dict.get("name"),
            # "Runtime": None, # Not applicable for containers
            # "Handler": None, # Not applicable for containers
        }
        return formatted_config

    # The original method that returns the object itself is still useful
    def get_cloud_run_service_object(self, service_name: str) -> run_v2.Service | None:
        """
        Retrieves a Cloud Run service by its name.
        Returns the Service object or None if not found.
        """
        full_service_name = self._run_client.service_path(
            project=self._project_id, location=self._region, service=service_name
        )
        try:
            request = run_v2.GetServiceRequest(name=full_service_name)
            service_object = self._run_client.get_service(request=request)
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

    # TODO: Check the functionality and modify this
    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> tuple[list[bool], float, float]:
        client = self._firestore_client

        # Calculate the expiration time for the sync node
        expiration_time = int(time.time()) + SYNC_TABLE_TTL

        # Record the consumed capacity (Write Capacity Units) for this operation
        # Refer to: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html
        consumed_write_capacity = 0.0

        # Check if the map exists and create it if not
        mc_response = client.update_item(
            TableName=SYNC_PREDECESSOR_COUNTER_TABLE,
            Key={"id": {"S": workflow_instance_id}},
            UpdateExpression="SET #s = if_not_exists(#s, :empty_map), #ttl = :ttl",
            ExpressionAttributeNames={
                "#s": sync_node_name,
                "#ttl": SYNC_TABLE_TTL_ATTRIBUTE_NAME,  # TTL attribute
            },
            ExpressionAttributeValues={
                ":empty_map": {"M": {}},
                ":ttl": {"N": str(expiration_time)},  # TTL as a number
            },
            ReturnConsumedCapacity="TOTAL",
        )

        consumed_write_capacity += mc_response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

        # Update the map with the new predecessor_name and direct_call
        if not direct_call:
            response = client.update_item(
                TableName=SYNC_PREDECESSOR_COUNTER_TABLE,
                Key={"id": {"S": workflow_instance_id}},
                UpdateExpression="SET #s.#p = if_not_exists(#s.#p, :direct_call)",
                ExpressionAttributeNames={"#s": sync_node_name, "#p": predecessor_name},
                ExpressionAttributeValues={":direct_call": {"BOOL": direct_call}},
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity="TOTAL",
            )
        else:
            response = client.update_item(
                TableName=SYNC_PREDECESSOR_COUNTER_TABLE,
                Key={"id": {"S": workflow_instance_id}},
                UpdateExpression="SET #s.#p = :direct_call",
                ExpressionAttributeNames={"#s": sync_node_name, "#p": predecessor_name},
                ExpressionAttributeValues={":direct_call": {"BOOL": direct_call}},
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity="TOTAL",
            )

        consumed_write_capacity += response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

        # Measure the size of the response
        response_size = len(json.dumps(response).encode("utf-8")) / (1024**3)

        return (
            [item["BOOL"] for item in response["Attributes"][sync_node_name]["M"].values()],
            response_size,
            consumed_write_capacity,
        )

    # TODO: move this from dynamodb to firestore
    def create_sync_tables(self) -> None:
        # Check if table exists
        client = self._client("dynamodb")
        for table in [SYNC_MESSAGES_TABLE, SYNC_PREDECESSOR_COUNTER_TABLE]:
            try:
                client.describe_table(TableName=table)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    client.create_table(
                        TableName=table,
                        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                        BillingMode="PAY_PER_REQUEST",
                    )
                else:
                    raise

        # Setup TTL for the sync tables
        self._setup_ttl_for_sync_tables()

    # TODO: move this from dynamodb to firestore
    def _setup_ttl_for_sync_tables(self) -> None:
        # Now also enable the expiration of items in the table
        client = self._client("dynamodb")
        for table in [SYNC_MESSAGES_TABLE, SYNC_PREDECESSOR_COUNTER_TABLE]:
            # Check if the table creation is complete (Wait for table to be created)
            client.get_waiter("table_exists").wait(TableName=table)

            # Check if TTL is already enabled
            ttl_description = client.describe_time_to_live(TableName=table)
            ttl_status = ttl_description.get("TimeToLiveDescription", {}).get("TimeToLiveStatus", "DISABLED")

            if ttl_status != "ENABLED":
                # Now also enable the expiration of items in the table
                client.update_time_to_live(
                    TableName=table,
                    TimeToLiveSpecification={"Enabled": True, "AttributeName": SYNC_TABLE_TTL_ATTRIBUTE_NAME},
                )

    # TODO: move this from dynamodb to firestore
    def upload_predecessor_data_at_sync_node(
        self, function_name: str, workflow_instance_id: str, message: str
    ) -> float:
        client = self._client("dynamodb")

        # Calculate the expiration time for the sync node
        expiration_time = int(time.time()) + SYNC_TABLE_TTL

        sync_node_id = f"{function_name}:{workflow_instance_id}"
        response = client.update_item(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": sync_node_id}},
            UpdateExpression="ADD #M :m SET #ttl = :ttl",
            ExpressionAttributeNames={
                "#M": "message",
                "#ttl": SYNC_TABLE_TTL_ATTRIBUTE_NAME,  # TTL attribute
            },
            ExpressionAttributeValues={
                ":m": {"SS": [message]},
                ":ttl": {"N": str(expiration_time)},
            },
            ReturnConsumedCapacity="TOTAL",
        )

        return response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

    # TODO: move this from dynamodb to firestore
    def get_predecessor_data(
        self,
        current_instance_name: str,
        workflow_instance_id: str,
        consistent_read: bool = True,  # pylint: disable=unused-argument
    ) -> tuple[list[str], float]:
        client = self._client("dynamodb")
        sync_node_id = f"{current_instance_name}:{workflow_instance_id}"
        # Currently we use strongly consistent reads for the sync messages
        # Refer to: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html
        response = client.get_item(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": sync_node_id}},
            ReturnConsumedCapacity="TOTAL",
            ConsistentRead=consistent_read,
        )

        # Record the consumed capacity (Read Capacity Units) for the sync node
        consumed_read_capacity = response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

        if "Item" not in response:
            return [], consumed_read_capacity

        item = response.get("Item")
        if item is not None and "message" in item:
            return item["message"]["SS"], consumed_read_capacity

        return [], consumed_read_capacity

    # TODO: i think this does not need much change
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
                dockerfile_content = self._generate_dockerfile(runtime, handler, additional_docker_commands)
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
            service_name = function_name,
            image_uri = image_uri,
            env = environment_variables,
            cpu = 1.0,
            memory_mib = memory_size,
            timeout_s = timeout,
            service_account_email = service_account_email,
        )

        return service_url

    def _create_cloud_run_service(self, service_name: str, image_uri: str, env: dict[str, str],
                                  cpu: float, memory_mib: int, timeout_s: int, service_account_email: str) -> str:
        client = self._run_client
        parent = f"projects/{self._project_id}/locations/{self._region}"
        full_name = f"{parent}/services/{service_name}"

        container = run_v2.Container()
        container.image = image_uri
        container.env = [run_v2.EnvVar({"name": k, "value": v}) for k, v in env.items()]
        container.resources = run_v2.ResourceRequirements(limits = {"cpu": str(cpu), "memory": f"{memory_mib}Mi"})

        template = run_v2.RevisionTemplate({
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
            op = client.update_service(service = svc)

        op.result()
        return client.get_service(name=full_name).uri

    # TODO: move from dynamodb to firestore, logic does not need much changes
    def _store_deployed_image_uri(self, function_name: str, image_name: str) -> None:
        workflow_instance_id = "-".join(function_name.split("-")[0:2])

        function_name_simple = function_name[len(workflow_instance_id) + 1 :].rsplit("_", 1)[0]

        if workflow_instance_id not in self._workflow_image_cache:
            self._workflow_image_cache[workflow_instance_id] = {}

        self._workflow_image_cache[workflow_instance_id].update({function_name_simple: image_name})

        client = self._session.client("dynamodb", region_name=GLOBAL_SYSTEM_REGION)

        # Check if the item exists and create dictionary if not
        client.update_item(
            TableName=CARIBOU_WORKFLOW_IMAGES_TABLE,
            Key={"key": {"S": workflow_instance_id}},
            UpdateExpression="SET #v = if_not_exists(#v, :empty_map)",
            ExpressionAttributeNames={"#v": "value"},
            ExpressionAttributeValues={":empty_map": {"M": {}}},
        )

        client.update_item(
            TableName=CARIBOU_WORKFLOW_IMAGES_TABLE,
            Key={"key": {"S": workflow_instance_id}},
            UpdateExpression="SET #v.#f = :value",
            ExpressionAttributeNames={"#v": "value", "#f": function_name_simple},
            ExpressionAttributeValues={":value": {"S": image_name}},
        )

    # TODO: move from dynamodb to firestore, logic does not need much changes
    def _get_deployed_image_uri(self, function_name: str, consistent_read: bool = True) -> str:
        workflow_instance_id = "-".join(function_name.split("-")[0:2])

        function_name_simple = function_name[len(workflow_instance_id) + 1 :].rsplit("_", 1)[0]

        if function_name_simple in self._workflow_image_cache.get(workflow_instance_id, {}):
            return self._workflow_image_cache[workflow_instance_id][function_name_simple]

        client = self._session.client("dynamodb", region_name=GLOBAL_SYSTEM_REGION)

        response = client.get_item(
            TableName=CARIBOU_WORKFLOW_IMAGES_TABLE,
            Key={"key": {"S": workflow_instance_id}},
            ConsistentRead=consistent_read,
        )

        if "Item" not in response:
            return ""

        item = response.get("Item")
        if item is not None and "value" in item:
            if workflow_instance_id not in self._workflow_image_cache:
                self._workflow_image_cache[workflow_instance_id] = {}
            self._workflow_image_cache[workflow_instance_id].update(
                {function_name_simple: item["value"]["M"].get(function_name_simple, {}).get("S", "")}
            )
            return item["value"]["M"].get(function_name_simple, {}).get("S", "")
        return ""

    # TODO: change from aws lambda insights to cloud run insights or whatever it is called
    def _generate_dockerfile(self, runtime: str, handler: str, additional_docker_commands: Optional[list[str]]) -> str:
        run_command = ""
        if additional_docker_commands and len(additional_docker_commands) > 0:
            run_command += " && ".join(additional_docker_commands)
        if len(run_command) > 0:
            run_command = f"RUN {run_command}"

        return f"""
        COPY requirements.txt ./
        {run_command}
        RUN pip3 install --no-cache-dir -r requirements.txt
        ENV ENABLE_PROFILER="true"
        COPY app.py ./
        COPY src ./src
        COPY caribou ./caribou
        CMD ["{handler}"]
        """

    # TODO: nothing to be changed i think
    def _build_docker_image(self, context_path: str, image_name: str) -> None:
        try:
            subprocess.run(["docker", "build", "--platform", "linux/amd64", "-t", image_name, context_path], check=True)
            logger.info("Docker image %s built successfully.", image_name)
        except subprocess.CalledProcessError as e:
            # This will catch errors from the subprocess and logger.info a message.
            logger.error("Failed to build Docker image %s. Error: %s", image_name, e)

    def _ensure_repository(self, repository_name: str) -> str:
        """
        Returns the full resource name of the repository (if exists). If it doesn't exist, it will be created.
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
            repository.format_=artifactregistry_v1.Repository.Format.DOCKER
            repository.description="Caribou build artifacts"
            client.create_repository(
                parent=f"projects/{self._project_id}/locations/{self._region}",
                repository_id=repository_name,
                repository=repository
            )

        return full_repo

    def _upload_image_to_artifact_registry(self, image_name: str) -> str:
        repo_id = "caribou" # Base artifact registry repo to hold the docker images used for deployment
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

                dockerfile_content = self._generate_dockerfile(runtime, handler, additional_docker_commands)
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
                name = f"projects/{self._project_id}",
                account_id=role_name,
                service_account=iam_admin_v1.ServiceAccount({"display_name": role_name}),
            )
            print("Created service account: ", service_account_email)

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

        project_client.set_iam_policy(
            request={"resource": f"projects/{self._project_id}",
                     "policy": project_policy}
        )
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
            client.create_service_account(
                name = f"projects/{self._project_id}",
                account_id=role_name,
                service_account=iam_admin_v1.ServiceAccount({"display_name": role_name}),
            )
            print("Created service account: ", service_account_email)

        project_policy = project_client.get_iam_policy(resource=f"projects/{self._project_id}")

        policy_member = f"serviceAccount:{service_account_email}"

        existing_roles = {binding.role: binding for binding in project_policy.bindings if policy_member in binding.members}

        print("desired roles: ", set(roles))
        print("existing roles: ", existing_roles)
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

        project_client.set_iam_policy(
            request={"resource": f"projects/{self._project_id}",
                     "policy": project_policy}
        )
        return self.get_service_account(service_account_email)

    # TODO: Create pubsub topic
    def create_sns_topic(self, topic_name: str) -> str:
        client = self._pubsub_publisher_client
        # If topic exists, the following will return the existing topic
        response = client.create_topic(name=topic_name)
        return response["name"]

    # TODO: create pubsub subscription
    def subscribe_sns_topic(self, topic_path: str, subscription_name) -> None:
        client = self._pubsub_subscriber_client
        response = client.create_subscription(name = subscription_name, topic = topic_path)
        return response["name"]

    def add_pubsub_permission_for_cloud_run(self, service_name: str) -> None:
        client = self._run_client
        full_name = client.service_path(self._project_id, self._region, service_name)
        policy = client.get_iam_policy(request={"resource": full_name})
        invoker_member = f"serviceAccount:service-{self._project_id}@gcp-sa-pubsub.iam.gserviceaccount.com"

        for binding in policy.bindings:
            if binding["role"] == "roles/run.invoker" and invoker_member in binding["members"]:
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

    def get_value_from_table(self, table_name: str, key: str, consistent_read: bool = True) -> tuple[str, float]:
        client = self._firestore_client
        doc = client.collection(table_name).document(key).get()

        # TODO: GCP capacity = 1/64 RCU
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

    # TODO: upload resource to google cloud storage
    def upload_resource(self, key: str, resource: bytes) -> None:
        client = self._client("s3")
        try:
            client.put_object(Body=resource, Bucket=self._deployment_resource_bucket, Key=key)
        except ClientError as e:
            raise RuntimeError(
                f"Could not upload resource {key} to S3, does the bucket {self._deployment_resource_bucket} exist and do you have permission to access it: {str(e)}"  # pylint: disable=line-too-long
            ) from e

    # TODO: download resource from google cloud storage
    def download_resource(self, key: str) -> bytes:
        client = self._client("s3")
        try:
            response = client.get_object(Bucket=self._deployment_resource_bucket, Key=key)
        except ClientError as e:
            raise RuntimeError(
                f"Could not upload resource {key} to S3, does the bucket {self._deployment_resource_bucket} exist and do you have permission to access it: {str(e)}"  # pylint: disable=line-too-long
            ) from e
        return response["Body"].read()

    # TODO: get firestore table keys
    def get_keys(self, table_name: str) -> list[str]:
        client = self._client("dynamodb")
        response = client.scan(TableName=table_name)
        if "Items" not in response:
            return []
        items = response.get("Items")
        if items is not None:
            return [item["key"]["S"] for item in items]
        return []

    # TODO: logging using monitoring v3
    def get_logs_since(self, function_instance: str, since: datetime) -> list[str]:
        time_ms_since_epoch = int(time.mktime(since.timetuple())) * 1000
        client = self._client("logs")

        next_token = None

        log_events: list[str] = []
        while True:
            if next_token:
                response = client.filter_log_events(
                    logGroupName=f"/aws/lambda/{function_instance}",
                    startTime=time_ms_since_epoch,
                    nextToken=next_token,
                )
            else:
                try:
                    response = client.filter_log_events(
                        logGroupName=f"/aws/lambda/{function_instance}", startTime=time_ms_since_epoch
                    )
                except client.exceptions.ResourceNotFoundException:
                    # No logs found
                    return []

            log_events.extend(event["message"] for event in response.get("events", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        return log_events

    # TODO: logging using monitoring v3
    def get_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        time_ms_start = int(start.timestamp() * 1000)
        time_ms_end = int(end.timestamp() * 1000)
        client = self._client("logs")

        next_token = None

        log_events: list[str] = []
        while True:
            if next_token:
                response = client.filter_log_events(
                    logGroupName=f"/aws/lambda/{function_instance}",
                    startTime=time_ms_start,
                    endTime=time_ms_end,
                    nextToken=next_token,
                )
            else:
                try:
                    response = client.filter_log_events(
                        logGroupName=f"/aws/lambda/{function_instance}", startTime=time_ms_start, endTime=time_ms_end
                    )
                except client.exceptions.ResourceNotFoundException:
                    # No logs found
                    return []

            log_events.extend(event["message"] for event in response.get("events", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        return log_events

    # TODO: logging using monitoring v3
    def get_insights_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        time_ms_start = int(start.timestamp() * 1000)
        time_ms_end = int(end.timestamp() * 1000)
        client = self._client("logs")

        next_token = None

        log_events: list[str] = []
        while True:
            if next_token:
                response = client.filter_log_events(
                    logGroupName="/aws/lambda-insights",
                    logStreamNamePrefix=function_instance,
                    startTime=time_ms_start,
                    endTime=time_ms_end,
                    nextToken=next_token,
                )
            else:
                try:
                    response = client.filter_log_events(
                        logGroupName="/aws/lambda-insights",
                        logStreamNamePrefix=function_instance,
                        startTime=time_ms_start,
                        endTime=time_ms_end,
                    )
                except client.exceptions.ResourceNotFoundException:
                    # No logs found
                    return []

            log_events.extend(event["message"] for event in response.get("events", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        return log_events

    # TODO: move to firestore
    def remove_key(self, table_name: str, key: str) -> None:
        client = self._client("dynamodb")

        try:
            client.delete_item(TableName=table_name, Key={"key": {"S": key}})
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                print(f"Table '{table_name}' Remove Key '{key}' Error.")
                print(f"{e.response['Error']['Code']}: {e.response['Error']['Message']}")
            else:
                raise

    # TODO: implement in cloud run
    def remove_function(self, function_name: str) -> None:
        client = self._run_client
        full_path = f"projects/{self._project_id}/locations/{self._region}/services/{function_name}"
        client.delete_service(name=full_path)

    def remove_role(self, role_name: str) -> None:
        client = self._iam_admin_client

        managed_policies = client.list_attached_role_policies(RoleName=role_name)
        for policy in managed_policies.get("AttachedPolicies", []):
            client.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])

        inline_policies = client.list_role_policies(RoleName=role_name)
        for policy_name in inline_policies.get("PolicyNames", []):
            client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)

        client.delete_role(RoleName=role_name)

    def remove_messaging_topic(self, topic_identifier: str) -> None:
        client = self._client("sns")

        # Get all subscriptions for the topic
        response = client.list_subscriptions_by_topic(TopicArn=topic_identifier)
        subscriptions = response.get("Subscriptions", [])

        # Handle pagination if there are more subscriptions
        while "NextToken" in response:
            response = client.list_subscriptions_by_topic(TopicArn=topic_identifier, NextToken=response["NextToken"])
            subscriptions.extend(response.get("Subscriptions", []))

        # Unsubscribe each subscription
        for subscription in subscriptions:
            client.unsubscribe(SubscriptionArn=subscription["SubscriptionArn"])

        # Delete the topic after unsubscribing all its subscriptions
        client.delete_topic(TopicArn=topic_identifier)

    def get_topic_identifier(self, topic_name: str) -> str:
        client = self._client("sns")
        next_token = ""

        while True:
            response = client.list_topics(NextToken=next_token) if next_token else client.list_topics()

            for topic in response["Topics"]:
                if topic_name in topic["TopicArn"]:
                    return topic["TopicArn"]

            next_token = response.get("NextToken")
            if not next_token:
                break

        raise RuntimeError(f"Topic {topic_name} not found")

    def remove_resource(self, key: str) -> None:
        client = self._client("s3")
        try:
            client.delete_object(Bucket=self._deployment_resource_bucket, Key=key)
        except ClientError as e:
            raise RuntimeError(
                f"Could not upload resource {key} to S3, does the bucket {self._deployment_resource_bucket} exist and do you have permission to access it: {str(e)}"  # pylint: disable=line-too-long
            ) from e

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

    # TODO: comment out for now, too complicated
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

        # Stage 2: Build the final image based on Lambda Python 3.12 runtime
        FROM public.ecr.aws/lambda/python:3.12

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
        arn, state = self._create_lambda_function(kwargs)

        if state != "Active":
            self._wait_for_function_to_become_active(function_name)

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
    print(gcp_remote_client.get_service_account(name="809845967121-compute@developer.gserviceaccount.com"))
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

    sa = gcp_remote_client.create_role("caribou-runtime", iam_policy, {})
    sa = gcp_remote_client.update_role("caribou-runtime", iam_policy_2, {})
    print("SA:", sa)