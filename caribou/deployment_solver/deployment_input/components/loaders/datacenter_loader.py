from typing import Any

from caribou.common.constants import (
    PROVIDER_REGION_TABLE,
    PROVIDER_TABLE,
    SOLVER_INPUT_AVERAGE_MEMORY_POWER_DEFAULT,
    SOLVER_INPUT_CFE_DEFAULT,
    SOLVER_INPUT_COMPUTE_COST_DEFAULT,
    SOLVER_INPUT_DYNAMODB_READ_COST_DEFAULT,
    SOLVER_INPUT_DYNAMODB_WRITE_COST_DEFAULT,
    SOLVER_INPUT_ECR_MONTHLY_STORAGE_COST_DEFAULT,
    SOLVER_INPUT_GCP_ARTIFACT_REGISTRY_MONTHLY_STORAGE_COST_DEFAULT,
    SOLVER_INPUT_GCP_COMPUTE_COST_DEFAULT,
    SOLVER_INPUT_GCP_FIRESTORE_READ_COST_DEFAULT,
    SOLVER_INPUT_GCP_FIRESTORE_WRITE_COST_DEFAULT,
    SOLVER_INPUT_GCP_INVOCATION_COST_DEFAULT,
    SOLVER_INPUT_GCP_MAX_CPU_POWER_DEFAULT,
    SOLVER_INPUT_GCP_MIN_CPU_POWER_DEFAULT,
    SOLVER_INPUT_GCP_PUBSUB_REQUEST_COST_DEFAULT,
    SOLVER_INPUT_GCP_PUE_DEFAULT,
    SOLVER_INPUT_GCP_TRANSMISSION_COST_DEFAULT,
    SOLVER_INPUT_INVOCATION_COST_DEFAULT,
    SOLVER_INPUT_MAX_CPU_POWER_DEFAULT,
    SOLVER_INPUT_MIN_CPU_POWER_DEFAULT,
    SOLVER_INPUT_PUE_DEFAULT,
    SOLVER_INPUT_SNS_REQUEST_COST_DEFAULT,
    SOLVER_INPUT_TRANSMISSION_COST_DEFAULT,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.provider import Provider
from caribou.deployment_solver.deployment_input.components.loader import InputLoader


class DatacenterLoader(InputLoader):
    _datacenter_data: dict[str, Any]
    _provider_data: dict[str, Any]
    _provider_table: str

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, PROVIDER_REGION_TABLE)
        self._provider_table = PROVIDER_TABLE

    def setup(self, available_regions: set[str]) -> None:
        self._datacenter_data = self._retrieve_region_data(available_regions)

        # Get the set of providers from the available regions
        providers = set()
        for region in available_regions:
            provider, _ = region.split(":")
            providers.add(provider)

        self._provider_data = self._retrieve_provider_data(providers)  # ignore as not yet implemented

    def get_average_memory_power(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get(
            "average_memory_power", SOLVER_INPUT_AVERAGE_MEMORY_POWER_DEFAULT
        )

    def get_pue(self, region_name: str) -> float:
        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            default_pue = SOLVER_INPUT_GCP_PUE_DEFAULT
        else:
            default_pue = SOLVER_INPUT_PUE_DEFAULT

        return self._datacenter_data.get(region_name, {}).get("pue", default_pue)

    def get_cfe(self, region_name: str) -> float:
        return self._datacenter_data.get(region_name, {}).get("cfe", SOLVER_INPUT_CFE_DEFAULT)

    def get_max_cpu_power(self, region_name: str) -> float:
        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            default_max_cpu_power = SOLVER_INPUT_GCP_MAX_CPU_POWER_DEFAULT
        else:
            default_max_cpu_power = SOLVER_INPUT_MAX_CPU_POWER_DEFAULT

        return self._datacenter_data.get(region_name, {}).get("max_cpu_power_kWh", default_max_cpu_power)

    def get_min_cpu_power(self, region_name: str) -> float:
        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            default_min_cpu_power = SOLVER_INPUT_GCP_MIN_CPU_POWER_DEFAULT
        else:
            default_min_cpu_power = SOLVER_INPUT_MIN_CPU_POWER_DEFAULT

        return self._datacenter_data.get(region_name, {}).get("min_cpu_power_kWh", default_min_cpu_power)

    def get_sns_request_cost(self, region_name: str) -> float:
        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            default_sns_cost = SOLVER_INPUT_GCP_PUBSUB_REQUEST_COST_DEFAULT
        else:
            default_sns_cost = SOLVER_INPUT_SNS_REQUEST_COST_DEFAULT

        return self._datacenter_data.get(region_name, {}).get("sns_cost", {}).get("sns_cost", default_sns_cost)

    def get_dynamodb_read_write_cost(self, region_name: str) -> tuple[float, float]:
        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            default_read_cost = SOLVER_INPUT_GCP_FIRESTORE_READ_COST_DEFAULT
            default_write_cost = SOLVER_INPUT_GCP_FIRESTORE_WRITE_COST_DEFAULT
        else:
            default_read_cost = SOLVER_INPUT_DYNAMODB_READ_COST_DEFAULT
            default_write_cost = SOLVER_INPUT_DYNAMODB_WRITE_COST_DEFAULT

        dynamodb_costs = self._datacenter_data.get(region_name, {}).get("dynamodb_cost", {})
        return dynamodb_costs.get("read_cost", default_read_cost), dynamodb_costs.get("write_cost", default_write_cost)

    def get_ecr_storage_cost(self, region_name: str) -> float:
        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            default_ecr_cost = SOLVER_INPUT_GCP_ARTIFACT_REGISTRY_MONTHLY_STORAGE_COST_DEFAULT
        else:
            default_ecr_cost = SOLVER_INPUT_ECR_MONTHLY_STORAGE_COST_DEFAULT

        return self._datacenter_data.get(region_name, {}).get("ecr_cost", {}).get("storage_cost", default_ecr_cost)

    def get_compute_cost(self, region_name: str, architecture: str) -> float | dict[str, float]:
        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            return (
                self._datacenter_data.get(region_name, {})
                .get("execution_cost", {})
                .get("compute_cost", SOLVER_INPUT_GCP_COMPUTE_COST_DEFAULT)
            )
        return (
            self._datacenter_data.get(region_name, {})
            .get("execution_cost", {})
            .get("compute_cost", {})
            .get(architecture, SOLVER_INPUT_COMPUTE_COST_DEFAULT)
        )

    def get_invocation_cost(self, region_name: str, architecture: str) -> float:
        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            return (
                self._datacenter_data.get(region_name, {})
                .get("execution_cost", {})
                .get("invocation_cost", {})
                .get("price", SOLVER_INPUT_GCP_INVOCATION_COST_DEFAULT)
            )

        return (
            self._datacenter_data.get(region_name, {})
            .get("execution_cost", {})
            .get("invocation_cost", {})
            .get(architecture, SOLVER_INPUT_INVOCATION_COST_DEFAULT)
        )

    def get_transmission_cost(self, region_name: str, intra_provider_transfer: bool) -> float:
        transfer_type = "provider_data_transfer" if intra_provider_transfer else "global_data_transfer"

        provider = region_name.split(":")[0]
        if provider == Provider.GCP.value:
            default_transmission_cost = SOLVER_INPUT_GCP_TRANSMISSION_COST_DEFAULT
        else:
            default_transmission_cost = SOLVER_INPUT_TRANSMISSION_COST_DEFAULT

        return (
            self._datacenter_data.get(region_name, {})
            .get("transmission_cost", {})
            .get(transfer_type, default_transmission_cost)
        )

    def _retrieve_provider_data(self, available_providers: set[str]) -> dict[str, Any]:
        all_data: dict[str, Any] = {}

        for provider in available_providers:
            all_data[provider] = self._retrieve_data(self._provider_table, provider)

        return all_data

    def to_dict(self) -> dict[str, Any]:
        return self._datacenter_data
