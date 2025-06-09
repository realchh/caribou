import datetime
import os
from collections import defaultdict
from decimal import Decimal
from typing import Any

import boto3
import googlemaps
import requests
from bs4 import BeautifulSoup
from google.cloud import billing_v1
from google.type import money_pb2 as Money

from caribou.common.constants import GLOBAL_TIME_ZONE
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.provider import Provider
from caribou.common.utils import str_to_bool
from caribou.data_collector.components.data_retriever import DataRetriever
from caribou.data_collector.utils.constants import AMAZON_REGION_URL, GCLOUD_REGION_URL


def _unit_price_to_float(price: Money) -> float:
    """Convert google.type.Money to a native float (USD)."""
    return float(Decimal(price.units) + Decimal(price.nanos) / Decimal(1e9))


class ProviderRetriever(DataRetriever):
    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client)
        self._integration_test_on = str_to_bool(os.environ.get("INTEGRATIONTEST_ON", "False"))
        self._google_api_key = os.environ.get("GOOGLE_API_KEY")
        if self._google_api_key is None and not self._integration_test_on:
            raise ValueError("GOOGLE_API_KEY environment variable not set")

        # Should be available in most if not all regions
        # But just to be sure, we use us-east-1 (As we know it's available there)
        self._aws_ec2_client = boto3.client("ec2", region_name="us-east-1")

        self._aws_pricing_client = boto3.client("pricing", region_name="us-east-1")  # Must be in us-east-1
        self._aws_region_name_to_code: dict[str, str] = {}
        self._gcp_region_name_to_code: dict[str, str] = {}
        self._gcp_catalog_client: billing_v1.CloudCatalogClient = billing_v1.CloudCatalogClient()
        self._gcp_cloud_run_service_id: str | None = None

    def retrieve_location(self, name: str) -> tuple[float, float]:
        google_maps = googlemaps.Client(key=self._google_api_key)

        if name == "Columbus":
            name = "Columbus, Ohio"  # Somehow Google Maps doesn't know where Columbus, OH is
        if name == "Canada (Central)":
            name = "Varennes, QC"
        geocode_result = google_maps.geocode(name)
        if geocode_result:
            latitude = geocode_result[0]["geometry"]["location"]["lat"]
            longitude = geocode_result[0]["geometry"]["location"]["lng"]
        else:
            raise ValueError(f"Could not find location {name}")
        return (latitude, longitude)

    def retrieve_available_regions(self) -> dict[str, dict[str, Any]]:
        available_regions = {}

        if self._integration_test_on:
            available_regions.update(self.retrieve_integrationtest_regions())
        else:
            for provider in Provider:
                if provider == Provider.AWS:
                    available_regions.update(self.retrieve_aws_regions())
                elif provider == Provider.GCP:
                    available_regions.update(self.retrieve_gcp_regions())
                elif provider in (Provider.TEST_PROVIDER1, Provider.TEST_PROVIDER2):
                    pass
                elif provider == Provider.INTEGRATION_TEST_PROVIDER:
                    pass  # Integration test regions are added separately and only IF the integration test is on
                else:
                    raise NotImplementedError(f"Provider {provider} not implemented")
        self._available_regions = available_regions
        return available_regions

    def _retrieve_enabled_aws_regions(self) -> list[str]:
        response = self._aws_ec2_client.describe_regions(AllRegions=False)

        # Extract region names from the response
        enabled_regions = [region["RegionName"] for region in response["Regions"]]

        return enabled_regions

    def retrieve_aws_regions(self) -> dict[str, dict[str, Any]]:
        # First we get a list of enabled regions for the current account
        all_enabled_regions = self._retrieve_enabled_aws_regions()

        # Then we get the list of all regions from the AWS regions page
        # To get the location of the regions (Based on location name)
        amazon_region_page = requests.get(AMAZON_REGION_URL, timeout=5)

        amazon_region_page_soup = BeautifulSoup(amazon_region_page.content, "html.parser")

        regions = {}

        tables = amazon_region_page_soup.find_all("table")

        if len(tables) == 0:
            raise ValueError("Could not find any tables on the AWS regions page")

        # Process the first table (which is the regions table)
        table = tables[0]
        table_rows = table.find_all("tr")[1:]  # Skip header row

        for table_row in table_rows:
            table_cells = table_row.find_all("td")
            if len(table_cells) < 2:  # We only need first two columns (Code and Name)
                continue

            region_code = table_cells[0].text.strip()
            region_name = table_cells[1].text.strip()

            if region_code not in all_enabled_regions:
                # Skip regions that are not enabled for the current account
                continue

            coordinates = self.retrieve_location(region_name)
            regions[f"{Provider.AWS.value}:{region_code}"] = {
                "name": region_name,
                "provider": Provider.AWS.value,
                "code": region_code,
                "latitude": coordinates[0],
                "longitude": coordinates[1],
            }
            self._aws_region_name_to_code[region_name] = region_code
        return regions

    def retrieve_gcp_regions(self) -> dict[str, dict[str, Any]]:
        # We get the list of all regions from the GCP regions page
        # To get the location of the regions (Based on location name)
        gcp_region_page = requests.get(GCLOUD_REGION_URL, timeout=5)

        gcp_region_page_soup = BeautifulSoup(gcp_region_page.content, "html.parser")

        regions = {}

        tables = gcp_region_page_soup.find_all("table")

        if len(tables) == 0:
            raise ValueError("Could not find any tables on the GCP regions page")

        # Process the first table (which is the regions table)
        table = tables[0]
        table_rows = table.find_all("tr")[1:]  # Skip header row

        for table_row in table_rows:
            table_cells = table_row.find_all("td")
            if len(table_cells) < 2:  # We only need first two columns (Code and Name)
                continue

            zone_code = table_cells[0].text.strip()
            region_code = zone_code.split("-")[0] + "-" + zone_code.split("-")[1]
            region_name = table_cells[1].text.strip()

            coordinates = self.retrieve_location(region_name)
            regions[f"{Provider.GCP.value}:{region_code}"] = {
                "name": region_name,
                "provider": Provider.GCP.value,
                "code": region_code,
                "latitude": coordinates[0],
                "longitude": coordinates[1],
            }
            self._gcp_region_name_to_code[region_name] = region_code
        return regions

    def retrieve_integrationtest_regions(self) -> dict[str, dict[str, Any]]:
        return {
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {
                "name": "Rivendell",
                "provider": Provider.INTEGRATION_TEST_PROVIDER.value,
                "code": "rivendell",
                "latitude": 51.509865,
                "longitude": -0.118092,
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:lothlorien": {
                "name": "Lothlorien",
                "provider": Provider.INTEGRATION_TEST_PROVIDER.value,
                "code": "lothlorien",
                "latitude": 51.752022,
                "longitude": -1.257726,
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:anduin": {
                "name": "Anduin",
                "provider": Provider.INTEGRATION_TEST_PROVIDER.value,
                "code": "anduin",
                "latitude": 48.856613,
                "longitude": 2.352222,
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:fangorn": {
                "name": "Fangorn",
                "provider": Provider.INTEGRATION_TEST_PROVIDER.value,
                "code": "fangorn",
                "latitude": 52.370216,
                "longitude": 4.895168,
            },
        }

    def retrieve_provider_region_data(self) -> dict[str, dict[str, Any]]:
        provider_data = {}

        grouped_by_provider: dict[str, list[str]] = {}
        for region_key, available_region in self._available_regions.items():
            provider = available_region["provider"]
            if provider not in grouped_by_provider:
                grouped_by_provider[provider] = []
            grouped_by_provider[provider].append(region_key)

        for provider, regions in grouped_by_provider.items():
            try:
                if provider == Provider.AWS.value:
                    provider_data.update(self._retrieve_provider_data_aws(regions))
                elif provider == Provider.GCP.value:
                    provider_data.update(self._retrieve_provider_data_gcp(regions))
                elif provider in (Provider.TEST_PROVIDER1.value, Provider.TEST_PROVIDER2.value):
                    pass
                elif provider == Provider.INTEGRATION_TEST_PROVIDER.value:
                    provider_data.update(self._retrieve_provider_data_integrationtest(regions))
                else:
                    raise NotImplementedError(f"Provider {provider} not implemented")
            except Exception as e:  # pylint: disable=broad-except
                print(f"Error while retrieving provider data for {provider}: {e}")

        return provider_data

    def _retrieve_provider_data_aws(self, aws_regions: list[str]) -> dict[str, Any]:
        transmission_cost_dict = self._retrieve_aws_transmission_cost(aws_regions)

        execution_cost_dict = self._retrieve_aws_execution_cost(aws_regions)

        sns_cost_dict = self._retrieve_aws_sns_cost(aws_regions)

        dynamodb_cost_dict = self._retrieve_aws_dynamodb_cost(aws_regions)

        ecr_cost_dict = self._retrieve_aws_ecr_cost(aws_regions)

        # data obtained from https://www.cloudcarbonfootprint.org/docs/methodology/#gcp-1
        return {
            region_key: {
                "execution_cost": execution_cost_dict[region_key],
                "transmission_cost": transmission_cost_dict[region_key],
                "sns_cost": sns_cost_dict[region_key],
                "dynamodb_cost": dynamodb_cost_dict[region_key],
                "ecr_cost": ecr_cost_dict[region_key],
                "pue": 1.135,
                "cfe": 0.0,
                "average_memory_power": 0.000392,
                "max_cpu_power_kWh": 0.0035,
                "min_cpu_power_kWh": 0.00074,
                "available_architectures": self._retrieve_aws_available_architectures(execution_cost_dict[region_key]),
            }
            for region_key in aws_regions
        }

    def _retrieve_provider_data_gcp(self, gcp_regions: list[str]) -> dict[str, Any]:
        transmission_cost_dict = self._retrieve_gcp_transmission_cost(gcp_regions)
        execution_cost_dict = self._retrieve_gcp_execution_cost(gcp_regions)
        pubsub_cost_dict = self._retrieve_gcp_pubsub_cost(gcp_regions)
        dynamodb_cost_dict = self._retrieve_gcp_firestore_cost(gcp_regions)
        ecr_cost_dict = self._retrieve_gcp_artifact_registry_cost(gcp_regions)

        return {
            region_key: {
                "execution_cost": execution_cost_dict[region_key],
                "transmission_cost": transmission_cost_dict[region_key],
                "sns_cost": pubsub_cost_dict[region_key],
                "dynamodb_cost": dynamodb_cost_dict[region_key],
                "ecr_cost": ecr_cost_dict[region_key],
                "pue": 1.1,
                "cfe": 0.0,
                "average_memory_power": 0.000392,
                "max_cpu_power_kWh": 0.00426,
                "min_cpu_power_kWh": 0.00071,
                "available_architectures": self._retrieve_gcp_available_architectures(),
            }
            for region_key in gcp_regions
        }

    def _retrieve_provider_data_integrationtest(self, regions: list[str]) -> dict[str, Any]:
        execution_cost_dict = {
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {
                "invocation_cost": {"arm64": 2.4e-7, "x86_64": 2.3e-7, "free_tier_invocations": 1000000},
                "compute_cost": {"arm64": 1.56138e-5, "x86_64": 1.95172e-5, "free_tier_compute_gb_s": 400000},
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:lothlorien": {
                "invocation_cost": {"arm64": 2.3e-7, "x86_64": 2.2e-7, "free_tier_invocations": 1000000},
                "compute_cost": {"arm64": 1.56118e-5, "x86_64": 1.93172e-5, "free_tier_compute_gb_s": 400000},
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:anduin": {
                "invocation_cost": {"arm64": 2.2e-7, "x86_64": 2.1e-7, "free_tier_invocations": 1000000},
                "compute_cost": {"arm64": 1.56128e-5, "x86_64": 1.91172e-5, "free_tier_compute_gb_s": 400000},
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:fangorn": {
                "invocation_cost": {"arm64": 2.1e-7, "x86_64": 2.0e-7, "free_tier_invocations": 1000000},
                "compute_cost": {"arm64": 1.56148e-5, "x86_64": 1.89172e-5, "free_tier_compute_gb_s": 400000},
                "unit": "USD",
            },
        }
        transmission_cost_dict = {
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {
                "global_data_transfer": 0.09,
                "provider_data_transfer": 0.02,
                "unit": "USD/GB",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:lothlorien": {
                "global_data_transfer": 0.04,
                "provider_data_transfer": 0.09,
                "unit": "USD/GB",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:anduin": {
                "global_data_transfer": 0.11,
                "provider_data_transfer": 0.05,
                "unit": "USD/GB",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:fangorn": {
                "global_data_transfer": 0.07,
                "provider_data_transfer": 0.03,
                "unit": "USD/GB",
            },
        }
        sns_cost_dict = {
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {
                "cost": 0.011,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:lothlorien": {
                "cost": 0.012,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:anduin": {
                "cost": 0.013,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:fangorn": {
                "cost": 0.014,
                "unit": "USD",
            },
        }
        dynamodb_cost_dict = {
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {
                "cost": 0.021,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:lothlorien": {
                "cost": 0.022,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:anduin": {
                "cost": 0.023,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:fangorn": {
                "cost": 0.024,
                "unit": "USD",
            },
        }
        ecr_cost_dict = {
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {
                "cost": 0.031,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:lothlorien": {
                "cost": 0.032,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:anduin": {
                "cost": 0.033,
                "unit": "USD",
            },
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:fangorn": {
                "cost": 0.034,
                "unit": "USD",
            },
        }
        return {
            region: {
                "execution_cost": execution_cost_dict[region],
                "transmission_cost": transmission_cost_dict[region],
                "sns_cost": sns_cost_dict[region],
                "dynamodb_cost": dynamodb_cost_dict[region],
                "ecr_cost": ecr_cost_dict[region],
                "pue": 1.11,
                "cfe": 0.0,
                "average_memory_power": 0.0003725,
                "max_cpu_power_kWh": 0.0035,
                "min_cpu_power_kWh": 0.00074,
                "available_architectures": ["arm64", "x86_64"],
            }
            for region in regions
        }

    def _retrieve_aws_available_architectures(self, execution_cost: dict[str, Any]) -> list[str]:
        available_architectures = []
        if execution_cost["invocation_cost"]["arm64"] > 0:
            available_architectures.append("arm64")
        if execution_cost["invocation_cost"]["x86_64"] > 0:
            available_architectures.append("x86_64")
        return available_architectures

    def _retrieve_gcp_available_architectures(self) -> list[str]:
        available_architectures = []
        available_architectures.append("arm64")
        available_architectures.append("x86_64")
        return available_architectures

    # this part is hardcoded because when using the api it only worked for us-east-1

    def _retrieve_aws_sns_cost(self, available_regions: list[str]) -> dict[str, Any]:
        result_sns_cost_dict = {}

        exact_region_codes = {
            "us-east-1": 0.50,
            "us-west-1": 0.60,
            "us-west-2": 0.50,
            "us-east-2": 0.50,
            "ca-central-1": 0.50,
            "eu-west-1": 0.50,
            "eu-central-1": 0.55,
            "ca-west-1": 0.55,
            "eu-west-2": 0.55,
            "eu-west-3": 0.55,
            "eu-north-1": 0.55,
            "ap-southeast-1": 0.60,
            "ap-southeast-2": 0.60,
            "ap-northeast-1": 0.55,
            "ap-northeast-2": 0.55,
            "ap-south-1": 0.60,
            "sa-east-1": 0.70,
            "af-south-1": 0.75,
            "me-south-1": 0.75,
            "il-central-1": 0.55,
        }

        for region_key in available_regions:
            if ":" not in region_key:
                raise ValueError(f"Invalid region key {region_key}")

            region_code = region_key.split(":")[1]

            # Check if the region code is in the dictionary
            if region_code in exact_region_codes:
                sns_cost = exact_region_codes[region_code]
            elif region_code.startswith("us-"):
                sns_cost = 0.50
            elif region_code.startswith("eu-"):
                sns_cost = 0.55
            elif region_code.startswith("ap-"):
                sns_cost = 0.60
            elif region_code.startswith("ca-"):
                sns_cost = 0.55
            elif region_code.startswith("sa-"):
                sns_cost = 0.70
            elif region_code.startswith("af-"):
                sns_cost = 0.75
            elif region_code.startswith("me-"):
                sns_cost = 0.75
            else:
                raise ValueError(f"Unknown region code {region_code}")

            result_sns_cost_dict[region_key] = {
                "request_cost": sns_cost / 1_000_000,
                "unit": "USD/requests",
            }

        return result_sns_cost_dict

    def _retrieve_gcp_pubsub_cost(self, available_regions: list[str]) -> dict[str, Any]:
        result_sns_cost_dict = {}

        for region_key in available_regions:
            if ":" not in region_key:
                raise ValueError(f"Invalid region key {region_key}")

            # from https://cloud.google.com/pubsub/pricing
            sns_cost = 40

            result_sns_cost_dict[region_key] = {
                "request_cost": sns_cost / 1024 * 1024 * 1024,
                "unit": "USD/requests",
            }

        return result_sns_cost_dict

    def _retrieve_aws_dynamodb_cost(self, available_region: list[str]) -> dict[str, Any]:
        dynamodb_cost_response = self._aws_pricing_client.list_price_lists(
            ServiceCode="AmazonDynamoDB", EffectiveDate=datetime.datetime.now(), CurrencyCode="USD"
        )

        available_region_code_to_key = {region_key.split(":")[1]: region_key for region_key in available_region}

        dynamodb_cost_dict = {}
        for price_list in dynamodb_cost_response["PriceLists"]:
            region_code = price_list["RegionCode"]

            if region_code not in available_region_code_to_key:
                continue

            price_list_arn = price_list["PriceListArn"]
            price_list_file = self._aws_pricing_client.get_price_list_file_url(
                PriceListArn=price_list_arn, FileFormat="JSON"
            )

            response = requests.get(price_list_file["Url"], timeout=5)
            price_list_file_json = response.json()

            read_request_sku, write_request_sku, storage_sku = self.get_dynamodb_on_demand_skus(price_list_file_json)

            read_request_cost = self.get_cost(price_list_file_json, read_request_sku)
            write_request_cost = self.get_cost(price_list_file_json, write_request_sku)
            storage_cost = self.get_cost(price_list_file_json, storage_sku)

            dynamodb_cost_dict[available_region_code_to_key[region_code]] = {
                "read_request_cost": read_request_cost,
                "write_request_cost": write_request_cost,
                "storage_cost": storage_cost,
                "unit": "USD",
            }

        return dynamodb_cost_dict

    def _retrieve_gcp_firestore_cost(self, available_region: list[str]) -> dict[str, Any]:
        client = self._gcp_catalog_client

        firestore_svc = None
        for svc in client.list_services():
            if "firestore" in svc.display_name.lower():
                firestore_svc = svc
                break

        available_region_code = {region_key.split(":")[1]: region_key for region_key in available_region}

        firestore_storage_cost_dict = defaultdict(
            lambda: {
                "read_request_cost": -1.0,  # USD per 100,000 reads
                "write_request_cost": -1.0,  # USD per 100,000 writes
                "storage_cost": -1.0,  # USD per gigabyte per month
                "unit": "USD",
            }
        )

        for sku in client.list_skus(parent=firestore_svc.name):
            if any(bad in sku.description for bad in ("Backup", "Data Transfer", "Enterprise", "(with free tier)")):
                continue

            if "FirestoreStorage" in sku.category.resource_group:
                kind = "storage_cost"
            elif "FirestoreEntityPutOps" in sku.category.resource_group:
                kind = "write_request_cost"
            elif "FirestoreReadOps" in sku.category.resource_group:
                kind = "read_request_cost"
            else:
                continue

            region_codes = sku.geo_taxonomy.regions

            available_regions = [r for r in region_codes if r in available_region_code]
            if not available_regions:
                continue

            pricing_expression = sku.pricing_info[0].pricing_expression
            price = _unit_price_to_float(pricing_expression.tiered_rates[0].unit_price)

            for r in available_regions:
                region_code = available_region_code[r]
                if kind in ("write_request_cost", "read_request_cost"):
                    price *= 10  # So that price unit is consistent (per 1M request)
                firestore_storage_cost_dict[region_code][kind] = price

        missing = [
            k
            for k, v in firestore_storage_cost_dict.items()
            if -1.0 in (v["read_request_cost"], v["write_request_cost"], v["storage_cost"])
        ]
        if missing:
            raise ValueError(f"Firestore prices missing in regions: {missing}")

        return dict(firestore_storage_cost_dict)

    def get_dynamodb_on_demand_skus(self, price_list_file_json: dict[str, Any]) -> tuple[str, str, str]:
        read_request_sku = ""
        write_request_sku = ""
        storage_sku = ""

        for sku, product_info in price_list_file_json["products"].items():
            product = product_info.get("product", product_info)
            if product.get("productFamily") == "Amazon DynamoDB PayPerRequest Throughput":
                attributes = product.get("attributes", {})
                group = attributes.get("group", "")
                if group == "DDB-ReadUnits":
                    read_request_sku = sku
                elif group == "DDB-WriteUnits":
                    write_request_sku = sku
            elif product.get("productFamily") == "Database Storage":
                storage_sku = sku

        return read_request_sku, write_request_sku, storage_sku

    def get_cost(self, price_list_file_json: dict[str, Any], sku: str) -> float:
        if sku:
            price_item = price_list_file_json["terms"]["OnDemand"][sku][
                list(price_list_file_json["terms"]["OnDemand"][sku].keys())[0]
            ]
            cost = float(
                price_item["priceDimensions"][list(price_item["priceDimensions"].keys())[0]]["pricePerUnit"]["USD"]
            )
            return cost
        return 0.0

    def _retrieve_aws_ecr_cost(self, available_region: list[str]) -> dict[str, Any]:
        ecr_cost_response = self._aws_pricing_client.list_price_lists(
            ServiceCode="AmazonECR", EffectiveDate=datetime.datetime.now(GLOBAL_TIME_ZONE), CurrencyCode="USD"
        )

        available_region_code_to_key = {region_key.split(":")[1]: region_key for region_key in available_region}

        ecr_cost_dict = {}
        for price_list in ecr_cost_response["PriceLists"]:
            region_code = price_list["RegionCode"]

            if region_code not in available_region_code_to_key:
                continue

            price_list_arn = price_list["PriceListArn"]
            price_list_file = self._aws_pricing_client.get_price_list_file_url(
                PriceListArn=price_list_arn, FileFormat="JSON"
            )

            response = requests.get(price_list_file["Url"], timeout=5)
            price_list_file_json = response.json()

            storage_sku = self.get_ecr_skus(price_list_file_json)

            storage_cost = 0.0
            if storage_sku:
                storage_item = price_list_file_json["terms"]["OnDemand"][storage_sku][
                    list(price_list_file_json["terms"]["OnDemand"][storage_sku].keys())[0]
                ]
                storage_cost = float(
                    storage_item["priceDimensions"][list(storage_item["priceDimensions"].keys())[0]]["pricePerUnit"][
                        "USD"
                    ]
                )

            ecr_cost_dict[available_region_code_to_key[region_code]] = {
                "storage_cost": storage_cost,
                "unit": "USD",
            }

        return ecr_cost_dict

    def _retrieve_gcp_artifact_registry_cost(self, available_region: list[str]) -> dict[str, Any]:
        client = self._gcp_catalog_client

        artifact_registry_svc = None
        for svc in client.list_services():
            if "artifact registry" in svc.display_name.lower():
                artifact_registry_svc = svc
                break

        available_region_code = {region_key.split(":")[1]: region_key for region_key in available_region}

        artifact_registry_cost_dict = defaultdict(
            lambda: {
                "storage_cost": 0.0,
                "unit": "USD",
            }
        )
        for sku in client.list_skus(parent=artifact_registry_svc.name):
            if "Storage" not in sku.description:
                continue

            for region_code in sku.service_regions:
                if region_code == "global":
                    for region in available_region_code.keys():
                        artifact_registry_cost_dict[available_region_code[region]] = {
                            "storage_cost": 0.0,
                            "unit": "USD",
                        }

                elif region_code not in available_region_code:
                    continue
                else:
                    artifact_registry_cost_dict[available_region_code[region_code]] = {
                        "storage_cost": 0.0,
                        "unit": "USD",
                    }

        return artifact_registry_cost_dict

    def get_ecr_skus(self, price_list_file_json: dict[str, Any]) -> str:
        storage_sku = ""

        for sku, product in price_list_file_json["products"].items():
            attributes = product.get("attributes", {})
            if attributes.get("servicecode") == "AmazonECR":
                if "Storage" in attributes.get("usagetype", ""):
                    storage_sku = sku
                    break

        return storage_sku

    def _retrieve_aws_transmission_cost(self, available_region: list[str]) -> dict[str, Any]:
        result_transmission_cost_dict = {}

        exact_region_codes = {
            "ap-east-1": (0.12, 0.09),
            "ap-south-2": (0.1093, 0.086),
            "ap-southeast-3": (0.132, 0.10),
            "ap-southeast-4": (0.114, 0.10),
            "ap-south-1": (0.1093, 0.086),
            "ap-northeast-3": (0.114, 0.09),
            "ap-northeast-2": (0.126, 0.08),
            "ap-southeast-1": (0.12, 0.09),
            "ap-southeast-2": (0.114, 0.098),
            "ap-northeast-1": (0.114, 0.09),
            "me-south-1": (0.117, 0.1105),
            "me-central-1": (0.11, 0.085),
            "sa-east-1": (0.15, 0.138),
        }

        for region_key in available_region:
            if ":" not in region_key:
                raise ValueError(f"Invalid region key {region_key}")

            region_code = region_key.split(":")[1]

            # Check if the region code is in the dictionary
            if region_code in exact_region_codes:
                global_data_transfer, provider_data_transfer = exact_region_codes[region_code]
            elif region_code.startswith("us-"):
                global_data_transfer = 0.09
                provider_data_transfer = 0.02
            elif region_code.startswith("af-"):
                global_data_transfer = 0.154
                provider_data_transfer = 0.147
            elif region_code.startswith("ca-"):
                global_data_transfer = 0.09
                provider_data_transfer = 0.02
            elif region_code.startswith("eu-"):
                global_data_transfer = 0.09
                provider_data_transfer = 0.02
            elif region_code.startswith("il-"):
                global_data_transfer = 0.11
                provider_data_transfer = 0.08
            else:
                raise ValueError(f"Unknown region code {region_code}")

            result_transmission_cost_dict[region_key] = {
                "global_data_transfer": global_data_transfer,
                "provider_data_transfer": provider_data_transfer,
                "unit": "USD/GB",
            }

        return result_transmission_cost_dict

    def _retrieve_gcp_transmission_cost(self, available_region: list[str]) -> dict[str, Any]:
        result_transmission_cost_dict = {}

        exact_region_codes = {
            "asia-southeast2": (0.19, 0.1),
            "asia-northeast3": (0.19, 0.08),
            "me-central2": (0.19, 0.08),
            "northamerica-south1": (0.09265, 0.08),
        }

        for region_key in available_region:
            if ":" not in region_key:
                raise ValueError(f"Invalid region key {region_key}")

            region_code = region_key.split(":")[1]
            # Check if the region code is in the dictionary
            if region_code in exact_region_codes:
                global_data_transfer, provider_data_transfer = exact_region_codes[region_code]
            elif region_code.startswith("us-"):
                global_data_transfer = 0.12
                provider_data_transfer = 0.02
            elif region_code.startswith("africa-"):
                global_data_transfer = 0.15
                provider_data_transfer = 0.08
            elif region_code.startswith("asia-"):
                global_data_transfer = 0.12
                provider_data_transfer = 0.08
            elif region_code.startswith("northamerica-"):
                global_data_transfer = 0.12
                provider_data_transfer = 0.02
            elif region_code.startswith("europe-"):
                global_data_transfer = 0.12
                provider_data_transfer = 0.02
            elif region_code.startswith("me-"):
                global_data_transfer = 0.15
                provider_data_transfer = 0.08
            elif region_code.startswith("australia-"):
                global_data_transfer = 0.19
                provider_data_transfer = 0.08
            elif region_code.startswith("southamerica-"):
                global_data_transfer = 0.19
                provider_data_transfer = 0.14
            else:
                raise ValueError(f"Unknown region code {region_code}")

            result_transmission_cost_dict[region_key] = {
                "global_data_transfer": global_data_transfer,
                "provider_data_transfer": provider_data_transfer,
                "unit": "USD/GB",
            }

        return result_transmission_cost_dict

    def _retrieve_aws_execution_cost(self, available_region: list[str]) -> dict[str, Any]:
        execution_cost_response = self._aws_pricing_client.list_price_lists(
            ServiceCode="AWSLambda", EffectiveDate=datetime.datetime.now(GLOBAL_TIME_ZONE), CurrencyCode="USD"
        )

        available_region_code_to_key = {region_key.split(":")[1]: region_key for region_key in available_region}

        current_invocations = 0

        execution_cost_dict = {}
        for price_list in execution_cost_response["PriceLists"]:
            region_code = price_list["RegionCode"]

            if region_code not in available_region_code_to_key:
                continue

            price_list_arn = price_list["PriceListArn"]
            price_list_file = self._aws_pricing_client.get_price_list_file_url(
                PriceListArn=price_list_arn, FileFormat="JSON"
            )

            response = requests.get(price_list_file["Url"], timeout=5)
            price_list_file_json = response.json()

            (
                invocation_call_sku_arm64,
                invocation_duration_sku_arm64,
                invocation_call_sku_x86_64,
                invocation_duration_sku_x86_64,
                invocation_call_free_tier_sku,
                invocation_duration_free_tier_sku,
            ) = self.get_aws_product_skus(price_list_file_json)

            free_invocations_item = price_list_file_json["terms"]["OnDemand"][invocation_call_free_tier_sku][
                list(price_list_file_json["terms"]["OnDemand"][invocation_call_free_tier_sku].keys())[0]
            ]
            free_invocations = int(
                free_invocations_item["priceDimensions"][list(free_invocations_item["priceDimensions"].keys())[0]][
                    "endRange"
                ]
            )  # in requests

            free_duration_item = price_list_file_json["terms"]["OnDemand"][invocation_duration_free_tier_sku][
                list(price_list_file_json["terms"]["OnDemand"][invocation_duration_free_tier_sku].keys())[0]
            ]
            free_compute_gb_s = int(
                free_duration_item["priceDimensions"][list(free_duration_item["priceDimensions"].keys())[0]]["endRange"]
            )  # in seconds

            invocation_cost_arm64 = 0.0
            if len(invocation_call_sku_arm64) > 0:
                invocation_cost_item_arm64 = price_list_file_json["terms"]["OnDemand"][invocation_call_sku_arm64][
                    list(price_list_file_json["terms"]["OnDemand"][invocation_call_sku_arm64].keys())[0]
                ]
                invocation_cost_arm64 = float(
                    invocation_cost_item_arm64["priceDimensions"][
                        list(invocation_cost_item_arm64["priceDimensions"].keys())[0]
                    ]["pricePerUnit"]["USD"]
                )
                compute_cost_item_sku_arm64 = price_list_file_json["terms"]["OnDemand"][invocation_duration_sku_arm64][
                    list(price_list_file_json["terms"]["OnDemand"][invocation_duration_sku_arm64].keys())[0]
                ]

                compute_cost_arm64 = compute_cost_item_sku_arm64["priceDimensions"]
                compute_cost_arm64 = self._get_compute_cost(compute_cost_arm64, current_invocations)

            invocation_cost_item_x86_64 = price_list_file_json["terms"]["OnDemand"][invocation_call_sku_x86_64][
                list(price_list_file_json["terms"]["OnDemand"][invocation_call_sku_x86_64].keys())[0]
            ]
            invocation_cost_x86_64 = float(
                invocation_cost_item_x86_64["priceDimensions"][
                    list(invocation_cost_item_x86_64["priceDimensions"].keys())[0]
                ]["pricePerUnit"]["USD"]
            )

            compute_cost_item_sku_x86_64 = price_list_file_json["terms"]["OnDemand"][invocation_duration_sku_x86_64][
                list(price_list_file_json["terms"]["OnDemand"][invocation_duration_sku_x86_64].keys())[0]
            ]

            compute_cost_x86_64 = compute_cost_item_sku_x86_64["priceDimensions"]

            compute_cost_x86_64 = self._get_compute_cost(compute_cost_x86_64, current_invocations)

            execution_cost_dict[available_region_code_to_key[region_code]] = {
                "invocation_cost": {
                    "arm64": invocation_cost_arm64 if len(invocation_call_sku_arm64) > 0 else 0,
                    "x86_64": invocation_cost_x86_64,
                    "free_tier_invocations": free_invocations,
                },
                "compute_cost": {
                    "arm64": compute_cost_arm64 if len(invocation_call_sku_arm64) > 0 else 0,
                    "x86_64": compute_cost_x86_64,
                    "free_tier_compute_gb_s": free_compute_gb_s,
                },
                "unit": "USD",
            }

        if len(execution_cost_dict) != len(available_region):
            raise ValueError("Not all regions have execution cost data")
        return execution_cost_dict

    def _retrieve_gcp_execution_cost(self, available_region: list[str]) -> dict[str, Any]:
        client = self._gcp_catalog_client

        cloud_run_svc = next(s for s in client.list_services() if s.display_name == "Cloud Run Functions")

        available_region_code = {region_key.split(":")[1]: region_key for region_key in available_region}

        data_by_region = defaultdict(
            lambda: {
                "invocation_cost": {},
                "compute_cost": {},
                "unit": "USD",
            }
        )

        for sku in client.list_skus(parent=cloud_run_svc.name):
            region_codes = [r for r in sku.service_regions if r in available_region_code]
            if not region_codes:
                continue

            pricing_expression = sku.pricing_info[0].pricing_expression
            unit = pricing_expression.usage_unit

            if unit not in {"s", "GiBy.s"}:
                continue

            if "(1st Gen)" in sku.description:
                continue

            if "Min-Instance" in sku.description:
                continue

            price = _unit_price_to_float(pricing_expression.tiered_rates[0].unit_price)

            for reg in region_codes:
                key = available_region_code[reg]
                if unit == "GiBy.s":
                    data_by_region[key]["compute_cost"]["memory_gb_s"] = price
                    data_by_region[key]["compute_cost"]["free_tier_compute_gb_s"] = 0.9 / price

                elif unit == "s":
                    data_by_region[key]["compute_cost"]["cpu_s"] = price
                    data_by_region[key]["compute_cost"]["free_tier_cpu_s"] = 4.32 / price

                data_by_region[key]["invocation_cost"]["price"] = 0.4 / 1000000
                data_by_region[key]["invocation_cost"]["free_tier_invocations"] = 2000000

        for region in available_region_code.values():
            if region not in data_by_region.keys():
                data_by_region[region] = self._gcp_execution_fallback(region)

        return dict(data_by_region)

    def _gcp_execution_fallback(self, region_key: str) -> dict[str, Any]:
        region_code = region_key.split(":")[1]
        tier1_pricing = {"cpu": 0.000024, "mem": 0.0000025}
        tier2_pricing = {"cpu": 0.0000336, "mem": 0.0000035}

        tier1_regions = {
            "asia-east1",
            "asia-northeast1",
            "asia-northeast2",
            "asia-south1",
            "europe-north1",
            "europe-north2",
            "europe-southwest1",
            "europe-west1",
            "europe-west4",
            "europe-west8",
            "europe-west9",
            "me-west1",
            "northamerica-south1",
            "us-central1",
            "us-east1",
            "us-east4",
            "us-east5",
            "us-south1",
            "us-west1",
        }

        tier2_regions = {
            "africa-south1",
            "asia-east2",
            "asia-northeast3",
            "asia-south2",
            "asia-southeast1",
            "asia-southeast2",
            "australia-southeast1",
            "australia-southeast2",
            "europe-central2",
            "europe-west2",
            "europe-west3",
            "europe-west6",
            "europe-west10",
            "europe-west12",
            "me-central1",
            "me-central2",
            "northamerica-northeast1",
            "northamerica-northeast2",
            "southamerica-east1",
            "southamerica-west1",
            "us-west2",
            "us-west3",
            "us-west4",
        }

        if region_code in tier1_regions:
            return {
                "compute_cost": {
                    "cpu_s": tier1_pricing.get("cpu"),
                    "memory_gb_s": tier1_pricing.get("mem"),
                    "free_tier_compute_gb_s": 0.9 / tier1_pricing.get("mem"),
                    "free_tier_cpu_s": 4.32 / tier1_pricing.get("cpu"),
                },
                "invocation_cost": {
                    "price": 0.4 / 1000000,
                    "free_tier_invocations": 2000000,
                },
                "unit": "USD",
            }
        if region_code in tier2_regions:
            return {
                "compute_cost": {
                    "cpu_s": tier2_pricing.get("cpu"),
                    "memory_gb_s": tier2_pricing.get("mem"),
                    "free_tier_compute_gb_s": 0.9 / tier2_pricing.get("mem"),
                    "free_tier_cpu_s": 4.32 / tier2_pricing.get("cpu"),
                },
                "invocation_cost": {
                    "price": 0.4 / 1000000,
                    "free_tier_invocations": 2000000,
                },
                "unit": "USD",
            }

        raise ValueError(f"Region not found: {region_code}")

    def _get_compute_cost(self, compute_cost: dict, current_invocations: int) -> float:
        for value in compute_cost.values():
            if int(value["beginRange"]) <= current_invocations and (
                value["endRange"] == "Inf" or current_invocations <= int(value["endRange"])
            ):
                return float(value["pricePerUnit"]["USD"])
        raise ValueError(f"Could not find compute cost for {current_invocations} invocations")

    def get_aws_product_skus(self, price_list_file_json: dict) -> tuple[str, str, str, str, str, str]:
        """
        Returns the product UIDs for the invocation and duration of a Lambda function

        architecture: "x86_64" or "arm64"
        price_list: price list from the AWS Pricing API
        """
        invocation_call_sku_arm64 = ""
        invocation_duration_sku_arm64 = ""
        invocation_call_sku_x86_64 = ""
        invocation_duration_sku_x86_64 = ""
        invocation_call_free_tier_sku = ""
        invocation_duration_free_tier_sku = ""

        for product in price_list_file_json["products"].values():
            if (
                product["attributes"]["group"] == "AWS-Lambda-Requests-ARM"
                and product["attributes"]["location"] != "Any"
            ):
                invocation_call_sku_arm64 = product["sku"]
            if (
                product["attributes"]["group"] == "AWS-Lambda-Duration-ARM"
                and product["attributes"]["location"] != "Any"
            ):
                invocation_duration_sku_arm64 = product["sku"]
            if product["attributes"]["group"] == "AWS-Lambda-Requests" and product["attributes"]["location"] != "Any":
                invocation_call_sku_x86_64 = product["sku"]
            if product["attributes"]["group"] == "AWS-Lambda-Duration" and product["attributes"]["location"] != "Any":
                invocation_duration_sku_x86_64 = product["sku"]
            if product["attributes"]["group"] == "AWS-Lambda-Requests" and product["attributes"]["location"] == "Any":
                invocation_call_free_tier_sku = product["sku"]
            if product["attributes"]["group"] == "AWS-Lambda-Duration" and product["attributes"]["location"] == "Any":
                invocation_duration_free_tier_sku = product["sku"]

        return (
            invocation_call_sku_arm64,
            invocation_duration_sku_arm64,
            invocation_call_sku_x86_64,
            invocation_duration_sku_x86_64,
            invocation_call_free_tier_sku,
            invocation_duration_free_tier_sku,
        )
