import datetime
import unittest
from unittest.mock import Mock, patch, MagicMock

from google.type.money_pb2 import Money

from caribou.data_collector.components.provider.provider_retriever import ProviderRetriever, _unit_price_to_float
from caribou.common.models.remote_client.remote_client import RemoteClient
import requests
import os

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
from caribou.data_collector.components.provider.provider_retriever import ProviderRetriever, _unit_price_to_float
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.provider import Provider
from google.type import money_pb2 as Money
from google.cloud import billing_v1
import requests
import os


class TestProviderRetriever(unittest.TestCase):
    def setUp(self):
        self.boto3_patcher = patch("boto3.client")
        self.str_to_bool_patcher = patch("caribou.common.utils.str_to_bool")
        self.googlemaps_patcher = patch("googlemaps.Client")
        self.catalog_client_patcher = patch("google.cloud.billing_v1.CloudCatalogClient")
        self.billing_client_patcher = patch("google.cloud.billing_v1.CloudBillingClient")
        self.env_patcher = patch.dict("os.environ", {"GOOGLE_API_KEY": "test_key", "INTEGRATIONTEST_ON": "False"})

        self.mock_boto3 = self.boto3_patcher.start()
        self.mock_str_to_bool = self.str_to_bool_patcher.start()
        self.mock_googlemaps = self.googlemaps_patcher.start()
        self.mock_catalog_client = self.catalog_client_patcher.start()
        self.mock_billing_client = self.billing_client_patcher.start()
        self.env_patcher.start()

        self.addCleanup(self.boto3_patcher.stop)
        self.addCleanup(self.str_to_bool_patcher.stop)
        self.addCleanup(self.googlemaps_patcher.stop)
        self.addCleanup(self.catalog_client_patcher.stop)
        self.addCleanup(self.billing_client_patcher.stop)
        self.addCleanup(self.env_patcher.stop)

        # Configure default mock behaviors
        self.mock_str_to_bool.return_value = False
        self.remote_client = MagicMock(spec=RemoteClient)

        # Now it's safe to instantiate the class under test
        self.provider_retriever = ProviderRetriever(self.remote_client)

    @patch.dict(os.environ, {"AWS_REGION": "us-east-1"})
    def test_retrieve_aws_sns_cost(self):
        available_regions = [
            "aws:us-east-1",
            "aws:eu-west-1",
            "aws:ap-southeast-1",
            "aws:us-east-2",
            "aws:eu-west-3",
            "aws:ap-southeast-2",
            "aws:ap-northeast-1",
        ]
        expected_sns_cost = {
            "aws:us-east-1": {"request_cost": 0.50 / 1_000_000, "unit": "USD/requests"},
            "aws:eu-west-1": {"request_cost": 0.50 / 1_000_000, "unit": "USD/requests"},
            "aws:ap-southeast-1": {"request_cost": 0.60 / 1_000_000, "unit": "USD/requests"},
            "aws:us-east-2": {"request_cost": 0.50 / 1_000_000, "unit": "USD/requests"},
            "aws:eu-west-3": {"request_cost": 0.55 / 1_000_000, "unit": "USD/requests"},
            "aws:ap-southeast-2": {"request_cost": 0.60 / 1_000_000, "unit": "USD/requests"},
            "aws:ap-northeast-1": {"request_cost": 0.55 / 1_000_000, "unit": "USD/requests"},
        }

        actual_sns_cost = self.provider_retriever._retrieve_aws_sns_cost(available_regions)

        self.assertEqual(actual_sns_cost, expected_sns_cost)

    @patch.dict(os.environ, {"AWS_REGION": "us-east-1"})
    def test_retrieve_aws_sns_cost_with_invalid_region(self):
        with self.assertRaises(ValueError) as context:
            self.provider_retriever._retrieve_aws_sns_cost(["aws:invalid-region"])

        self.assertTrue("Unknown region code invalid-region" in str(context.exception))

    @patch("requests.get")
    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "mocked_api_key_value", "AWS_REGION": "us-east-1"})
    def test_retrieve_aws_dynamodb_cost(self, mock_boto3_client, mock_requests_get):
        # Mock AWS pricing client
        mock_aws_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_aws_pricing_client

        # Mock the list_price_lists response
        mock_aws_pricing_client.list_price_lists.return_value = {
            "PriceLists": [
                {
                    "RegionCode": "us-east-1",
                    "PriceListArn": "arn:aws:pricing::price-list/AmazonDynamoDB/us-east-1",
                },
                {
                    "RegionCode": "us-west-2",
                    "PriceListArn": "arn:aws:pricing::price-list/AmazonDynamoDB/us-west-2",
                },
                {
                    "RegionCode": "eu-central-1",
                    "PriceListArn": "arn:aws:pricing::price-list/AmazonDynamoDB/eu-central-1",
                },
                {
                    "RegionCode": "ap-southeast-1",
                    "PriceListArn": "arn:aws:pricing::price-list/AmazonDynamoDB/ap-southeast-1",
                },
                {"RegionCode": "ap-east-1", "PriceListArn": "arn:aws:pricing::price-list/AmazonDynamoDB/ap-east-1"},
                {
                    "RegionCode": "ap-southeast-4",
                    "PriceListArn": "arn:aws:pricing::price-list/AmazonDynamoDB/ap-southeast-4",
                },
            ]
        }

        # Mock the get_price_list_file_url response
        mock_aws_pricing_client.get_price_list_file_url.return_value = {
            "Url": "http://example.com/dynamodb_price_list.json"
        }

        # Mock the requests.get response
        mock_price_list_response = {
            "products": {
                "read_sku": {
                    "productFamily": "Amazon DynamoDB PayPerRequest Throughput",
                    "attributes": {"group": "DDB-ReadUnits"},
                },
                "write_sku": {
                    "productFamily": "Amazon DynamoDB PayPerRequest Throughput",
                    "attributes": {"group": "DDB-WriteUnits"},
                },
                "storage_sku": {"productFamily": "Database Storage"},
            },
            "terms": {
                "OnDemand": {
                    "read_sku": {
                        "read_sku.terms": {
                            "priceDimensions": {"read_sku.priceDimension": {"pricePerUnit": {"USD": "0.00000025"}}}
                        }
                    },
                    "write_sku": {
                        "write_sku.terms": {
                            "priceDimensions": {"write_sku.priceDimension": {"pricePerUnit": {"USD": "0.00000125"}}}
                        }
                    },
                    "storage_sku": {
                        "storage_sku.terms": {
                            "priceDimensions": {"storage_sku.priceDimension": {"pricePerUnit": {"USD": "0.1"}}}
                        }
                    },
                }
            },
        }

        mock_requests_get.return_value.json.return_value = mock_price_list_response

        # Initialize the AWSPricingRetriever with the mocked client
        provider_retriever = ProviderRetriever(client=mock_aws_pricing_client)

        # Define the input and expected output
        available_regions = [
            "aws:us-east-1",
        ]
        expected_dynamodb_cost = {
            "aws:us-east-1": {
                "read_request_cost": 2.5e-07,
                "write_request_cost": 1.25e-06,
                "storage_cost": 0.1,
                "unit": "USD",
            }
        }

        # Call the method and check the result
        actual_dynamodb_cost = provider_retriever._retrieve_aws_dynamodb_cost(available_regions)
        self.assertEqual(actual_dynamodb_cost, expected_dynamodb_cost)

    @patch.dict(os.environ, {"AWS_REGION": "us-east-1"})
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "mocked_api_key_value", "AWS_REGION": "us-east-1"})
    def test_retrieve_aws_dynamodb_cost_with_no_regions(self):
        expected_dynamodb_cost = {}  # Assuming no regions results in no costs

        actual_dynamodb_cost = self.provider_retriever._retrieve_aws_dynamodb_cost([])
        self.assertEqual(actual_dynamodb_cost, expected_dynamodb_cost)

    @patch("requests.get")
    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "mocked_api_key_value", "AWS_REGION": "us-east-1"})
    def test_retrieve_aws_dynamodb_cost_invalid_region(self, mock_boto3_client, mock_requests_get):
        # Setup mocks
        mock_aws_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_aws_pricing_client
        mock_requests_get.return_value.json.return_value = {}  # Assume empty response for invalid region

        provider_retriever = ProviderRetriever(client=mock_aws_pricing_client)
        invalid_regions = ["aws:invalid-region"]
        expected_result = {}  # Expecting empty result for invalid region

        # Act
        actual_result = provider_retriever._retrieve_aws_dynamodb_cost(invalid_regions)

        # Assert
        self.assertEqual(actual_result, expected_result)

    @patch("requests.get")
    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "mocked_api_key_value", "AWS_REGION": "us-east-1"})
    def test_retrieve_aws_dynamodb_cost_empty_price_list(self, mock_boto3_client, mock_requests_get):
        # Setup mocks for empty price list response
        mock_aws_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_aws_pricing_client
        mock_aws_pricing_client.list_price_lists.return_value = {"PriceLists": []}  # Empty price list

        provider_retriever = ProviderRetriever(client=mock_aws_pricing_client)
        regions = ["aws:us-east-1"]
        expected_result = {}  # Expecting empty result for empty price list

        # Act
        actual_result = provider_retriever._retrieve_aws_dynamodb_cost(regions)

        # Assert
        self.assertEqual(actual_result, expected_result)

    @patch("googlemaps.Client")
    def test_retrieve_location(self, mock_googlemaps_client):
        mock_googlemaps_client.return_value.geocode.return_value = [
            {"geometry": {"location": {"lat": 40.7128, "lng": 74.0060}}}
        ]
        lat, lng = self.provider_retriever.retrieve_location("New York")
        self.assertEqual((lat, lng), (40.7128, 74.0060))

    @patch("requests.get")
    @patch("googlemaps.Client")
    def test_retrieve_aws_regions(self, mock_googlemaps_client, mock_requests_get):
        mock_html_content = """
        <html>
            <body>
                <div class="table-container">
                    <div class="table-contents disable-scroll">
                        <table id="w136aab7c13b7">
                            <thead>
                                <tr>
                                    <th>Code</th>
                                    <th>Name</th>
                                    <th>AZs</th>
                                    <th>Geography</th>
                                    <th>Opt-in status</th>
                                </tr>
                            </thead>
                            <tr>
                                <td tabindex="-1">us-east-1</td>
                                <td tabindex="-1">US East (N. Virginia)</td>
                                <td tabindex="-1">6</td>
                                <td tabindex="-1">United States of America</td>
                                <td tabindex="-1">Not required</td>
                            </tr>
                            <tr>
                                <td tabindex="-1">us-west-1</td>
                                <td tabindex="-1">US West (N. California)</td>
                                <td tabindex="-1">3</td>
                                <td tabindex="-1">United States of America</td>
                                <td tabindex="-1">Not required</td>
                            </tr>
                            <tr>
                                <td tabindex="-1">eu-west-1</td>
                                <td tabindex="-1">EU (Ireland)</td>
                                <td tabindex="-1">3</td>
                                <td tabindex="-1">Ireland</td>
                                <td tabindex="-1">Not required</td>
                            </tr>
                        </table>
                    </div>
                </div>
            </body>
        </html>
        """
        mock_response = MagicMock()
        mock_response.content = mock_html_content
        mock_requests_get.return_value = mock_response

        mock_googlemaps_client.return_value.geocode.return_value = [
            {"geometry": {"location": {"lat": 37.7749, "lng": -122.4194}}}
        ]

        with patch("os.environ.get") as mock_os_environ_get, patch("boto3.client") as mock_boto3, patch(
            "caribou.common.utils.str_to_bool"
        ) as mock_str_to_bool, patch.object(ProviderRetriever, "__init__", lambda x, y: None) as mock_init:
            mock_boto3.return_value = MagicMock()
            mock_os_environ_get.return_value = "test_key"
            mock_str_to_bool.return_value = False
            provider_retriever = ProviderRetriever(None)  # Assuming None can be passed as a dummy RemoteClient
            provider_retriever._aws_region_name_to_code = {}
            provider_retriever._google_api_key = "mock_api_key"

            provider_retriever._retrieve_enabled_aws_regions = Mock(return_value=["us-east-1", "us-west-1"])

        regions = provider_retriever.retrieve_aws_regions()

        # Verify the regions dictionary structure
        self.assertIn("aws:us-east-1", regions)
        self.assertIn("aws:us-west-1", regions)
        self.assertNotIn("aws:eu-west-1", regions)  # Should not be included as it's not in enabled regions

        # Verify the region data structure
        us_east_region = regions["aws:us-east-1"]
        self.assertEqual(us_east_region["name"], "US East (N. Virginia)")
        self.assertEqual(us_east_region["provider"], "aws")
        self.assertEqual(us_east_region["code"], "us-east-1")
        self.assertEqual(us_east_region["latitude"], 37.7749)
        self.assertEqual(us_east_region["longitude"], -122.4194)

        # Verify the region name to code mapping
        self.assertEqual(provider_retriever._aws_region_name_to_code["US East (N. Virginia)"], "us-east-1")
        self.assertEqual(provider_retriever._aws_region_name_to_code["US West (N. California)"], "us-west-1")

    @patch("requests.get")
    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "mocked_api_key_value", "AWS_REGION": "us-east-1"})
    def test_retrieve_aws_ecr_cost(self, mock_boto3_client, mock_requests_get):
        # Mock AWS pricing client
        mock_aws_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_aws_pricing_client

        # Mock the list_price_lists response
        mock_aws_pricing_client.list_price_lists.return_value = {
            "PriceLists": [
                {
                    "RegionCode": "us-east-1",
                    "PriceListArn": "arn:aws:pricing::price-list/1",
                }
            ]
        }

        # Mock the get_price_list_file_url response
        mock_aws_pricing_client.get_price_list_file_url.return_value = {"Url": "http://example.com/price_list.json"}

        # Mock the requests.get response
        mock_price_list_response = {
            "products": {
                "sku-1": {"attributes": {"servicecode": "AmazonECR", "usagetype": "EC2-Other-Storage"}},
            },
            "terms": {
                "OnDemand": {
                    "sku-1": {"sku-1-term": {"priceDimensions": {"sku-1-term-dim": {"pricePerUnit": {"USD": "0.10"}}}}},
                }
            },
        }

        mock_requests_get.return_value.json.return_value = mock_price_list_response

        # Initialize the ProviderRetriever with the mocked client
        provider_retriever = ProviderRetriever(client=mock_aws_pricing_client)

        # Define the input and expected output
        available_regions = ["aws:us-east-1"]
        expected_ecr_cost = {
            "aws:us-east-1": {
                "storage_cost": 0.10,
                "unit": "USD",
            }
        }

        # Call the method and check the result
        actual_ecr_cost = provider_retriever._retrieve_aws_ecr_cost(available_regions)
        self.assertEqual(actual_ecr_cost, expected_ecr_cost)

    @patch("requests.get")
    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "mocked_api_key_value", "AWS_REGION": "us-east-1"})
    def test_retrieve_aws_ecr_cost_empty_response(self, mock_boto3_client, mock_requests_get):
        mock_aws_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_aws_pricing_client

        mock_aws_pricing_client.list_price_lists.return_value = {"PriceLists": []}  # Empty response
        mock_requests_get.return_value.json.return_value = {}

        provider_retriever = ProviderRetriever(client=mock_aws_pricing_client)
        available_regions = ["aws:us-east-1"]
        expected_ecr_cost = {}  # Expecting an empty result due to empty response

        actual_ecr_cost = provider_retriever._retrieve_aws_ecr_cost(available_regions)
        self.assertEqual(actual_ecr_cost, expected_ecr_cost)

    @patch("requests.get")
    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "mocked_api_key_value", "AWS_REGION": "us-east-1"})
    def test_retrieve_aws_ecr_cost_invalid_region(self, mock_boto3_client, mock_requests_get):
        mock_aws_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_aws_pricing_client

        # Mock the list_price_lists response for an invalid region
        mock_aws_pricing_client.list_price_lists.return_value = {"PriceLists": []}  # No price lists for invalid regions

        provider_retriever = ProviderRetriever(client=mock_aws_pricing_client)
        available_regions = ["aws:invalid-region"]
        expected_ecr_cost = {}  # Expecting an empty result for invalid region

        actual_ecr_cost = provider_retriever._retrieve_aws_ecr_cost(available_regions)
        self.assertEqual(actual_ecr_cost, expected_ecr_cost)

    @patch("requests.get")
    @patch("bs4.BeautifulSoup")
    def test_retrieve_aws_regions_invalid_html(self, mock_beautiful_soup, mock_requests_get):
        mock_response = MagicMock()
        mock_response.content = "<html></html>"  # Simplified HTML content
        mock_requests_get.return_value = mock_response

        mock_soup_instance = MagicMock()
        mock_beautiful_soup.return_value = mock_soup_instance
        mock_soup_instance.find_all.return_value = []  # No tables found scenario

        with self.assertRaises(ValueError) as context:
            self.provider_retriever.retrieve_aws_regions()
        self.assertTrue("Could not find any tables on the AWS regions page" in str(context.exception))

    @patch("caribou.data_collector.components.provider.provider_retriever.ProviderRetriever.retrieve_aws_regions")
    @patch("caribou.data_collector.components.provider.provider_retriever.ProviderRetriever.retrieve_gcp_regions")
    def test_retrieve_available_regions(self, mock_retrieve_aws_regions, mock_retrieve_gcp_regions):
        mock_retrieve_aws_regions.return_value = {"aws:dummy_region": {"code": "dummy_region"}}
        mock_retrieve_gcp_regions.return_value = {"gcp:dummy_region2": {"code": "dummy_region2"}}

        result = self.provider_retriever.retrieve_available_regions()
        self.assertIn("aws:dummy_region", result)
        self.assertIn("gcp:dummy_region2", result)
        self.assertEqual(result["aws:dummy_region"]["code"], "dummy_region")
        self.assertEqual(result["gcp:dummy_region2"]["code"], "dummy_region2")

    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    @patch("requests.get")
    def test_retrieve_aws_execution_cost(self, mock_requests_get, mock_boto3_client):
        mock_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_pricing_client
        mock_pricing_client.list_price_lists.return_value = {"PriceLists": []}

        self.provider_retriever._aws_pricing_client = mock_pricing_client

        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_requests_get.return_value = mock_response

        with self.assertRaises(ValueError) as context:
            self.provider_retriever._retrieve_aws_execution_cost(["aws:dummy_region"])
        self.assertTrue("Not all regions have execution cost data" in str(context.exception))

    @patch.dict(os.environ, {"GOOGLE_API_KEY": "fake_key"})
    @patch("caribou.data_collector.components.provider.provider_retriever.googlemaps.Client")
    def test_retrieve_location_google_maps_failure(self, mock_googlemaps_client):
        mock_googlemaps_client.return_value.geocode.return_value = []

        with self.assertRaises(ValueError) as context:
            self.provider_retriever.retrieve_location("Atlantis")
        self.assertTrue("Could not find location Atlantis" in str(context.exception))

    @patch("caribou.data_collector.components.provider.provider_retriever.requests.get")
    def test_retrieve_aws_regions_http_error(self, mock_requests_get):
        mock_requests_get.side_effect = requests.exceptions.HTTPError("HTTP Error occurred")

        with self.assertRaises(requests.exceptions.HTTPError):
            self.provider_retriever.retrieve_aws_regions()

    @patch("caribou.data_collector.components.provider.provider_retriever.str_to_bool")
    @patch("caribou.data_collector.components.provider.provider_retriever.os.environ.get")
    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    def test_aws_pricing_client_initialization_failure(self, mock_boto3_client, mock_os_environ_get, mock_str_to_bool):
        mock_os_environ_get.return_value = None  # No AWS credentials
        mock_str_to_bool.return_value = True  # Is integration test
        mock_boto3_client.side_effect = Exception("AWS client initialization failed")

        with self.assertRaises(Exception) as context:
            self.provider_retriever.__init__(self.remote_client)
        self.assertTrue("AWS client initialization failed" in str(context.exception))

    @patch(
        "caribou.data_collector.components.provider.provider_retriever.ProviderRetriever._retrieve_aws_transmission_cost"
    )
    @patch(
        "caribou.data_collector.components.provider.provider_retriever.ProviderRetriever._retrieve_aws_execution_cost"
    )
    @patch("caribou.data_collector.components.provider.provider_retriever.ProviderRetriever._retrieve_aws_sns_cost")
    @patch(
        "caribou.data_collector.components.provider.provider_retriever.ProviderRetriever._retrieve_aws_dynamodb_cost"
    )
    @patch("caribou.data_collector.components.provider.provider_retriever.ProviderRetriever._retrieve_aws_ecr_cost")
    @patch(
        "caribou.data_collector.components.provider.provider_retriever.ProviderRetriever._retrieve_aws_available_architectures"
    )
    def test_retrieve_provider_data_aws(
        self,
        mock_retrieve_aws_available_architectures,
        mock_retrieve_aws_ecr_cost,
        mock_retrieve_aws_dynamodb_cost,
        mock_retrieve_aws_sns_cost,
        mock_retrieve_aws_execution_cost,
        mock_retrieve_aws_transmission_cost,
    ):
        aws_regions = ["aws:us-east-1", "aws:eu-west-1"]

        mock_retrieve_aws_transmission_cost.return_value = {
            "aws:us-east-1": {"global_data_transfer": 0.002},
            "aws:eu-west-1": {"global_data_transfer": 0.003},
        }
        mock_retrieve_aws_execution_cost.return_value = {
            "aws:us-east-1": {
                "compute_cost": {"arm64": 0.0001, "x86": 0.00006},
                "invocation_cost": {"arm64": 0.002, "x86_64": 0.003},
            },
            "aws:eu-west-1": {
                "compute_cost": {"arm64": 0.0002, "x86": 0.00008},
                "invocation_cost": {"arm64": 0.0025, "x86_64": 0.0035},
            },
        }
        mock_retrieve_aws_sns_cost.return_value = {
            "aws:us-east-1": {"request_cost": 0.5, "unit": "USD/requests"},
            "aws:eu-west-1": {"request_cost": 0.55, "unit": "USD/requests"},
        }
        mock_retrieve_aws_dynamodb_cost.return_value = {
            "aws:us-east-1": {
                "read_request_cost": 0.01,
                "write_request_cost": 0.05,
                "storage_cost": 0.1,
                "unit": "USD",
            },
            "aws:eu-west-1": {
                "read_request_cost": 0.015,
                "write_request_cost": 0.055,
                "storage_cost": 0.15,
                "unit": "USD",
            },
        }
        mock_retrieve_aws_ecr_cost.return_value = {
            "aws:us-east-1": {"ecr_cost": 0.1},
            "aws:eu-west-1": {"ecr_cost": 0.15},
        }
        mock_retrieve_aws_available_architectures.return_value = ["arm64", "x86"]

        self.provider_retriever._available_regions = {
            "aws:us-east-1": {"provider": "aws"},
            "aws:eu-west-1": {"provider": "aws"},
        }

        expected_result = {
            "aws:us-east-1": {
                "execution_cost": {
                    "compute_cost": {"arm64": 0.0001, "x86": 0.00006},
                    "invocation_cost": {"arm64": 0.002, "x86_64": 0.003},
                },
                "transmission_cost": {"global_data_transfer": 0.002},
                "sns_cost": {"request_cost": 0.5, "unit": "USD/requests"},
                "dynamodb_cost": {
                    "read_request_cost": 0.01,
                    "write_request_cost": 0.05,
                    "storage_cost": 0.1,
                    "unit": "USD",
                },
                "ecr_cost": {"ecr_cost": 0.1},
                "pue": 1.15,
                "cfe": 0.0,
                "average_memory_power": 0.000392,
                "max_cpu_power_kWh": 0.0035,
                "min_cpu_power_kWh": 0.00074,
                "available_architectures": ["arm64", "x86"],
            },
            "aws:eu-west-1": {
                "execution_cost": {
                    "compute_cost": {"arm64": 0.0002, "x86": 0.00008},
                    "invocation_cost": {"arm64": 0.0025, "x86_64": 0.0035},
                },
                "transmission_cost": {"global_data_transfer": 0.003},
                "sns_cost": {"request_cost": 0.55, "unit": "USD/requests"},
                "dynamodb_cost": {
                    "read_request_cost": 0.015,
                    "write_request_cost": 0.055,
                    "storage_cost": 0.15,
                    "unit": "USD",
                },
                "ecr_cost": {"ecr_cost": 0.15},
                "pue": 1.11,
                "cfe": 0.0,
                "average_memory_power": 0.000392,
                "max_cpu_power_kWh": 0.0035,
                "min_cpu_power_kWh": 0.00074,
                "available_architectures": ["arm64", "x86"],
            },
        }

        result = self.provider_retriever._retrieve_provider_data_aws(aws_regions)

        self.assertEqual(result, expected_result)

    def test_retrieve_aws_transmission_cost(self):
        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:us-west-1"])
        self.assertEqual(
            result, {"aws:us-west-1": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"}}
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:af-south-1"])
        self.assertEqual(
            result,
            {"aws:af-south-1": {"global_data_transfer": 0.154, "provider_data_transfer": 0.147, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-east-1"])
        self.assertEqual(
            result, {"aws:ap-east-1": {"global_data_transfer": 0.12, "provider_data_transfer": 0.09, "unit": "USD/GB"}}
        )

        with self.assertRaises(ValueError):
            self.provider_retriever._retrieve_aws_transmission_cost(["aws:unknown-region"])

        with self.assertRaises(ValueError, msg="Invalid region key us-west-1"):
            self.provider_retriever._retrieve_aws_transmission_cost(["us-west-1"])

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-south-2"])
        self.assertEqual(
            result,
            {"aws:ap-south-2": {"global_data_transfer": 0.1093, "provider_data_transfer": 0.086, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-southeast-3"])
        self.assertEqual(
            result,
            {"aws:ap-southeast-3": {"global_data_transfer": 0.132, "provider_data_transfer": 0.10, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-southeast-4"])
        self.assertEqual(
            result,
            {"aws:ap-southeast-4": {"global_data_transfer": 0.114, "provider_data_transfer": 0.10, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-south-1"])
        self.assertEqual(
            result,
            {"aws:ap-south-1": {"global_data_transfer": 0.1093, "provider_data_transfer": 0.086, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-northeast-3"])
        self.assertEqual(
            result,
            {"aws:ap-northeast-3": {"global_data_transfer": 0.114, "provider_data_transfer": 0.09, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-northeast-2"])
        self.assertEqual(
            result,
            {"aws:ap-northeast-2": {"global_data_transfer": 0.126, "provider_data_transfer": 0.08, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-southeast-1"])
        self.assertEqual(
            result,
            {"aws:ap-southeast-1": {"global_data_transfer": 0.12, "provider_data_transfer": 0.09, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-southeast-2"])
        self.assertEqual(
            result,
            {"aws:ap-southeast-2": {"global_data_transfer": 0.114, "provider_data_transfer": 0.098, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ap-northeast-1"])
        self.assertEqual(
            result,
            {"aws:ap-northeast-1": {"global_data_transfer": 0.114, "provider_data_transfer": 0.09, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:ca-central-1"])
        self.assertEqual(
            result,
            {"aws:ca-central-1": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:eu-west-1"])
        self.assertEqual(
            result, {"aws:eu-west-1": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"}}
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:il-central-1"])
        self.assertEqual(
            result,
            {"aws:il-central-1": {"global_data_transfer": 0.11, "provider_data_transfer": 0.08, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:me-south-1"])
        self.assertEqual(
            result,
            {"aws:me-south-1": {"global_data_transfer": 0.117, "provider_data_transfer": 0.1105, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:me-central-1"])
        self.assertEqual(
            result,
            {"aws:me-central-1": {"global_data_transfer": 0.11, "provider_data_transfer": 0.085, "unit": "USD/GB"}},
        )

        result = self.provider_retriever._retrieve_aws_transmission_cost(["aws:sa-east-1"])
        self.assertEqual(
            result, {"aws:sa-east-1": {"global_data_transfer": 0.15, "provider_data_transfer": 0.138, "unit": "USD/GB"}}
        )

    @patch("caribou.data_collector.components.provider.provider_retriever.requests.get")
    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    def test_retrieve_aws_execution_cost_success(self, mock_boto3_client, mock_requests_get):
        mock_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_pricing_client

        mock_pricing_client.list_price_lists.return_value = {
            "PriceLists": [
                {"RegionCode": "dummy_region", "PriceListArn": "arn:aws:pricing:::product/aws-lambda/dummy_region"}
            ]
        }

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "terms": {
                "OnDemand": {
                    "invocation_call_sku_arm64": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {"pricePerUnit": {"USD": "0.0000166667"}, "endRange": "Inf"}
                            }
                        }
                    },
                    "invocation_duration_sku_arm64": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {
                                    "pricePerUnit": {"USD": "0.0000133333"},
                                    "beginRange": "0",
                                    "endRange": "Inf",
                                }
                            }
                        }
                    },
                    "invocation_call_sku_x86_64": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {"pricePerUnit": {"USD": "0.0000166667"}, "endRange": "Inf"}
                            }
                        }
                    },
                    "invocation_duration_sku_x86_64": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {
                                    "pricePerUnit": {"USD": "0.0000133333"},
                                    "beginRange": "0",
                                    "endRange": "Inf",
                                }
                            }
                        }
                    },
                    "invocation_call_sku_arm64_any": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {"pricePerUnit": {"USD": "0.0000166667"}, "endRange": "5000"}
                            }
                        }
                    },
                    "invocation_duration_sku_arm64_any": {
                        "offerTermCode": {
                            "priceDimensions": {
                                "rateCode": {
                                    "pricePerUnit": {"USD": "0.0000133333"},
                                    "beginRange": "0",
                                    "endRange": "6000",
                                }
                            }
                        }
                    },
                }
            },
            "products": {
                "invocation_call_sku_arm64": {
                    "attributes": {"group": "AWS-Lambda-Requests-ARM", "location": "US East (N. Virginia)"},
                    "sku": "invocation_call_sku_arm64",
                },
                "invocation_duration_sku_arm64": {
                    "attributes": {"group": "AWS-Lambda-Duration-ARM", "location": "US East (N. Virginia)"},
                    "sku": "invocation_duration_sku_arm64",
                },
                "invocation_call_sku_x86_64": {
                    "attributes": {"group": "AWS-Lambda-Requests", "location": "US East (N. Virginia)"},
                    "sku": "invocation_call_sku_x86_64",
                },
                "invocation_duration_sku_x86_64": {
                    "attributes": {"group": "AWS-Lambda-Duration", "location": "US East (N. Virginia)"},
                    "sku": "invocation_duration_sku_x86_64",
                },
                "invocation_call_sku_arm64_any": {
                    "attributes": {"group": "AWS-Lambda-Requests", "location": "Any"},
                    "sku": "invocation_call_sku_arm64_any",
                },
                "invocation_duration_sku_arm64_any": {
                    "attributes": {"group": "AWS-Lambda-Duration", "location": "Any"},
                    "sku": "invocation_duration_sku_arm64_any",
                },
            },
        }

        mock_requests_get.return_value = mock_response

        self.provider_retriever._aws_pricing_client = mock_pricing_client

        result = self.provider_retriever._retrieve_aws_execution_cost(["aws:dummy_region:dummy_code"])

        self.assertIn("aws:dummy_region:dummy_code", result)
        self.assertIn("invocation_cost", result["aws:dummy_region:dummy_code"])
        self.assertIn("compute_cost", result["aws:dummy_region:dummy_code"])

    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    def test_retrieve_aws_execution_cost_api_failure(self, mock_boto3_client):
        mock_pricing_client = MagicMock()
        mock_boto3_client.return_value = mock_pricing_client
        mock_pricing_client.list_price_lists.side_effect = Exception("AWS Pricing API error")

        self.provider_retriever._aws_pricing_client = mock_pricing_client

        with self.assertRaises(Exception) as context:
            self.provider_retriever._retrieve_aws_execution_cost(["aws:dummy_region:dummy_code"])
        self.assertTrue("AWS Pricing API error" in str(context.exception))

    def test_get_aws_product_skus_all_present(self):
        price_list_json = {
            "products": {
                "sku1": {
                    "attributes": {"group": "AWS-Lambda-Requests-ARM", "location": "US East (N. Virginia)"},
                    "sku": "sku1",
                },
                "sku2": {
                    "attributes": {"group": "AWS-Lambda-Duration-ARM", "location": "US East (N. Virginia)"},
                    "sku": "sku2",
                },
                "sku3": {
                    "attributes": {"group": "AWS-Lambda-Requests", "location": "US East (N. Virginia)"},
                    "sku": "sku3",
                },
                "sku4": {
                    "attributes": {"group": "AWS-Lambda-Duration", "location": "US East (N. Virginia)"},
                    "sku": "sku4",
                },
                "sku5": {"attributes": {"group": "AWS-Lambda-Requests", "location": "Any"}, "sku": "sku5"},
                "sku6": {"attributes": {"group": "AWS-Lambda-Duration", "location": "Any"}, "sku": "sku6"},
            }
        }

        result = self.provider_retriever.get_aws_product_skus(price_list_json)
        self.assertEqual(result, ("sku1", "sku2", "sku3", "sku4", "sku5", "sku6"))

    def test_get_aws_product_skus_missing_skus(self):
        price_list_json = {
            "products": {
                "sku3": {
                    "attributes": {"group": "AWS-Lambda-Requests", "location": "US East (N. Virginia)"},
                    "sku": "sku3",
                },
                "sku4": {
                    "attributes": {"group": "AWS-Lambda-Duration", "location": "US East (N. Virginia)"},
                    "sku": "sku4",
                },
            }
        }

        result = self.provider_retriever.get_aws_product_skus(price_list_json)
        self.assertEqual(result, ("", "", "sku3", "sku4", "", ""))

    def test_get_aws_product_skus_empty_json(self):
        price_list_json = {"products": {}}

        result = self.provider_retriever.get_aws_product_skus(price_list_json)
        self.assertEqual(result, ("", "", "", "", "", ""))

    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    def test_retrieve_enabled_aws_regions_success(self, mock_boto3_client):
        with patch("os.environ.get") as mock_os_environ_get, patch(
            "caribou.common.utils.str_to_bool"
        ) as mock_str_to_bool, patch.object(ProviderRetriever, "__init__", lambda x, y: None):
            mock_os_environ_get.return_value = "test_key"
            mock_str_to_bool.return_value = False
            provider_retriever = ProviderRetriever(None)

        provider_retriever._google_api_key = "mock_api_key"

        mock_ec2_client = MagicMock()

        mock_boto3_client.return_value = mock_ec2_client

        mock_ec2_client.describe_regions.return_value = {
            "Regions": [
                {"RegionName": "us-east-1"},
                {"RegionName": "us-west-2"},
                {"RegionName": "eu-west-1"},
            ]
        }

        provider_retriever._aws_ec2_client = mock_ec2_client

        expected_regions = ["us-east-1", "us-west-2", "eu-west-1"]

        actual_regions = provider_retriever._retrieve_enabled_aws_regions()
        self.assertEqual(actual_regions, expected_regions)

    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    def test_retrieve_enabled_aws_regions_empty(self, mock_boto3_client):
        with patch("os.environ.get") as mock_os_environ_get, patch(
            "caribou.common.utils.str_to_bool"
        ) as mock_str_to_bool, patch.object(ProviderRetriever, "__init__", lambda x, y: None):
            mock_os_environ_get.return_value = "test_key"
            mock_str_to_bool.return_value = False
            provider_retriever = ProviderRetriever(None)

        provider_retriever._google_api_key = "mock_api_key"

        mock_ec2_client = MagicMock()
        mock_boto3_client.return_value = mock_ec2_client

        mock_ec2_client.describe_regions.return_value = {"Regions": []}

        provider_retriever._aws_ec2_client = mock_ec2_client

        expected_regions = []

        actual_regions = provider_retriever._retrieve_enabled_aws_regions()
        self.assertEqual(actual_regions, expected_regions)

    @patch("caribou.data_collector.components.provider.provider_retriever.boto3.client")
    def test_retrieve_enabled_aws_regions_api_failure(self, mock_boto3_client):
        with patch("os.environ.get") as mock_os_environ_get, patch(
            "caribou.common.utils.str_to_bool"
        ) as mock_str_to_bool, patch.object(ProviderRetriever, "__init__", lambda x, y: None):
            mock_os_environ_get.return_value = "test_key"
            mock_str_to_bool.return_value = False
            provider_retriever = ProviderRetriever(None)

        provider_retriever._google_api_key = "mock_api_key"

        mock_ec2_client = MagicMock()
        mock_boto3_client.return_value = mock_ec2_client

        mock_ec2_client.describe_regions.side_effect = Exception("AWS API error")

        provider_retriever._aws_ec2_client = mock_ec2_client

        with self.assertRaises(Exception) as context:
            provider_retriever._retrieve_enabled_aws_regions()
        self.assertTrue("AWS API error" in str(context.exception))


class TestProviderRetrieverExtended(unittest.TestCase):
    def setUp(self):
        with (
            patch("boto3.client") as mock_boto3,
            patch("caribou.common.utils.str_to_bool") as mock_str_to_bool,
            patch("googlemaps.Client") as mock_googlemaps_client,
            patch("google.cloud.billing_v1.CloudCatalogClient") as mock_google_cloud_catalog_client,
        ):
            self.remote_client = MagicMock(spec=RemoteClient)
            mock_boto3.return_value = MagicMock()

            test_environment = {"GOOGLE_API_KEY": "test_key", "INTEGRATIONTEST_ON": "False"}

            self.env_patcher = patch.dict("os.environ", test_environment)
            self.env_patcher.start()

            mock_str_to_bool.return_value = False
            self.provider_retriever = ProviderRetriever(self.remote_client)

    def tearDown(self):
        self.env_patcher.stop()

    # Test for _unit_price_to_float function (line 24)
    def test_unit_price_to_float(self):
        # Test with whole units only
        price = Money.Money()
        price.units = 5
        price.nanos = 0
        result = _unit_price_to_float(price)
        self.assertEqual(result, 5.0)

        # Test with nanos only
        price = Money.Money()
        price.units = 0
        price.nanos = 500000000  # 0.5
        result = _unit_price_to_float(price)
        self.assertEqual(result, 0.5)

        # Test with both units and nanos
        price = Money.Money()
        price.units = 3
        price.nanos = 750000000  # 0.75
        result = _unit_price_to_float(price)
        self.assertEqual(result, 3.75)

    # Test for __init__ with INTEGRATIONTEST_ON = True (line 34)
    @patch("boto3.client")
    @patch("caribou.common.utils.str_to_bool")
    @patch("google.cloud.billing_v1.CloudCatalogClient")
    def test_init_integration_test_on(self, mock_catalog_client, mock_str_to_bool, mock_boto3):
        mock_str_to_bool.return_value = True
        mock_boto3.return_value = MagicMock()

        with patch.dict(os.environ, {"INTEGRATIONTEST_ON": "True"}):
            provider_retriever = ProviderRetriever(self.remote_client)
            self.assertTrue(provider_retriever._integration_test_on)

    # Test for __init__ without GOOGLE_API_KEY (line 50)
    @patch("boto3.client")
    @patch("caribou.common.utils.str_to_bool")
    @patch("google.cloud.billing_v1.CloudCatalogClient")
    def test_init_no_google_api_key(self, mock_catalog_client, mock_str_to_bool, mock_boto3):
        mock_str_to_bool.return_value = False
        mock_boto3.return_value = MagicMock()

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError) as context:
                ProviderRetriever(self.remote_client)
            self.assertIn("GOOGLE_API_KEY environment variable not set", str(context.exception))

    # Test for retrieve_location with special cases (lines 67, 79)
    @patch("googlemaps.Client")
    def test_retrieve_location_special_cases(self, mock_googlemaps_client):
        mock_client = MagicMock()
        mock_googlemaps_client.return_value = mock_client

        # Test Columbus case
        mock_client.geocode.return_value = [{"geometry": {"location": {"lat": 39.9612, "lng": -82.9988}}}]
        self.provider_retriever._google_api_key = "test_key"

        lat, lng = self.provider_retriever.retrieve_location("Columbus")
        mock_client.geocode.assert_called_with("Columbus, Ohio")
        self.assertEqual((lat, lng), (39.9612, -82.9988))

        # Test Canada (Central) case
        mock_client.geocode.return_value = [{"geometry": {"location": {"lat": 45.5839, "lng": -73.4803}}}]
        lat, lng = self.provider_retriever.retrieve_location("Canada (Central)")
        mock_client.geocode.assert_called_with("Varennes, QC")

        # Test Malaysia case
        mock_client.geocode.return_value = [{"geometry": {"location": {"lat": 3.1390, "lng": 101.6869}}}]
        lat, lng = self.provider_retriever.retrieve_location("Malaysia")
        mock_client.geocode.assert_called_with("Kuala Lumpur, Malaysia")

    # Test for retrieve_available_regions with INTEGRATION_TEST_PROVIDER (lines 206-230)
    @patch.object(ProviderRetriever, "retrieve_integrationtest_regions")
    @patch.object(ProviderRetriever, "retrieve_aws_regions")
    @patch.object(ProviderRetriever, "retrieve_gcp_regions")
    def test_retrieve_available_regions_integration_test(self, mock_gcp, mock_aws, mock_integration):
        self.provider_retriever._integration_test_on = True

        mock_integration.return_value = {f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {"code": "rivendell"}}
        mock_aws.return_value = {}
        mock_gcp.return_value = {}

        result = self.provider_retriever.retrieve_available_regions()

        mock_integration.assert_called_once()
        self.assertIn(f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell", result)

    # Test for retrieve_integrationtest_regions (lines 206-230)
    def test_retrieve_integrationtest_regions(self):
        result = self.provider_retriever.retrieve_integrationtest_regions()

        expected_regions = ["rivendell", "lothlorien", "anduin", "fangorn"]
        for region in expected_regions:
            key = f"{Provider.INTEGRATION_TEST_PROVIDER.value}:{region}"
            self.assertIn(key, result)
            self.assertEqual(result[key]["provider"], Provider.INTEGRATION_TEST_PROVIDER.value)
            self.assertEqual(result[key]["code"], region)
            self.assertIn("latitude", result[key])
            self.assertIn("longitude", result[key])

    # Test for retrieve_gcp_regions (lines 138-171)
    @patch("requests.get")
    @patch("googlemaps.Client")
    def test_retrieve_gcp_regions(self, mock_googlemaps_client, mock_requests_get):
        mock_html_content = """
        <html>
            <body>
                <table>
                    <tr>
                        <th>Zone</th>
                        <th>Location</th>
                    </tr>
                    <tr>
                        <td>us-central1-a</td>
                        <td>Iowa, USA</td>
                    </tr>
                    <tr>
                        <td>europe-west1-b</td>
                        <td>Belgium</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        mock_response = MagicMock()
        mock_response.content = mock_html_content
        mock_requests_get.return_value = mock_response

        mock_googlemaps_client.return_value.geocode.return_value = [
            {"geometry": {"location": {"lat": 41.2619, "lng": -95.8608}}}
        ]

        result = self.provider_retriever.retrieve_gcp_regions()

        self.assertIn("gcp:us-central1", result)
        self.assertIn("gcp:europe-west1", result)
        self.assertEqual(result["gcp:us-central1"]["name"], "Iowa, USA")
        self.assertEqual(result["gcp:europe-west1"]["name"], "Belgium")

    # Test for retrieve_gcp_regions with no tables (line 174)
    @patch("requests.get")
    def test_retrieve_gcp_regions_no_tables(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.content = "<html><body></body></html>"
        mock_requests_get.return_value = mock_response

        with self.assertRaises(ValueError) as context:
            self.provider_retriever.retrieve_gcp_regions()
        self.assertIn("Could not find any tables on the GCP regions page", str(context.exception))

    # Test for retrieve_provider_region_data with exception handling (lines 264-271)
    @patch.object(ProviderRetriever, "_retrieve_provider_data_aws")
    @patch.object(ProviderRetriever, "_retrieve_provider_data_gcp")
    @patch.object(ProviderRetriever, "_retrieve_provider_data_integrationtest")
    def test_retrieve_provider_region_data_with_exception(self, mock_integration, mock_gcp, mock_aws):
        self.provider_retriever._available_regions = {
            "aws:us-east-1": {"provider": "aws"},
            "gcp:us-central1": {"provider": "gcp"},
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {
                "provider": Provider.INTEGRATION_TEST_PROVIDER.value
            },
        }

        # Make AWS retrieval fail
        mock_aws.side_effect = Exception("AWS API error")
        mock_gcp.return_value = {"gcp:us-central1": {"data": "gcp_data"}}
        mock_integration.return_value = {
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell": {"data": "integration_data"}
        }

        with patch("builtins.print") as mock_print:
            result = self.provider_retriever.retrieve_provider_region_data()

        # Should have caught the AWS exception
        mock_print.assert_called_with("Error while retrieving provider data for aws: AWS API error")

        # Should still have GCP and integration test data
        self.assertIn("gcp:us-central1", result)
        self.assertIn(f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell", result)

    # Test for _retrieve_provider_data_integrationtest (lines 289-387)
    def test_retrieve_provider_data_integrationtest(self):
        regions = [
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:rivendell",
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:lothlorien",
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:anduin",
            f"{Provider.INTEGRATION_TEST_PROVIDER.value}:fangorn",
        ]

        result = self.provider_retriever._retrieve_provider_data_integrationtest(regions)

        for region in regions:
            self.assertIn(region, result)
            self.assertIn("execution_cost", result[region])
            self.assertIn("transmission_cost", result[region])
            self.assertIn("sns_cost", result[region])
            self.assertIn("dynamodb_cost", result[region])
            self.assertIn("ecr_cost", result[region])
            self.assertEqual(result[region]["pue"], 1.11)
            self.assertEqual(result[region]["cfe"], 0.0)
            self.assertIn("available_architectures", result[region])

    # Test for _retrieve_aws_available_architectures (lines 405-410)
    def test_retrieve_aws_available_architectures(self):
        # Test with both architectures available
        execution_cost = {"invocation_cost": {"arm64": 0.0001, "x86_64": 0.0002}}
        result = self.provider_retriever._retrieve_aws_available_architectures(execution_cost)
        self.assertEqual(result, ["arm64", "x86_64"])

        # Test with only x86_64 available
        execution_cost = {"invocation_cost": {"arm64": 0, "x86_64": 0.0002}}
        result = self.provider_retriever._retrieve_aws_available_architectures(execution_cost)
        self.assertEqual(result, ["x86_64"])

        # Test with only arm64 available
        execution_cost = {"invocation_cost": {"arm64": 0.0001, "x86_64": 0}}
        result = self.provider_retriever._retrieve_aws_available_architectures(execution_cost)
        self.assertEqual(result, ["arm64"])

    # Test for _retrieve_gcp_available_architectures (lines 413-415)
    def test_retrieve_gcp_available_architectures(self):
        result = self.provider_retriever._retrieve_gcp_available_architectures()
        self.assertEqual(result, ["x86_64"])

    # Test for _retrieve_aws_pue (lines 479-493)
    def test_retrieve_aws_pue(self):
        regions = [
            "aws:us-east-1",
            "aws:eu-central-1",
            "aws:ap-southeast-1",
            "aws:ca-central-1",
            "aws:sa-east-1",
            "aws:af-south-1",
            "aws:me-south-1",
            "aws:mx-central-1",
            "aws:unknown-region",
        ]

        result = self.provider_retriever._retrieve_aws_pue(regions)

        self.assertEqual(result["aws:us-east-1"], 1.15)
        self.assertEqual(result["aws:eu-central-1"], 1.35)
        self.assertEqual(result["aws:ap-southeast-1"], 1.32)
        self.assertEqual(result["aws:ca-central-1"], 1.19)
        self.assertEqual(result["aws:sa-east-1"], 1.17)
        self.assertEqual(result["aws:af-south-1"], 1.24)
        self.assertEqual(result["aws:me-south-1"], 1.31)
        self.assertEqual(result["aws:mx-central-1"], 1.14)
        self.assertEqual(result["aws:unknown-region"], 1.15)  # Default value

    # Test for _retrieve_gcp_pue (lines 532-549)
    def test_retrieve_gcp_pue(self):
        regions = ["gcp:us-east1", "gcp:us-east4", "gcp:europe-west1", "gcp:asia-southeast1", "gcp:unknown-region"]

        result = self.provider_retriever._retrieve_gcp_pue(regions)

        self.assertEqual(result["gcp:us-east1"], 1.1)
        self.assertEqual(result["gcp:us-east4"], 1.08)
        self.assertEqual(result["gcp:europe-west1"], 1.08)
        self.assertEqual(result["gcp:asia-southeast1"], 1.13)
        self.assertEqual(result["gcp:unknown-region"], 1.09)  # Default value

    # Test for _retrieve_gcp_pubsub_cost (lines 557-585)
    def test_retrieve_gcp_pubsub_cost(self):
        regions = ["gcp:us-central1", "gcp:europe-west1"]

        result = self.provider_retriever._retrieve_gcp_pubsub_cost(regions)

        expected_cost = 40 / (1024 * 1024 * 1024)
        for region in regions:
            self.assertIn(region, result)
            self.assertEqual(result[region]["request_cost"], expected_cost)
            self.assertEqual(result[region]["unit"], "USD/requests")

    # Test for get_dynamodb_on_demand_skus (lines 626-684)
    def test_get_dynamodb_on_demand_skus(self):
        price_list_json = {
            "products": {
                "sku1": {
                    "product": {
                        "productFamily": "Amazon DynamoDB PayPerRequest Throughput",
                        "attributes": {"group": "DDB-ReadUnits"},
                    }
                },
                "sku2": {
                    "product": {
                        "productFamily": "Amazon DynamoDB PayPerRequest Throughput",
                        "attributes": {"group": "DDB-WriteUnits"},
                    }
                },
                "sku3": {"product": {"productFamily": "Database Storage"}},
            }
        }

        read_sku, write_sku, storage_sku = self.provider_retriever.get_dynamodb_on_demand_skus(price_list_json)

        self.assertEqual(read_sku, "sku1")
        self.assertEqual(write_sku, "sku2")
        self.assertEqual(storage_sku, "sku3")

    # Test for get_cost (line 714)
    def test_get_cost_empty_sku(self):
        price_list_json = {"terms": {"OnDemand": {}}}
        result = self.provider_retriever.get_cost(price_list_json, "")
        self.assertEqual(result, 0.0)

    # Test for _retrieve_gcp_firestore_cost with no Firestore service (line 867)
    @patch.object(billing_v1.CloudCatalogClient, "list_services")
    def test_retrieve_gcp_firestore_cost_no_service(self, mock_list_services):
        mock_list_services.return_value = []

        with self.assertRaises(RuntimeError) as context:
            self.provider_retriever._retrieve_gcp_firestore_cost(["gcp:us-central1"])
        self.assertIn("Could not find Firestore service", str(context.exception))

    # Test for _retrieve_gcp_artifact_registry_cost with no service (line 933)
    @patch.object(billing_v1.CloudCatalogClient, "list_services")
    def test_retrieve_gcp_artifact_registry_cost_no_service(self, mock_list_services):
        mock_list_services.return_value = []

        with self.assertRaises(RuntimeError) as context:
            self.provider_retriever._retrieve_gcp_artifact_registry_cost(["gcp:us-central1"])
        self.assertIn("Could not find artifact registry service", str(context.exception))

    # Test for _retrieve_gcp_transmission_cost (lines 1021-1071)
    def test_retrieve_gcp_transmission_cost(self):
        regions = [
            "gcp:us-central1",
            "gcp:africa-south1",
            "gcp:asia-southeast2",
            "gcp:northamerica-south1",
            "gcp:europe-west1",
            "gcp:me-central2",
            "gcp:australia-southeast1",
            "gcp:southamerica-east1",
        ]

        result = self.provider_retriever._retrieve_gcp_transmission_cost(regions)

        # Check specific regions
        self.assertEqual(result["gcp:us-central1"]["global_data_transfer"], 0.12)
        self.assertEqual(result["gcp:us-central1"]["provider_data_transfer"], 0.02)

        self.assertEqual(result["gcp:africa-south1"]["global_data_transfer"], 0.15)
        self.assertEqual(result["gcp:africa-south1"]["provider_data_transfer"], 0.08)

        self.assertEqual(result["gcp:asia-southeast2"]["global_data_transfer"], 0.19)
        self.assertEqual(result["gcp:asia-southeast2"]["provider_data_transfer"], 0.1)

        self.assertEqual(result["gcp:southamerica-east1"]["global_data_transfer"], 0.19)
        self.assertEqual(result["gcp:southamerica-east1"]["provider_data_transfer"], 0.14)

    # Test for _gcp_execution_fallback (line 1186)
    def test_gcp_execution_fallback(self):
        # Test tier 1 region
        result = self.provider_retriever._gcp_execution_fallback("gcp:us-central1")
        self.assertEqual(result["compute_cost"]["cpu_s"], 0.000024)
        self.assertEqual(result["compute_cost"]["memory_gb_s"], 0.0000025)

        # Test tier 2 region
        result = self.provider_retriever._gcp_execution_fallback("gcp:africa-south1")
        self.assertEqual(result["compute_cost"]["cpu_s"], 0.0000336)
        self.assertEqual(result["compute_cost"]["memory_gb_s"], 0.0000035)

        # Test unknown region
        with self.assertRaises(ValueError) as context:
            self.provider_retriever._gcp_execution_fallback("gcp:unknown-region")
        self.assertIn("Region not found", str(context.exception))


if __name__ == "__main__":
    unittest.main()
