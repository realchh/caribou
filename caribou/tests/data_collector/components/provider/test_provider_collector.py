import unittest
from unittest.mock import MagicMock, Mock, patch
from caribou.data_collector.components.provider.provider_exporter import ProviderExporter
from caribou.data_collector.components.provider.provider_retriever import ProviderRetriever
from caribou.data_collector.components.provider.provider_collector import ProviderCollector


class TestProviderCollector(unittest.TestCase):
    def setUp(self):
        # We patch ProviderRetriever where it's used inside the provider_collector module.
        # This patch will replace the real ProviderRetriever with a mock.
        self.retriever_patcher = patch(
            "caribou.data_collector.components.provider.provider_collector.ProviderRetriever"
        )

        # We also patch the Exporter, as the `run` method will try to create it.
        self.exporter_patcher = patch("caribou.data_collector.components.provider.provider_collector.ProviderExporter")

        # Start the patchers and get the mock classes
        self.mock_retriever_class = self.retriever_patcher.start()
        self.mock_exporter_class = self.exporter_patcher.start()

        # Ensure the patchers are stopped after each test
        self.addCleanup(self.retriever_patcher.stop)
        self.addCleanup(self.exporter_patcher.stop)

        # Now it's safe to instantiate the ProviderCollector because its
        # dependencies (ProviderRetriever and ProviderExporter) are mocked.
        self.provider_collector = ProviderCollector()

    def test_run(
        self,
    ):
        # --- Arrange ---
        # The __init__ of ProviderCollector created mock instances of the retriever and exporter.
        # We can now access them through the class attributes.
        mock_retriever_instance = self.provider_collector._data_retriever
        mock_exporter_instance = self.provider_collector._data_exporter

        # Configure the return values for the methods that will be called
        mock_retriever_instance.retrieve_available_regions.return_value = {
            "aws:region1": {"Region Specification Data": "Data"}
        }
        mock_retriever_instance.retrieve_provider_region_data.return_value = {
            "aws:region1": {"Provider Region Data": "Data"}
        }
        mock_exporter_instance.get_modified_regions.return_value = {"aws:region1"}

        # --- Act ---
        self.provider_collector.run()

        # --- Assert ---
        # Verify that the methods on our mock instances were called as expected
        mock_retriever_instance.retrieve_available_regions.assert_called_once()
        mock_exporter_instance.export_available_region_table.assert_called_once_with(
            {"aws:region1": {"Region Specification Data": "Data"}}
        )
        mock_retriever_instance.retrieve_provider_region_data.assert_called_once()
        mock_exporter_instance.export_all_data.assert_called_once_with(
            {"aws:region1": {"Provider Region Data": "Data"}}, {}
        )
        mock_exporter_instance.get_modified_regions.assert_called_once()
        mock_exporter_instance.update_available_region_timestamp.assert_called_once_with(
            "provider_collector", {"aws:region1"}
        )


if __name__ == "__main__":
    unittest.main()
