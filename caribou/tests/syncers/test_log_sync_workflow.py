import unittest
from unittest.mock import Mock, call, patch
from datetime import datetime, timedelta
import json
from caribou.syncers.log_sync_workflow import LogSyncWorkflow
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.syncers.components.workflow_run_sample import WorkflowRunSample
from caribou.common.constants import (
    WORKFLOW_SUMMARY_TABLE,
    TIME_FORMAT,
    TIME_FORMAT_DAYS,
    FORGETTING_TIME_DAYS,
    GLOBAL_TIME_ZONE,
    BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD,
    FORGETTING_NUMBER,
    LOG_VERSION,
)


class TestLogSyncWorkflow(unittest.TestCase):
    def setUp(self):
        # Initialize LogSyncWorkflow with mock data
        self.workflow_id = "test_workflow_id"
        self.region_clients = {("region1", "client1"): Mock(spec=RemoteClient)}
        self.deployment_manager_config_str = '{"deployed_regions": "{}"}'
        self.time_intervals_to_sync = [(datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE))]
        self.workflow_summary_client = Mock(spec=RemoteClient)
        self.previous_data = {"key": "value"}

        # Instantiate the LogSyncWorkflow
        self.log_sync_workflow = LogSyncWorkflow(
            self.workflow_id,
            self.region_clients,
            self.deployment_manager_config_str,
            self.time_intervals_to_sync,
            self.workflow_summary_client,
            self.previous_data,
        )

    def test_init(self):
        # Test that initialization sets up attributes correctly
        self.assertEqual(self.log_sync_workflow.workflow_id, self.workflow_id)
        self.assertEqual(self.log_sync_workflow._region_clients, self.region_clients)
        self.assertEqual(self.log_sync_workflow._workflow_summary_client, self.workflow_summary_client)
        self.assertEqual(self.log_sync_workflow._previous_data, self.previous_data)
        self.assertEqual(self.log_sync_workflow._collected_logs, {})
        self.assertEqual(self.log_sync_workflow._deployed_regions, {})

    def test_load_information(self):
        # Test that deployment information is loaded correctly
        config_str = '{"deployed_regions": "{\\"region1\\": \\"client1\\"}"}'
        self.log_sync_workflow._load_information(config_str)
        expected = {"region1": "client1"}
        self.assertEqual(self.log_sync_workflow._deployed_regions, expected)

    @patch("caribou.syncers.log_sync_workflow.RemoteClientFactory.get_remote_client")
    def test_get_remote_client(self, get_remote_client_mock):
        # Test getting a remote client
        get_remote_client_mock.return_value = Mock(spec=RemoteClient)
        provider_region = {"provider": "test_provider", "region": "test_region"}

        # Call the method and get the result
        result = self.log_sync_workflow._get_remote_client(provider_region)

        # Check that the result is a RemoteClient
        self.assertIsInstance(result, RemoteClient)

        # Check that the RemoteClient was added to _region_clients
        self.assertIn(("test_provider", "test_region"), self.log_sync_workflow._region_clients)

        # Check that get_remote_client was called with the correct arguments
        get_remote_client_mock.assert_called_once_with("test_provider", "test_region")

    @patch("caribou.syncers.log_sync_workflow.RemoteClientFactory.get_remote_client")
    def test_get_remote_client_cached(self, get_remote_client_mock):
        """Test that remote client is cached and not recreated"""
        mock_client = Mock(spec=RemoteClient)
        self.log_sync_workflow._region_clients[("test_provider", "test_region")] = mock_client

        provider_region = {"provider": "test_provider", "region": "test_region"}
        result = self.log_sync_workflow._get_remote_client(provider_region)

        # Should return cached client without calling factory
        self.assertEqual(result, mock_client)
        get_remote_client_mock.assert_not_called()

    @patch.object(LogSyncWorkflow, "_sync_logs")
    @patch.object(LogSyncWorkflow, "_prepare_data_for_upload")
    @patch.object(LogSyncWorkflow, "_upload_data")
    def test_sync_workflow(self, upload_data_mock, prepare_data_for_upload_mock, sync_logs_mock):
        # Test the sync_workflow method
        prepare_data_for_upload_mock.return_value = "{}"

        # Call the method
        self.log_sync_workflow.sync_workflow()

        # Check that the mocks were called in the correct order with the correct arguments
        sync_logs_mock.assert_called_once()
        prepare_data_for_upload_mock.assert_called_once_with(self.previous_data)
        upload_data_mock.assert_called_once_with("{}")

    def test_upload_data(self):
        # Test the _upload_data method
        data_for_upload = "test_data"
        self.log_sync_workflow._upload_data(data_for_upload)

        # Check that update_value_in_table was called with the correct arguments
        self.workflow_summary_client.update_value_in_table.assert_called_once_with(
            WORKFLOW_SUMMARY_TABLE,
            self.workflow_id,
            data_for_upload,
            convert_to_bytes=True,
        )

    @patch.object(LogSyncWorkflow, "_process_logs_for_instance_for_one_region")
    @patch.object(LogSyncWorkflow, "_check_to_forget")
    def test_sync_logs(self, check_to_forget_mock, process_logs_for_instance_for_one_region_mock):
        # Test the _sync_logs method
        self.log_sync_workflow._deployed_regions = {
            "function1": {"deploy_region": {"provider": "aws", "region": "us-east-1"}},
            "function2": {"deploy_region": {"provider": "aws", "region": "us-east-2"}},
        }
        self.log_sync_workflow._time_intervals_to_sync = [
            (datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE))
        ]

        # Call the method
        self.log_sync_workflow._sync_logs()

        # Check that the mocks were called with the correct arguments
        calls = [
            call(
                "function1",
                {"provider": "aws", "region": "us-east-1"},
                self.log_sync_workflow._time_intervals_to_sync[0][0],
                self.log_sync_workflow._time_intervals_to_sync[0][1],
            ),
            call(
                "function2",
                {"provider": "aws", "region": "us-east-2"},
                self.log_sync_workflow._time_intervals_to_sync[0][0],
                self.log_sync_workflow._time_intervals_to_sync[0][1],
            ),
        ]
        process_logs_for_instance_for_one_region_mock.assert_has_calls(calls)
        check_to_forget_mock.assert_called_once()

    @patch.object(LogSyncWorkflow, "_process_logs_for_instance_for_one_region")
    @patch.object(LogSyncWorkflow, "_check_to_forget")
    def test_sync_logs_multiple_intervals(self, check_to_forget_mock, process_logs_mock):
        """Test syncing logs with multiple time intervals"""
        now = datetime.now(GLOBAL_TIME_ZONE)
        self.log_sync_workflow._deployed_regions = {
            "function1": {"deploy_region": {"provider": "aws", "region": "us-east-1"}},
        }
        self.log_sync_workflow._time_intervals_to_sync = [
            (now - timedelta(hours=2), now - timedelta(hours=1)),
            (now - timedelta(hours=1), now),
        ]

        self.log_sync_workflow._sync_logs()

        # Should be called twice for each interval
        self.assertEqual(process_logs_mock.call_count, 2)
        check_to_forget_mock.assert_called_once()

    @patch.object(LogSyncWorkflow, "_process_logs_for_instance_for_one_region")
    @patch.object(LogSyncWorkflow, "_check_to_forget")
    def test_sync_logs_empty_regions(self, check_to_forget_mock, process_logs_mock):
        """Test syncing logs with no deployed regions"""
        self.log_sync_workflow._deployed_regions = {}
        self.log_sync_workflow._time_intervals_to_sync = [
            (datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE))
        ]

        self.log_sync_workflow._sync_logs()

        process_logs_mock.assert_not_called()
        check_to_forget_mock.assert_called_once()

    @patch.object(LogSyncWorkflow, "_get_remote_client")
    @patch.object(LogSyncWorkflow, "_process_log_entry")
    @patch.object(LogSyncWorkflow, "_setup_lambda_insights")
    def test_process_logs_for_instance_for_one_region(
        self, setup_lambda_insights_mock, process_log_entry_mock, get_remote_client_mock
    ):
        # Test processing logs for one region
        functions_instance = "test_instance"
        provider_region = {"provider": "test_provider", "region": "test_region"}
        time_from = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)

        # Set up the return value for _get_remote_client
        mock_remote_client = Mock()
        mock_remote_client.get_logs_between.return_value = ["[CARIBOU] log1", "log2"]
        mock_remote_client.get_insights_logs_between.return_value = ["insight_log1"]
        get_remote_client_mock.return_value = mock_remote_client

        # Call the method
        self.log_sync_workflow._process_logs_for_instance_for_one_region(
            functions_instance, provider_region, time_from, time_to
        )

        # Check that the mocks were called with the correct arguments
        get_remote_client_mock.assert_called_once_with(provider_region)
        mock_remote_client.get_logs_between.assert_called_once_with(functions_instance, time_from, time_to)
        mock_remote_client.get_insights_logs_between.assert_called_once_with(
            functions_instance,
            time_from - timedelta(minutes=BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD),
            time_to + timedelta(minutes=BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD),
        )
        process_log_entry_mock.assert_any_call("[CARIBOU] log1", provider_region, time_to)
        setup_lambda_insights_mock.assert_called_once_with(["insight_log1"])

    @patch.object(LogSyncWorkflow, "_get_remote_client")
    @patch.object(LogSyncWorkflow, "_process_log_entry")
    @patch.object(LogSyncWorkflow, "_setup_lambda_insights")
    def test_process_logs_for_instance_no_caribou_logs(
        self, setup_lambda_insights_mock, process_log_entry_mock, get_remote_client_mock
    ):
        """Test processing when there are no CARIBOU logs"""
        functions_instance = "test_instance"
        provider_region = {"provider": "test_provider", "region": "test_region"}
        time_from = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)

        mock_remote_client = Mock()
        mock_remote_client.get_logs_between.return_value = ["regular log1", "regular log2"]
        mock_remote_client.get_insights_logs_between.return_value = []
        get_remote_client_mock.return_value = mock_remote_client

        self.log_sync_workflow._process_logs_for_instance_for_one_region(
            functions_instance, provider_region, time_from, time_to
        )

        # Should not process any logs since none start with [CARIBOU]
        process_log_entry_mock.assert_not_called()
        setup_lambda_insights_mock.assert_called_once_with([])

    def test_setup_lambda_insights(self):
        # Test setting up lambda insights
        logs = [
            json.dumps(
                {
                    "request_id": "1",
                    "duration": 100,
                    "cold_start": True,
                    "memory_utilization": 50,
                    "total_network": 10,
                    "cpu_total_time": 5,
                }
            ),
            json.dumps(
                {
                    "request_id": "2",
                    "duration": 200,
                    "cold_start": False,
                    "memory_utilization": 60,
                    "total_network": 20,
                    "cpu_total_time": 10,
                }
            ),
        ]

        # Call the method
        self.log_sync_workflow._setup_lambda_insights(logs)

        # Check the _insights_logs attribute
        expected_result = {
            "1": {
                "duration": 0.1,
                "cold_start": True,
                "memory_utilization": 50,
                "total_network": 10,
                "cpu_total_time": 0.005,
            },
            "2": {
                "duration": 0.2,
                "cold_start": False,
                "memory_utilization": 60,
                "total_network": 20,
                "cpu_total_time": 0.01,
            },
        }
        self.assertEqual(self.log_sync_workflow._insights_logs, expected_result)

    def test_process_log_entry(self):
        # Test processing a log entry
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        log_entry = f"[CARIBOU]	2024-08-02T16:43:12.323Z	366b3663-2679-447c-86a0-ea2d8df06bcf	TIME (2024-08-02 16:43:12,323041+0000) RUN_ID (5f627048-fbc7-4a6f-9eee-309de1ea852a) MESSAGE (WPD_OVERRIDE: WPD was overriden by debug_workflow_placement_override OVERRIDING_WORKFLOW_PLACEMENT_SIZE (1.8067657947540283e-06) GB) LOG_VERSION (0.0.4)"
        provider_region = {"provider": "test_provider", "region": "test_region"}

        # Call the method
        self.log_sync_workflow._process_log_entry(log_entry, provider_region, time_to)

        # Check that the workflow_run_sample is created and updated
        run_id = "5f627048-fbc7-4a6f-9eee-309de1ea852a"
        self.assertIn(run_id, self.log_sync_workflow._collected_logs)
        workflow_run_sample = self.log_sync_workflow._collected_logs[run_id]
        self.assertIsInstance(workflow_run_sample, WorkflowRunSample)
        self.assertIn("366b3663-2679-447c-86a0-ea2d8df06bcf", workflow_run_sample.request_ids)

    def test_process_log_entry_blacklisted_run_id(self):
        """Test that blacklisted run IDs are ignored"""
        blacklisted_run_id = "blacklisted-run-id"
        self.log_sync_workflow._blacklisted_run_ids.add(blacklisted_run_id)

        log_entry = f"[CARIBOU] TIME (2024-08-02 16:43:12,323041+0000) RUN_ID ({blacklisted_run_id}) MESSAGE (test)"
        provider_region = {"provider": "test", "region": "test"}

        self.log_sync_workflow._process_log_entry(log_entry, provider_region, datetime.now(GLOBAL_TIME_ZONE))

        # Should not be added to collected logs
        self.assertNotIn(blacklisted_run_id, self.log_sync_workflow._collected_logs)

    def test_extract_from_string(self):
        # Test extracting a string from a log entry
        log_entry = "RequestId: test_request_id\t"
        regex = r"RequestId: (.*?)\t"
        result = self.log_sync_workflow._extract_from_string(log_entry, regex)
        self.assertEqual(result, "test_request_id")

    def test_extract_from_string_no_match(self):
        # Test extracting a string with no match
        log_entry = "RequestId: test_request_id\t"
        regex = r"RUN_ID: (.*?)\t"
        result = self.log_sync_workflow._extract_from_string(log_entry, regex)
        self.assertIsNone(result)

    def test_check_to_forget(self):
        # Test the _check_to_forget method
        self.log_sync_workflow._collected_logs = {
            "run1": Mock(spec=WorkflowRunSample, request_ids={"request1"}),
            "run2": Mock(spec=WorkflowRunSample, request_ids={"request2"}),
            "run3": Mock(spec=WorkflowRunSample, request_ids={"request3"}),
        }
        self.log_sync_workflow._tainted_cold_start_samples = {"request2"}
        self.log_sync_workflow._blacklisted_run_ids = set()

        # Call the method
        self.log_sync_workflow._check_to_forget()

        # Check that the tainted run was removed from _collected_logs and added to _blacklisted_run_ids
        self.assertNotIn("run2", self.log_sync_workflow._collected_logs)
        self.assertIn("run2", self.log_sync_workflow._blacklisted_run_ids)

        # Check that _forgetting is False because the size of _collected_logs is not equal to FORGETTING_NUMBER
        self.assertFalse(self.log_sync_workflow._forgetting)

    def test_check_to_forget_forgetting_threshold(self):
        """Test forgetting behavior when threshold is reached"""
        # Create enough logs to trigger forgetting
        collected_logs = {}
        for i in range(FORGETTING_NUMBER):
            collected_logs[f"run{i}"] = Mock(spec=WorkflowRunSample, request_ids={f"request{i}"})

        self.log_sync_workflow._collected_logs = collected_logs
        self.log_sync_workflow._tainted_cold_start_samples = set()

        self.log_sync_workflow._check_to_forget()

        # Should enable forgetting mode
        self.assertTrue(self.log_sync_workflow._forgetting)

    def test_format_region(self):
        # Test formatting a region string
        region = {"provider": "aws", "region": "us-east-1"}
        result = self.log_sync_workflow._format_region(region)
        self.assertEqual(result, "aws:us-east-1")

    def test_format_region_none(self):
        # Test formatting a region string with None
        result = self.log_sync_workflow._format_region(None)
        self.assertIsNone(result)

    def test_format_region_missing_keys(self):
        """Test formatting region with missing keys"""
        incomplete_region = {"provider": "aws"}  # Missing region
        with self.assertRaises(KeyError):
            self.log_sync_workflow._format_region(incomplete_region)

    def test_fill_up_collected_logs(self):
        # Test filling up collected logs
        now = datetime.now(GLOBAL_TIME_ZONE)
        collected_logs = [{"start_time": (now - timedelta(days=1)).strftime(TIME_FORMAT)}]
        previous_data = {
            "logs": [
                {"start_time": (now - timedelta(days=5)).strftime(TIME_FORMAT)},
                {"start_time": (now - timedelta(days=4)).strftime(TIME_FORMAT)},
                {"start_time": (now - timedelta(days=3)).strftime(TIME_FORMAT)},
                {"start_time": (now - timedelta(days=2)).strftime(TIME_FORMAT)},
            ]
        }

        # Call the method
        self.log_sync_workflow._fill_up_collected_logs(collected_logs, previous_data)

        # Check that the collected_logs list was updated as expected
        expected_result = [
            {"start_time": (now - timedelta(days=5)).strftime(TIME_FORMAT)},
            {"start_time": (now - timedelta(days=4)).strftime(TIME_FORMAT)},
            {"start_time": (now - timedelta(days=3)).strftime(TIME_FORMAT)},
            {"start_time": (now - timedelta(days=2)).strftime(TIME_FORMAT)},
            {"start_time": (now - timedelta(days=1)).strftime(TIME_FORMAT)},
        ]
        self.assertEqual(collected_logs, expected_result)

    def test_fill_up_collected_logs_already_full(self):
        # Test that collected logs are not updated when they are already full
        now = datetime.now(GLOBAL_TIME_ZONE)

        collected_logs = [{"start_time": (now - timedelta(days=i)).strftime(TIME_FORMAT)} for i in range(2, -1, -1)]
        previous_data = {
            "logs": [{"start_time": (now - timedelta(days=i)).strftime(TIME_FORMAT)} for i in range(3, 2, -1)]
        }
        # Call the method
        self.log_sync_workflow._fill_up_collected_logs(collected_logs, previous_data)

        # Check that the collected_logs list was not updated because it was already full
        expected_result = [{"start_time": (now - timedelta(days=i)).strftime(TIME_FORMAT)} for i in range(3, -1, -1)]
        self.assertEqual(collected_logs, expected_result)

    def test_fill_up_collected_logs_no_previous_logs(self):
        """Test filling up when there are no previous logs"""
        collected_logs = []
        previous_data = {}

        self.log_sync_workflow._fill_up_collected_logs(collected_logs, previous_data)

        # Should remain empty
        self.assertEqual(collected_logs, [])

    @patch.object(LogSyncWorkflow, "_extend_existing_execution_instance_region")
    @patch.object(LogSyncWorkflow, "_extend_existing_transmission_from_instance_to_instance_region")
    @patch("caribou.syncers.log_sync_workflow.WorkflowRunSample")
    def test_format_collected_logs(
        self,
        WorkflowRunSampleMock,
        extend_existing_transmission_from_instance_to_instance_region_mock,
        extend_existing_execution_instance_region_mock,
    ):
        # Test formatting collected logs
        WorkflowRunSampleMock.return_value.is_valid_and_complete.return_value = True
        WorkflowRunSampleMock.return_value.to_dict.return_value = (
            "2022-01-01T00:00:00,000+00:00",
            {
                "execution_data": [{"instance_name": "function1", "provider_region": "provider1:region1"}],
                "transmission_data": [
                    {
                        "from_instance": "instance1",
                        "to_instance": "instance2",
                        "from_region": "provider1:region1",
                        "to_region": "provider2:region2",
                        "transmission_size": 1.0,
                    }
                ],
            },
        )

        self.log_sync_workflow._collected_logs = {"workflow1": WorkflowRunSampleMock()}

        # Call the method
        result = self.log_sync_workflow._format_collected_logs()

        # Check that the result is as expected
        expected_result = [
            {
                "execution_data": [{"instance_name": "function1", "provider_region": "provider1:region1"}],
                "transmission_data": [
                    {
                        "from_instance": "instance1",
                        "to_instance": "instance2",
                        "from_region": "provider1:region1",
                        "to_region": "provider2:region2",
                        "transmission_size": 1.0,
                    }
                ],
            }
        ]
        self.assertEqual(result, expected_result)

        # Check that the mocks were called with the correct arguments
        extend_existing_execution_instance_region_mock.assert_called_once_with(expected_result[0])
        extend_existing_transmission_from_instance_to_instance_region_mock.assert_called_once_with(expected_result[0])

    def test_filter_daily_invocation_counts(self):
        # Test filtering daily invocation counts
        now = datetime.now(GLOBAL_TIME_ZONE)
        previous_daily_invocation_counts = {
            (now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i for i in range(FORGETTING_TIME_DAYS + 2)
        }

        # Call the method
        self.log_sync_workflow._filter_daily_counts(previous_daily_invocation_counts)

        # Check that the previous_daily_invocation_counts dictionary was updated as expected
        expected_result = {(now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i for i in range(FORGETTING_TIME_DAYS)}
        self.assertEqual(previous_daily_invocation_counts, expected_result)

    def test_merge_daily_invocation_counts(self):
        # Test merging daily invocation counts
        now = datetime.now(GLOBAL_TIME_ZONE)
        previous_daily_invocation_counts = {(now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i for i in range(5)}
        self.log_sync_workflow._daily_invocation_set = {
            (now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): set(range(i, i + 5)) for i in range(5)
        }

        # Call the method
        self.log_sync_workflow._merge_daily_invocation_counts(previous_daily_invocation_counts)

        # Check that the previous_daily_invocation_counts dictionary was updated as expected
        expected_result = {(now - timedelta(days=i)).strftime(TIME_FORMAT_DAYS): i + 5 for i in range(5)}
        self.assertEqual(previous_daily_invocation_counts, expected_result)

    def test_check_for_missing_execution_instance_region(self):
        # Test checking for missing execution instance region
        previous_log = {
            "execution_data": {
                "function1": {"provider_region": "provider1:region1"},
                "function2": {"provider_region": "provider1:region2"},
            }
        }
        self.log_sync_workflow._existing_data = {
            "execution_instance_region": {
                "function1": {"provider1:region1": 5},
            }
        }

        # Call the method
        result = self.log_sync_workflow._check_for_missing_execution_instance_region(previous_log)

        # Check that the _existing_data dictionary was updated as expected
        expected_data = {
            "execution_instance_region": {
                "function1": {"provider1:region1": 6},
                "function2": {"provider1:region2": 1},
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

        # Check that the method returned the correct value
        self.assertTrue(result)

    def test_extend_existing_execution_instance_region(self):
        # Test extending existing execution instance region
        log = {
            "execution_data": [
                {"instance_name": "function1", "provider_region": "provider1:region1"},
                {"instance_name": "function2", "provider_region": "provider1:region2"},
            ]
        }
        self.log_sync_workflow._existing_data = {
            "execution_instance_region": {
                "function1": {"provider1:region1": 5},
            }
        }

        # Call the method
        self.log_sync_workflow._extend_existing_execution_instance_region(log)

        # Check that the _existing_data dictionary was updated as expected
        expected_data = {
            "execution_instance_region": {
                "function1": {"provider1:region1": 6},
                "function2": {"provider1:region2": 1},
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

    def test_extend_existing_transmission_from_instance_to_instance_region(self):
        # Test extending existing transmission from instance to instance region
        log = {
            "transmission_data": [
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region2",
                    "transmission_size": 1.0,
                    "from_direct_successor": True,
                    "successor_invoked": True,
                },
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region3",
                    "transmission_size": 2.0,
                    "from_direct_successor": True,
                    "successor_invoked": False,
                },
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region4",
                    "transmission_size": 5.0,
                    "from_direct_successor": True,
                    "successor_invoked": True,
                },
            ]
        }
        self.log_sync_workflow._existing_data = {
            "transmission_from_instance_to_instance_region": {
                "instance1": {
                    "instance2": {
                        "provider1:region1": {"provider2:region2": 5},
                    }
                }
            }
        }

        # Call the method
        self.log_sync_workflow._extend_existing_transmission_from_instance_to_instance_region(log)

        # Check that the _existing_data dictionary was updated as expected
        expected_data = {
            "transmission_from_instance_to_instance_region": {
                "instance1": {
                    "instance2": {
                        "provider1:region1": {"provider2:region2": 6, "provider2:region3": 0, "provider2:region4": 1},
                    }
                }
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

    def test_check_for_missing_transmission_from_instance_to_instance_region(self):
        # Test checking for missing transmission from instance to instance region
        previous_log = {
            "transmission_data": [
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region2",
                },
                {
                    "from_instance": "instance1",
                    "to_instance": "instance2",
                    "from_region": "provider1:region1",
                    "to_region": "provider2:region3",
                },
            ]
        }
        self.log_sync_workflow._existing_data = {
            "transmission_from_instance_to_instance_region": {
                "instance1": {
                    "instance2": {
                        "provider1:region1": {"provider2:region2": 5},
                    }
                }
            }
        }

        # Call the method
        result = self.log_sync_workflow._check_for_missing_transmission_from_instance_to_instance_region(previous_log)

        # Check that the _existing_data dictionary was updated as expected
        expected_data = {
            "transmission_from_instance_to_instance_region": {
                "instance1": {
                    "instance2": {
                        "provider1:region1": {"provider2:region2": 6, "provider2:region3": 1},
                    }
                }
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

        # Check that the method returned the correct value
        self.assertTrue(result)

    @patch.object(LogSyncWorkflow, "_get_remote_client")
    @patch.object(LogSyncWorkflow, "_process_log_entry")
    @patch.object(LogSyncWorkflow, "_setup_gcp_insights")
    def test_process_logs_for_instance_gcp_provider(
        self, setup_gcp_insights_mock, process_log_entry_mock, get_remote_client_mock
    ):
        """Test processing logs specifically for GCP provider"""
        functions_instance = "test-gcp-function"
        provider_region = {"provider": "gcp", "region": "us-central1"}
        time_from = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)

        # Set up GCP-specific mock remote client with proper JSON format
        mock_gcp_client = Mock()
        mock_gcp_client.get_logs_between.return_value = [
            json.dumps(
                {
                    "jsonPayload": {"severity": "CARIBOU", "message": "test message"},
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                }
            ),
            json.dumps(
                {
                    "jsonPayload": {"severity": "CARIBOU", "message": "test message 2"},
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                }
            ),
            json.dumps(
                {"jsonPayload": {"severity": "OTHER", "message": "non-caribou"}, "logName": "projects/test/logs/other"}
            ),
        ]
        get_remote_client_mock.return_value = mock_gcp_client

        self.log_sync_workflow._process_logs_for_instance_for_one_region(
            functions_instance, provider_region, time_from, time_to
        )

        # Verify GCP client methods were called
        get_remote_client_mock.assert_called_once_with(provider_region)
        mock_gcp_client.get_logs_between.assert_called_once_with(functions_instance, time_from, time_to)

        # Should setup GCP insights and process logs
        setup_gcp_insights_mock.assert_called_once()
        # Should process only CARIBOU logs (2 out of 3)
        self.assertEqual(process_log_entry_mock.call_count, 2)

    @patch.object(LogSyncWorkflow, "_get_remote_client")
    def test_process_logs_gcp_client_error_handling(self, get_remote_client_mock):
        """Test error handling for GCP client failures"""
        mock_gcp_client = Mock()
        mock_gcp_client.get_logs_between.side_effect = Exception("GCP API error")
        get_remote_client_mock.return_value = mock_gcp_client

        provider_region = {"provider": "gcp", "region": "us-central1"}

        # Test that the exception is properly raised
        with self.assertRaises(Exception) as context:
            self.log_sync_workflow._process_logs_for_instance_for_one_region(
                "test-function", provider_region, datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE)
            )

        self.assertEqual(str(context.exception), "GCP API error")

    def test_format_region_gcp(self):
        """Test formatting GCP provider region"""
        gcp_region = {"provider": "gcp", "region": "us-central1"}
        result = self.log_sync_workflow._format_region(gcp_region)
        self.assertEqual(result, "gcp:us-central1")

    @patch.object(LogSyncWorkflow, "_get_remote_client")
    @patch.object(LogSyncWorkflow, "_process_log_entry")
    @patch.object(LogSyncWorkflow, "_setup_gcp_insights")
    def test_process_logs_gcp_cloud_logging_format(
        self, setup_gcp_insights_mock, process_log_entry_mock, get_remote_client_mock
    ):
        """Test processing GCP Cloud Logging specific log formats"""
        functions_instance = "test-gcp-function"
        provider_region = {"provider": "gcp", "region": "us-central1"}
        time_from = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)

        # GCP Cloud Logging specific log format with proper JSON structure
        gcp_logs = [
            json.dumps(
                {
                    "jsonPayload": {
                        "severity": "CARIBOU",
                        "message": f"TIME (2024-08-02 16:43:12,323041+0000) RUN_ID (gcp-run-id-123) MESSAGE (GCP Cloud Run execution) LOG_VERSION ({LOG_VERSION})",
                    },
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                    "trace": "projects/test/traces/gcp-trace-123",
                }
            ),
            json.dumps(
                {
                    "jsonPayload": {
                        "severity": "CARIBOU",
                        "message": f"TIME (2024-08-02 16:43:13,456078+0000) RUN_ID (gcp-run-id-456) MESSAGE (GCP Pub/Sub trigger) LOG_VERSION ({LOG_VERSION})",
                    },
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                    "trace": "projects/test/traces/gcp-trace-456",
                }
            ),
        ]

        mock_gcp_client = Mock()
        mock_gcp_client.get_logs_between.return_value = gcp_logs
        get_remote_client_mock.return_value = mock_gcp_client

        self.log_sync_workflow._process_logs_for_instance_for_one_region(
            functions_instance, provider_region, time_from, time_to
        )

        # Verify both GCP logs were processed
        setup_gcp_insights_mock.assert_called_once()
        self.assertEqual(process_log_entry_mock.call_count, 2)

        # Check that the logs were processed with correct parameters
        call_args = process_log_entry_mock.call_args_list
        self.assertEqual(len(call_args), 2)

    @patch("caribou.syncers.log_sync_workflow.RemoteClientFactory.get_remote_client")
    def test_get_remote_client_gcp_caching(self, get_remote_client_mock):
        """Test that GCP remote client is properly cached"""
        from caribou.common.models.remote_client.gcp_remote_client import GCPRemoteClient

        mock_gcp_client = Mock(spec=GCPRemoteClient)
        get_remote_client_mock.return_value = mock_gcp_client

        provider_region = {"provider": "gcp", "region": "us-central1"}

        # First call should create client
        result1 = self.log_sync_workflow._get_remote_client(provider_region)

        # Second call should return cached client
        result2 = self.log_sync_workflow._get_remote_client(provider_region)

        # Should be the same instance
        self.assertEqual(result1, result2)
        self.assertEqual(result1, mock_gcp_client)

        # Factory should only be called once
        get_remote_client_mock.assert_called_once_with("gcp", "us-central1")

        # Client should be cached
        self.assertIn(("gcp", "us-central1"), self.log_sync_workflow._region_clients)

    def test_process_log_entry_gcp_specific_formats(self):
        """Test processing GCP-specific log entry formats"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)

        # GCP Cloud Run log format - properly formatted JSON with complete message format
        gcp_log_entry = json.dumps(
            {
                "jsonPayload": {
                    "severity": "CARIBOU",
                    "message": f"TIME (2024-08-02 16:43:12,323041+0000) RUN_ID (gcp-cloud-run-12345) MESSAGE (GCP_CLOUD_RUN: Function execution started) LOG_VERSION ({LOG_VERSION})",
                },
                "logName": "projects/test/logs/cloud-run",  # Not a request log
                "trace": "projects/test/traces/366b3663-2679-447c-86a0-ea2d8df06bcf",
            }
        )
        provider_region = {"provider": "gcp", "region": "us-central1"}

        self.log_sync_workflow._process_log_entry(gcp_log_entry, provider_region, time_to)

        # Verify GCP run ID was processed
        run_id = "gcp-cloud-run-12345"
        self.assertIn(run_id, self.log_sync_workflow._collected_logs)

        workflow_run_sample = self.log_sync_workflow._collected_logs[run_id]
        self.assertIsInstance(workflow_run_sample, WorkflowRunSample)
        # Check that the request ID from trace was added
        self.assertIn("366b3663-2679-447c-86a0-ea2d8df06bcf", workflow_run_sample.request_ids)

    @patch.object(LogSyncWorkflow, "_process_logs_for_instance_for_one_region")
    @patch.object(LogSyncWorkflow, "_check_to_forget")
    def test_sync_logs_mixed_providers_including_gcp(self, check_to_forget_mock, process_logs_mock):
        """Test syncing logs from mixed providers including GCP"""
        self.log_sync_workflow._deployed_regions = {
            "aws-function": {"deploy_region": {"provider": "aws", "region": "us-east-1"}},
            "gcp-function": {"deploy_region": {"provider": "gcp", "region": "us-central1"}},
            "azure-function": {"deploy_region": {"provider": "azure", "region": "eastus"}},
        }
        self.log_sync_workflow._time_intervals_to_sync = [
            (datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE))
        ]

        self.log_sync_workflow._sync_logs()

        # Should process all three providers
        self.assertEqual(process_logs_mock.call_count, 3)

        # Verify GCP function was included
        gcp_call_found = False
        for call in process_logs_mock.call_args_list:
            args, _ = call
            if args[0] == "gcp-function" and args[1]["provider"] == "gcp":
                gcp_call_found = True
                break

        self.assertTrue(gcp_call_found, "GCP function processing call not found")
        check_to_forget_mock.assert_called_once()

    def test_extend_existing_execution_instance_region_gcp(self):
        """Test extending execution instance region data with GCP regions"""
        log = {
            "execution_data": [
                {"instance_name": "gcp-function1", "provider_region": "gcp:us-central1"},
                {"instance_name": "gcp-function2", "provider_region": "gcp:europe-west1"},
                {"instance_name": "aws-function", "provider_region": "aws:us-east-1"},
            ]
        }
        self.log_sync_workflow._existing_data = {
            "execution_instance_region": {
                "gcp-function1": {"gcp:us-central1": 3},
            }
        }

        self.log_sync_workflow._extend_existing_execution_instance_region(log)

        expected_data = {
            "execution_instance_region": {
                "gcp-function1": {"gcp:us-central1": 4},
                "gcp-function2": {"gcp:europe-west1": 1},
                "aws-function": {"aws:us-east-1": 1},
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

    def test_extend_existing_transmission_gcp_regions(self):
        """Test extending transmission data with GCP regions"""
        log = {
            "transmission_data": [
                {
                    "from_instance": "gcp-function1",
                    "to_instance": "gcp-function2",
                    "from_region": "gcp:us-central1",
                    "to_region": "gcp:europe-west1",
                    "transmission_size": 2.5,
                    "from_direct_successor": True,
                    "successor_invoked": True,
                },
                {
                    "from_instance": "gcp-function1",
                    "to_instance": "aws-function",
                    "from_region": "gcp:us-central1",
                    "to_region": "aws:us-east-1",
                    "transmission_size": 1.5,
                    "from_direct_successor": True,
                    "successor_invoked": True,
                },
            ]
        }
        self.log_sync_workflow._existing_data = {"transmission_from_instance_to_instance_region": {}}

        self.log_sync_workflow._extend_existing_transmission_from_instance_to_instance_region(log)

        expected_data = {
            "transmission_from_instance_to_instance_region": {
                "gcp-function1": {
                    "gcp-function2": {"gcp:us-central1": {"gcp:europe-west1": 1}},
                    "aws-function": {"gcp:us-central1": {"aws:us-east-1": 1}},
                }
            }
        }
        self.assertEqual(self.log_sync_workflow._existing_data, expected_data)

    def test_setup_lambda_insights_gcp_metrics(self):
        """Test setting up insights with GCP-specific metrics (if applicable)"""
        # Note: This tests the actual _setup_lambda_insights method with GCP-like data
        # but the method itself doesn't differentiate between providers
        gcp_metrics_logs = [
            json.dumps(
                {
                    "request_id": "gcp-request-1",
                    "duration": 150,  # milliseconds
                    "cold_start": True,
                    "memory_utilization": 45,
                    # Note: GCP metrics might not have all AWS Lambda fields
                }
            ),
            json.dumps(
                {
                    "request_id": "gcp-request-2",
                    "duration": 85,
                    "cold_start": False,
                    "memory_utilization": 32,
                }
            ),
        ]

        self.log_sync_workflow._setup_lambda_insights(gcp_metrics_logs)

        # The actual method only processes fields that exist in the log
        expected_result = {
            "gcp-request-1": {
                "duration": 0.15,  # converted to seconds
                "cold_start": True,
                "memory_utilization": 45,
            },
            "gcp-request-2": {
                "duration": 0.085,
                "cold_start": False,
                "memory_utilization": 32,
            },
        }
        self.assertEqual(self.log_sync_workflow._insights_logs, expected_result)

    @patch.object(LogSyncWorkflow, "_get_remote_client")
    def test_process_logs_gcp_empty_response(self, get_remote_client_mock):
        """Test processing when GCP client returns empty logs"""
        mock_gcp_client = Mock()
        mock_gcp_client.get_logs_between.return_value = []
        get_remote_client_mock.return_value = mock_gcp_client

        provider_region = {"provider": "gcp", "region": "us-central1"}

        # When logs are empty, the method should return early
        self.log_sync_workflow._process_logs_for_instance_for_one_region(
            "test-function", provider_region, datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE)
        )

        # Should call get_logs_between but return early due to empty logs
        mock_gcp_client.get_logs_between.assert_called_once()

    # Fix the failing test_prepare_data_for_upload_complete by mocking the format method correctly
    @patch.object(LogSyncWorkflow, "_format_collected_logs")
    @patch.object(LogSyncWorkflow, "_fill_up_collected_logs")
    @patch.object(LogSyncWorkflow, "_filter_daily_counts")
    @patch.object(LogSyncWorkflow, "_merge_daily_invocation_counts")
    def test_prepare_data_for_upload_complete_fixed(self, merge_mock, filter_mock, fill_mock, format_mock):
        """Test complete data preparation for upload"""
        # Mock format_collected_logs to return proper structure with runtime_s
        format_mock.return_value = [{"test": "data", "runtime_s": 1.5}, {"test": "data2", "runtime_s": 2.0}]

        previous_data = {
            "logs": [],
            "daily_invocation_counts": {},
            "daily_user_code_failure_counts": {},  # Add this missing field
            "execution_instance_region": {},
            "transmission_from_instance_to_instance_region": {},
        }

        result = self.log_sync_workflow._prepare_data_for_upload(previous_data)

        # Verify all methods were called
        merge_mock.assert_called_once()
        filter_mock.assert_called()  # Called twice for different data types
        fill_mock.assert_called_once()
        format_mock.assert_called_once()

        # Should return JSON string
        self.assertIsInstance(result, str)


if __name__ == "__main__":
    unittest.main()
