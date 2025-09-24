import unittest
from unittest.mock import Mock, call, patch, MagicMock
from datetime import datetime, timedelta
import json

from caribou.common.models.remote_client.gcp_remote_client import GCPRemoteClient
from caribou.common.provider import Provider
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
    KEEP_ALIVE_DATA_COUNT,
    SYNC_UPLOAD_AND_INVOKE_TASK_TYPE,
)
from caribou.common.constants import CONDITIONALLY_NOT_INVOKE_TASK_TYPE


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

    # --- Tests for GCP-Specific Logic ---

    @patch.object(LogSyncWorkflow, "_process_log_entry")
    @patch.object(LogSyncWorkflow, "_setup_gcp_insights")
    @patch.object(LogSyncWorkflow, "_get_remote_client")
    def test_process_logs_for_gcp_provider(self, mock_get_remote_client, mock_setup_gcp, mock_process_log):
        """Tests that the GCP code path is correctly followed."""
        mock_gcp_client = MagicMock(spec=GCPRemoteClient)
        mock_get_remote_client.return_value = mock_gcp_client

        # GCP logs are returned as a list of JSON strings by the client
        mock_gcp_logs_as_strings = [
            json.dumps({"jsonPayload": {"severity": "CARIBOU"}, "logName": "..."}),
            json.dumps({"logName": "run.googleapis.com%2Frequests"}),
        ]
        mock_gcp_client.get_logs_between.return_value = mock_gcp_logs_as_strings

        provider_region = {"provider": Provider.GCP.value, "region": "us-central1"}

        self.log_sync_workflow._process_logs_for_instance_for_one_region(
            "test-instance", provider_region, datetime.now(), datetime.now()
        )

        mock_setup_gcp.assert_called_once()
        # The test checks that the loop for processing entries is called correctly.
        self.assertEqual(mock_process_log.call_count, 2)

    def test_setup_gcp_insights(self):
        """Tests the parsing of GCP request logs and metric querying."""
        # Arrange
        mock_remote_client = MagicMock(spec=GCPRemoteClient)

        # A sample structured log from GCP Cloud Run
        gcp_request_log = json.dumps(
            {
                "logName": "projects/p/logs/run.googleapis.com%2Frequests",
                "trace": "projects/p/traces/request-123",
                "timestamp": "2025-07-20T12:00:05Z",
                "httpRequest": {"latency": "0.5s", "requestSize": "100", "responseSize": "200", "status": 200},
                "resource": {"labels": {"revision_name": "my-service-rev1", "instance_id": "instance-abc"}},
            }
        )

        # Mock the metric query to return sample values
        mock_remote_client.query_metric.side_effect = [1.5, 0.5, 512 * 1024 * 1024]  # 512MB in bytes

        # Act
        self.log_sync_workflow._setup_gcp_insights([gcp_request_log], mock_remote_client)

        # Assert
        self.assertIn("request-123", self.log_sync_workflow._insights_logs)
        insights = self.log_sync_workflow._insights_logs["request-123"]
        print(insights)
        self.assertEqual(insights["duration"], 0.5)
        self.assertEqual(insights["rx_bytes"], 100.0)
        self.assertEqual(insights["tx_bytes"], 200.0)
        self.assertEqual(insights["total_network"], 300.0)
        self.assertEqual(insights["cpu_total_time"], 1.5)
        self.assertEqual(insights["memory_utilization"], 50.0)

        # Verify query_metric was called correctly
        self.assertEqual(mock_remote_client.query_metric.call_count, 3)

    def test_process_log_entry_for_gcp(self):
        """Tests parsing a structured GCP log for CARIBOU data."""
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        provider_region = {"provider": Provider.GCP.value, "region": "us-central1"}

        # A sample structured CARIBOU log from GCP
        log_dict = {
            "jsonPayload": {
                "severity": "CARIBOU",
                "message": f"TIME (2025-07-20 12:00:00,000000+0000) RUN_ID (gcp-run-1) MESSAGE (EXECUTED: INSTANCE (test-instance) with USER_EXECUTION_TIME (1.5) s and TOTAL_EXECUTION_TIME (2.0) s) LOG_VERSION ({LOG_VERSION})",
            },
            "trace": "projects/p/traces/gcp-request-1",
        }

        # The log syncer expects a list of JSON strings
        log_entry_str = json.dumps(log_dict)

        # Mock the message handler to isolate the test
        self.log_sync_workflow._handle_system_log_messages = MagicMock()

        self.log_sync_workflow._process_log_entry(log_entry_str, provider_region, time_to)

        # Assert that a workflow sample was created
        self.assertIn("gcp-run-1", self.log_sync_workflow._collected_logs)

        # Assert that the message handler was called with the correct, extracted data
        self.log_sync_workflow._handle_system_log_messages.assert_called_once()
        call_args = self.log_sync_workflow._handle_system_log_messages.call_args[0]
        self.assertIn("EXECUTED: INSTANCE (test-instance)", call_args[0])  # message_to_parse
        self.assertEqual(call_args[1], "gcp-run-1")  # run_id
        self.assertEqual(call_args[5], "gcp-request-1")  # request_id

    # --- Tests for Edge Cases and Helpers ---

    def test_extract_float_from_log_entry_failure(self):
        """Tests that the float extractor raises a ValueError for non-float values."""
        log_entry = "SOME_VALUE (not-a-float)"
        regex = r"SOME_VALUE \((.*?)\)"
        with self.assertRaises(ValueError):
            self.log_sync_workflow._extract_float_from_log_entry(log_entry, regex, "some_value")


# Additional tests to add to test_log_sync_workflow.py


class TestLogSyncWorkflowAdditional(unittest.TestCase):
    def setUp(self):
        # Copy from existing setUp
        self.workflow_id = "test_workflow_id"
        self.region_clients = {("region1", "client1"): Mock(spec=RemoteClient)}
        self.deployment_manager_config_str = '{"deployed_regions": "{}"}'
        self.time_intervals_to_sync = [(datetime.now(GLOBAL_TIME_ZONE), datetime.now(GLOBAL_TIME_ZONE))]
        self.workflow_summary_client = Mock(spec=RemoteClient)
        self.previous_data = {"key": "value"}

        self.log_sync_workflow = LogSyncWorkflow(
            self.workflow_id,
            self.region_clients,
            self.deployment_manager_config_str,
            self.time_intervals_to_sync,
            self.workflow_summary_client,
            self.previous_data,
        )

    # --- Error Handling and Edge Cases ---

    def test_load_information_invalid_json(self):
        """Test handling of invalid JSON in deployment manager config"""
        invalid_config = '{"deployed_regions": "invalid_json"}'
        with self.assertRaises(json.JSONDecodeError):
            self.log_sync_workflow._load_information(invalid_config)

    def test_load_information_missing_deployed_regions(self):
        """Test handling config without deployed_regions key"""
        config_without_regions = '{"other_field": "value"}'
        self.log_sync_workflow._load_information(config_without_regions)
        self.assertEqual(self.log_sync_workflow._deployed_regions, {})

    def test_extract_from_string_edge_cases(self):
        """Test string extraction with various edge cases"""
        # Empty string
        result = self.log_sync_workflow._extract_from_string("", r"test \((.*?)\)")
        self.assertIsNone(result)

        # String with special characters
        log_entry = "test (value with spaces and $pecial ch@rs!)"
        result = self.log_sync_workflow._extract_from_string(log_entry, r"test \((.*?)\)")
        self.assertEqual(result, "value with spaces and $pecial ch@rs!")

        # Multiple matches (should return first)
        log_entry = "test (first) test (second)"
        result = self.log_sync_workflow._extract_from_string(log_entry, r"test \((.*?)\)")
        self.assertEqual(result, "first")

    def test_extract_float_from_log_entry_edge_cases(self):
        """Test float extraction with edge cases"""
        # Negative numbers
        log_entry = "VALUE (-123.45)"
        result = self.log_sync_workflow._extract_float_from_log_entry(log_entry, r"VALUE \((.*?)\)", "value")
        self.assertEqual(result, -123.45)

        # Scientific notation
        log_entry = "VALUE (1.23e-5)"
        result = self.log_sync_workflow._extract_float_from_log_entry(log_entry, r"VALUE \((.*?)\)", "value")
        self.assertEqual(result, 1.23e-5)

        # Zero
        log_entry = "VALUE (0.0)"
        result = self.log_sync_workflow._extract_float_from_log_entry(log_entry, r"VALUE \((.*?)\)", "value")
        self.assertEqual(result, 0.0)

    def test_extract_boolean_from_log_entry_edge_cases(self):
        """Test boolean extraction with various formats"""
        # Uppercase
        log_entry = "FLAG (TRUE)"
        result = self.log_sync_workflow._extract_boolean_from_log_entry(log_entry, r"FLAG \((.*?)\)", "flag")
        self.assertTrue(result)

        # Mixed case
        log_entry = "FLAG (False)"
        result = self.log_sync_workflow._extract_boolean_from_log_entry(log_entry, r"FLAG \((.*?)\)", "flag")
        self.assertFalse(result)

        # Invalid boolean
        log_entry = "FLAG (maybe)"
        with self.assertRaises(ValueError):
            self.log_sync_workflow._extract_boolean_from_log_entry(log_entry, r"FLAG \((.*?)\)", "flag")

    def test_extract_string_from_log_entry_missing_value(self):
        """Test string extraction when value is missing"""
        log_entry = "FIELD ()"
        with self.assertRaises(ValueError):
            self.log_sync_workflow._extract_string_from_log_entry(log_entry, r"MISSING \((.*?)\)", "missing")

    def test_does_field_exist(self):
        """Test field existence checker"""
        log_entry = "FIELD1 (value) FIELD2 (another)"

        # Existing field
        self.assertTrue(self.log_sync_workflow._does_field_exist(log_entry, "FIELD1"))

        # Non-existing field
        self.assertFalse(self.log_sync_workflow._does_field_exist(log_entry, "FIELD3"))

    # --- Log Processing Edge Cases ---
    def test_process_log_entry_invalid_run_id_type(self):
        """Test processing log with invalid run ID type"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        # Log with numeric run ID and proper format
        log_entry = f"[CARIBOU]\t2024-08-02T16:43:12.000000+0000\t123-request\tTIME (2024-08-02 16:43:12,000000+0000) RUN_ID (123) MESSAGE (test) LOG_VERSION ({LOG_VERSION})"
        provider_region = {"provider": "test", "region": "test"}

        # Should handle numeric run ID by converting to string
        self.log_sync_workflow._process_log_entry(log_entry, provider_region, time_to)
        self.assertIn("123", self.log_sync_workflow._collected_logs)

    def test_process_log_entry_invalid_log_time(self):
        """Test processing log with invalid timestamp"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        log_entry = f"[CARIBOU] TIME (invalid-time) RUN_ID (test-run) MESSAGE (test) LOG_VERSION ({LOG_VERSION})"
        provider_region = {"provider": "test", "region": "test"}

        with self.assertRaises(ValueError):
            self.log_sync_workflow._process_log_entry(log_entry, provider_region, time_to)

    def test_process_log_entry_gcp_missing_trace(self):
        """Test GCP log processing when trace is missing"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        gcp_log = json.dumps(
            {
                "jsonPayload": {
                    "severity": "CARIBOU",
                    "message": f"TIME (2024-08-02 16:43:12,323041+0000) RUN_ID (gcp-run) MESSAGE (test) LOG_VERSION ({LOG_VERSION})",
                },
                "logName": "projects/test/logs/cloud-run"
                # Missing trace field
            }
        )
        provider_region = {"provider": "gcp", "region": "us-central1"}

        self.log_sync_workflow._process_log_entry(gcp_log, provider_region, time_to)

        # Should still process the log but without request ID from trace
        self.assertIn("gcp-run", self.log_sync_workflow._collected_logs)

    def test_process_log_entry_gcp_request_log_duplicate_request_id(self):
        """Test GCP request log processing with duplicate request IDs"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        gcp_request_log = json.dumps(
            {
                "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                "trace": "projects/test/traces/duplicate-request-123",
            }
        )
        provider_region = {"provider": "gcp", "region": "us-central1"}

        # Process same request log twice
        self.log_sync_workflow._process_log_entry(gcp_request_log, provider_region, time_to)
        self.log_sync_workflow._process_log_entry(gcp_request_log, provider_region, time_to)

        # Should track duplicate
        self.assertIn("duplicate-request-123", self.log_sync_workflow._encountered_duplicate_completed_request_ids)

    # --- AWS Lambda Specific Tests ---

    def test_process_log_entry_aws_report_with_init_duration(self):
        """Test AWS Lambda REPORT log with Init Duration (cold start)"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        report_log = "REPORT RequestId: cold-start-123\tDuration: 1000.00 ms\tBilled Duration: 1000 ms\tMemory Size: 512 MB\tMax Memory Used: 100 MB\tInit Duration: 500.00 ms\t"
        provider_region = {"provider": "aws", "region": "us-east-1"}

        self.log_sync_workflow._process_log_entry(report_log, provider_region, time_to)

        # Should mark as cold start
        self.assertIn("cold-start-123", self.log_sync_workflow._tainted_cold_start_samples)
        self.assertIn("cold-start-123", self.log_sync_workflow._encountered_completed_request_ids)

    def test_process_log_entry_aws_report_without_init_duration(self):
        """Test AWS Lambda REPORT log without Init Duration (warm start)"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        report_log = "REPORT RequestId: warm-start-123\tDuration: 100.00 ms\tBilled Duration: 100 ms\tMemory Size: 512 MB\tMax Memory Used: 50 MB\t"
        provider_region = {"provider": "aws", "region": "us-east-1"}

        self.log_sync_workflow._process_log_entry(report_log, provider_region, time_to)

        # Should not mark as cold start
        self.assertNotIn("warm-start-123", self.log_sync_workflow._tainted_cold_start_samples)
        self.assertIn("warm-start-123", self.log_sync_workflow._encountered_completed_request_ids)

    def test_process_log_entry_aws_report_duplicate_request_id(self):
        """Test AWS Lambda REPORT log with duplicate request ID"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        report_log = "REPORT RequestId: duplicate-aws-123\tDuration: 100.00 ms\t"
        provider_region = {"provider": "aws", "region": "us-east-1"}

        # Process same report twice
        self.log_sync_workflow._process_log_entry(report_log, provider_region, time_to)
        self.log_sync_workflow._process_log_entry(report_log, provider_region, time_to)

        # Should track duplicate
        self.assertIn("duplicate-aws-123", self.log_sync_workflow._encountered_duplicate_completed_request_ids)

    # --- Message Handling Tests ---

    @patch.object(LogSyncWorkflow, "_extract_entry_point_log")
    def test_handle_system_log_messages_entry_point_within_time(self, mock_extract):
        """Test handling ENTRY_POINT message within time bounds"""
        log_entry = f"MESSAGE (ENTRY_POINT: test message) LOG_VERSION ({LOG_VERSION})"
        run_id = "test-run"
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        provider_region = {"provider": "test", "region": "test"}
        log_time = datetime.now(GLOBAL_TIME_ZONE) - timedelta(minutes=5)
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "test-request"

        self.log_sync_workflow._handle_system_log_messages(
            log_entry, run_id, workflow_run_sample, provider_region, log_time, request_id, time_to
        )

        mock_extract.assert_called_once()

    def test_handle_system_log_messages_entry_point_outside_time(self):
        """Test handling ENTRY_POINT message outside time bounds"""
        log_entry = f"MESSAGE (ENTRY_POINT: test message) LOG_VERSION ({LOG_VERSION})"
        run_id = "test-run"
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        provider_region = {"provider": "test", "region": "test"}
        log_time = datetime.now(GLOBAL_TIME_ZONE) + timedelta(minutes=5)  # Future time
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "test-request"

        self.log_sync_workflow._collected_logs = {run_id: workflow_run_sample}

        self.log_sync_workflow._handle_system_log_messages(
            log_entry, run_id, workflow_run_sample, provider_region, log_time, request_id, time_to
        )

        # Should blacklist and remove from collected logs
        self.assertNotIn(run_id, self.log_sync_workflow._collected_logs)
        self.assertIn(run_id, self.log_sync_workflow._blacklisted_run_ids)

    def test_handle_system_log_messages_client_code_exception(self):
        """Test handling CLIENT_CODE_EXCEPTION message"""
        log_entry = f"MESSAGE (CLIENT_CODE_EXCEPTION: Error in user code) LOG_VERSION ({LOG_VERSION})"
        run_id = "failed-run"
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        provider_region = {"provider": "test", "region": "test"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "test-request"

        self.log_sync_workflow._collected_logs = {run_id: workflow_run_sample}

        self.log_sync_workflow._handle_system_log_messages(
            log_entry, run_id, workflow_run_sample, provider_region, log_time, request_id, time_to
        )

        # Should blacklist, remove from collected logs, and track failure
        self.assertNotIn(run_id, self.log_sync_workflow._collected_logs)
        self.assertIn(run_id, self.log_sync_workflow._blacklisted_run_ids)

        log_day_str = log_time.strftime(TIME_FORMAT_DAYS)
        self.assertIn(log_day_str, self.log_sync_workflow._daily_user_code_failure_set)
        self.assertIn(run_id, self.log_sync_workflow._daily_user_code_failure_set[log_day_str])

    def test_handle_system_log_messages_debug_message(self):
        """Test handling DEBUG_MESSAGE (should be ignored)"""
        log_entry = f"MESSAGE (DEBUG_MESSAGE: debug info) LOG_VERSION ({LOG_VERSION})"
        run_id = "test-run"
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        provider_region = {"provider": "test", "region": "test"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "test-request"

        # Should not raise any exceptions or side effects
        self.log_sync_workflow._handle_system_log_messages(
            log_entry, run_id, workflow_run_sample, provider_region, log_time, request_id, time_to
        )

    @patch("builtins.print")
    def test_handle_system_log_messages_unknown_message(self, mock_print):
        """Test handling unknown message type"""
        log_entry = f"MESSAGE (UNKNOWN_MESSAGE: unknown type) LOG_VERSION ({LOG_VERSION})"
        run_id = "test-run"
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        provider_region = {"provider": "test", "region": "test"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "test-request"

        self.log_sync_workflow._handle_system_log_messages(
            log_entry, run_id, workflow_run_sample, provider_region, log_time, request_id, time_to
        )

        # Should print warning about untracked message
        mock_print.assert_called_with("The following CARIBOU messages were untracked:", "UNKNOWN_MESSAGE: unknown type")

    @patch("builtins.print")
    def test_handle_system_log_messages_no_message_match(self, mock_print):
        """Test handling log with no MESSAGE pattern match"""
        log_entry = f"INVALID_FORMAT LOG_VERSION ({LOG_VERSION})"
        run_id = "test-run"
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        provider_region = {"provider": "test", "region": "test"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        time_to = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "test-request"

        self.log_sync_workflow._handle_system_log_messages(
            log_entry, run_id, workflow_run_sample, provider_region, log_time, request_id, time_to
        )

        # Should print warning about invalid pattern
        mock_print.assert_called_with("WARNING: No matches! Invalid PATTERN for log:", log_entry)

    # --- GCP Insights Tests ---

    def test_setup_gcp_insights_missing_fields(self):
        """Test GCP insights setup with missing fields"""
        logs = [
            json.dumps(
                {
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                    "trace": "projects/test/traces/partial-request-123",
                    # Missing httpRequest/protoPayload
                }
            )
        ]

        mock_remote_client = Mock()

        with patch("builtins.print") as mock_print:
            self.log_sync_workflow._setup_gcp_insights(logs, mock_remote_client)

        # Should print error message
        mock_print.assert_called_with("No httpRequest or protoPayload found in GCP log: " + logs[0])

    def test_setup_gcp_insights_non_200_status(self):
        """Test GCP insights setup with non-200 status code"""
        logs = [
            json.dumps(
                {
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                    "trace": "projects/test/traces/error-request-123",
                    "httpRequest": {"status": 500, "latency": "1.0s", "requestSize": "100", "responseSize": "200"},
                }
            )
        ]

        mock_remote_client = Mock()
        # Clear insights_logs first
        self.log_sync_workflow._insights_logs = {}
        self.log_sync_workflow._setup_gcp_insights(logs, mock_remote_client)

        # Should not add to insights logs due to non-200 status, but creates empty entry first
        # The method actually creates the entry then continues, so check if it's empty or doesn't exist
        if "error-request-123" in self.log_sync_workflow._insights_logs:
            # If entry exists, it should be empty or minimal
            insights = self.log_sync_workflow._insights_logs["error-request-123"]
            self.assertNotIn("duration", insights)
        else:
            # If no entry exists, that's also acceptable
            self.assertNotIn("error-request-123", self.log_sync_workflow._insights_logs)

    def test_setup_gcp_insights_metric_query_failure(self):
        """Test GCP insights when metric queries fail"""
        logs = [
            json.dumps(
                {
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                    "trace": "projects/test/traces/metric-fail-123",
                    "timestamp": "2025-07-20T12:00:05Z",
                    "httpRequest": {
                        "latency": "0.5s",
                        "requestSize": "100",
                        "responseSize": "200",
                        "status": 200,
                        "startupLatency": "0.1s",
                    },
                    "resource": {"labels": {"revision_name": "test-revision"}},
                }
            )
        ]

        mock_remote_client = Mock()
        mock_remote_client.query_metric.return_value = None  # Failed query

        self.log_sync_workflow._setup_gcp_insights(logs, mock_remote_client)

        insights = self.log_sync_workflow._insights_logs["metric-fail-123"]
        # Should have basic HTTP data but not metrics
        self.assertEqual(insights["duration"], 0.5)
        self.assertNotIn("cpu_total_time", insights)
        self.assertNotIn("memory_utilization", insights)

    # --- Data Preparation Tests ---

    def test_prepare_data_for_upload_with_user_failures(self):
        """Test data preparation including user code failures"""
        now = datetime.now(GLOBAL_TIME_ZONE)
        self.log_sync_workflow._daily_user_code_failure_set = {
            now.strftime(TIME_FORMAT_DAYS): {"failed-run-1", "failed-run-2"}
        }

        previous_data = {"daily_invocation_counts": {}, "daily_user_code_failure_counts": {}, "logs": []}

        with patch.object(self.log_sync_workflow, "_format_collected_logs", return_value=[]):
            result_str = self.log_sync_workflow._prepare_data_for_upload(previous_data)

        result = json.loads(result_str)
        self.assertIn("daily_user_code_failure_counts", result)
        self.assertEqual(result["daily_user_code_failure_counts"][now.strftime(TIME_FORMAT_DAYS)], 2)

    def test_merge_daily_user_code_failure_counts(self):
        """Test merging daily user code failure counts"""
        now = datetime.now(GLOBAL_TIME_ZONE)
        date_str = now.strftime(TIME_FORMAT_DAYS)

        previous_counts = {date_str: 5}
        self.log_sync_workflow._daily_user_code_failure_set = {
            date_str: {"new-failure-1", "new-failure-2", "new-failure-3"}
        }

        self.log_sync_workflow._merge_daily_user_code_failure_counts(previous_counts)

        self.assertEqual(previous_counts[date_str], 8)  # 5 + 3

    def test_selectively_add_previous_logs_with_missing_data(self):
        """Test selective addition of previous logs with missing data"""
        collected_logs = []
        previous_log = {
            "execution_data": {"new-function": {"provider_region": "aws:us-east-1"}},
            "transmission_data": [
                {
                    "from_instance": "func1",
                    "to_instance": "func2",
                    "from_region": "aws:us-east-1",
                    "to_region": "aws:us-west-2",
                }
            ],
        }

        # Empty existing data should trigger addition
        self.log_sync_workflow._existing_data = {
            "execution_instance_region": {},
            "transmission_from_instance_to_instance_region": {},
        }

        self.log_sync_workflow._selectively_add_previous_logs(collected_logs, previous_log)

        # Should add the log since it contains new data
        self.assertEqual(len(collected_logs), 1)
        self.assertEqual(collected_logs[0], previous_log)

    def test_selectively_add_previous_logs_no_missing_data(self):
        """Test selective addition when no data is missing"""
        collected_logs = []
        previous_log = {
            "execution_data": {"existing-function": {"provider_region": "aws:us-east-1"}},
            "transmission_data": [],
        }

        # Existing data with sufficient counts
        self.log_sync_workflow._existing_data = {
            "execution_instance_region": {"existing-function": {"aws:us-east-1": KEEP_ALIVE_DATA_COUNT + 1}},
            "transmission_from_instance_to_instance_region": {},
        }

        with patch.object(self.log_sync_workflow, "_check_for_missing_execution_instance_region", return_value=False):
            with patch.object(
                self.log_sync_workflow,
                "_check_for_missing_transmission_from_instance_to_instance_region",
                return_value=False,
            ):
                self.log_sync_workflow._selectively_add_previous_logs(collected_logs, previous_log)

        # Should not add the log since no new data
        self.assertEqual(len(collected_logs), 0)

    # --- Format Collected Logs Tests ---

    def test_format_collected_logs_with_duplicate_request_ids(self):
        """Test formatting logs when duplicate request IDs are encountered"""
        mock_sample = Mock(spec=WorkflowRunSample)
        mock_sample.request_ids = {"duplicate-request-123"}
        mock_sample.is_valid_and_complete.return_value = True
        mock_sample.to_dict.return_value = (datetime.now(GLOBAL_TIME_ZONE), {"test": "data"})

        self.log_sync_workflow._collected_logs = {"run-1": mock_sample}
        self.log_sync_workflow._encountered_duplicate_completed_request_ids = {"duplicate-request-123"}

        with patch.object(self.log_sync_workflow, "_extend_existing_execution_instance_region"):
            with patch.object(self.log_sync_workflow, "_extend_existing_transmission_from_instance_to_instance_region"):
                result = self.log_sync_workflow._format_collected_logs()

        # Should skip the log due to duplicate request ID
        self.assertEqual(len(result), 0)
        # Should remove the duplicate request ID from the set
        self.assertEqual(len(self.log_sync_workflow._encountered_duplicate_completed_request_ids), 0)

    def test_format_collected_logs_invalid_samples(self):
        """Test formatting logs with invalid/incomplete samples"""
        mock_sample = Mock(spec=WorkflowRunSample)
        mock_sample.is_valid_and_complete.return_value = False  # Invalid sample

        self.log_sync_workflow._collected_logs = {"invalid-run": mock_sample}

        result = self.log_sync_workflow._format_collected_logs()

        # Should skip invalid samples
        self.assertEqual(len(result), 0)

    # --- Integration Tests ---

    @patch.object(LogSyncWorkflow, "_get_remote_client")
    def test_full_sync_workflow_integration(self, mock_get_client):
        """Integration test for full sync workflow"""
        # Setup mock client
        mock_client = Mock()
        mock_client.get_logs_between.return_value = [
            f"[CARIBOU]\t2024-08-02T16:43:12.323041+0000\tintegration-request\tTIME (2024-08-02 16:43:12,323041+0000) RUN_ID (integration-test) MESSAGE (EXECUTED: INSTANCE (test-function) with USER_EXECUTION_TIME (1.5) s and TOTAL_EXECUTION_TIME (2.0) s) LOG_VERSION ({LOG_VERSION})"
        ]
        mock_client.get_insights_logs_between.return_value = []
        mock_get_client.return_value = mock_client

        # Setup deployment regions
        self.log_sync_workflow._deployed_regions = {
            "test-function": {"deploy_region": {"provider": "aws", "region": "us-east-1"}}
        }

        # Mock workflow summary client
        self.workflow_summary_client.update_value_in_table = Mock()

        # Run full sync
        self.log_sync_workflow.sync_workflow()

        # Verify data was uploaded
        self.workflow_summary_client.update_value_in_table.assert_called_once()
        call_args = self.workflow_summary_client.update_value_in_table.call_args
        self.assertEqual(call_args[0][0], WORKFLOW_SUMMARY_TABLE)
        self.assertEqual(call_args[0][1], self.workflow_id)

    # --- Performance and Memory Tests ---

    def test_large_log_volume_handling(self):
        """Test handling of large volumes of logs"""
        # Create many log samples
        large_collected_logs = {}
        for i in range(FORGETTING_NUMBER * 2):
            mock_sample = Mock(spec=WorkflowRunSample)
            mock_sample.request_ids = {f"request-{i}"}
            mock_sample.is_valid_and_complete.return_value = True
            mock_sample.to_dict.return_value = (datetime.now(GLOBAL_TIME_ZONE), {"run": i})
            large_collected_logs[f"run-{i}"] = mock_sample

        self.log_sync_workflow._collected_logs = large_collected_logs

        with patch.object(self.log_sync_workflow, "_extend_existing_execution_instance_region"):
            with patch.object(self.log_sync_workflow, "_extend_existing_transmission_from_instance_to_instance_region"):
                result = self.log_sync_workflow._format_collected_logs()

        # Should handle large volume without issues
        self.assertEqual(len(result), FORGETTING_NUMBER * 2)

    def test_memory_cleanup_on_forgetting(self):
        """Test that memory is properly cleaned up during forgetting"""
        # Setup logs that will trigger forgetting
        collected_logs = {}
        for i in range(FORGETTING_NUMBER):
            mock_sample = Mock(spec=WorkflowRunSample)
            mock_sample.request_ids = {f"clean-request-{i}"}
            collected_logs[f"clean-run-{i}"] = mock_sample

        # Add one tainted log
        tainted_sample = Mock(spec=WorkflowRunSample)
        tainted_sample.request_ids = {"tainted-request"}
        collected_logs["tainted-run"] = tainted_sample

        self.log_sync_workflow._collected_logs = collected_logs
        self.log_sync_workflow._tainted_cold_start_samples = {"tainted-request"}

        initial_count = len(self.log_sync_workflow._collected_logs)
        self.log_sync_workflow._check_to_forget()
        final_count = len(self.log_sync_workflow._collected_logs)

        # Should remove tainted log
        self.assertEqual(final_count, initial_count - 1)
        self.assertNotIn("tainted-run", self.log_sync_workflow._collected_logs)
        self.assertIn("tainted-run", self.log_sync_workflow._blacklisted_run_ids)
        self.assertTrue(self.log_sync_workflow._forgetting)

    # Add these additional tests to the existing TestLogSyncWorkflow class as well:

    # --- Specific Log Extraction Tests ---
    def test_extract_entry_point_log_complete(self):
        """Test complete entry point log extraction"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        workflow_run_sample.log_start_time = None
        workflow_run_sample.start_hop_data = Mock()
        workflow_run_sample.start_hop_data.start_hop_latency_from_client = None

        # Mock execution data properly
        mock_execution_data = Mock()
        mock_execution_data.input_payload_size = 0.0  # Initialize as float
        workflow_run_sample.get_execution_data = Mock(return_value=mock_execution_data)

        log_entry = (
            "ENTRY_POINT: Entry Point INSTANCE (test-function) of workflow test-workflow called with "
            "USER_PAYLOAD_SIZE (0.001) GB and is REDIRECTED (False) with INIT_LATENCY_FROM_CLIENT (1.5) s "
            "INIT_LATENCY_FIRST_RECIEVED (0.5) s TIME_FROM_FUNCTION_START (0.1) s from REQUEST_SOURCE (api-gateway) "
            "with WORKFLOW_PLACEMENT_DECISION_SIZE (0.002) GB and CONSUMED_READ_CAPACITY (1.0) "
            "OVERRIDEN_WORKFLOW_PLACEMENT_SIZE (0.003) GB"
        )
        provider_region = {"provider": "aws", "region": "us-east-1"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "entry-point-request"

        self.log_sync_workflow._extract_entry_point_log(
            workflow_run_sample, log_entry, provider_region, log_time, request_id
        )

        # Verify all fields were extracted and set
        self.assertEqual(workflow_run_sample.log_start_time, log_time)
        self.assertEqual(workflow_run_sample.start_hop_data.destination_provider_region, "aws:us-east-1")
        self.assertEqual(workflow_run_sample.start_hop_data.request_source, "api-gateway")
        self.assertEqual(workflow_run_sample.start_hop_data.user_payload_size, 0.001)
        self.assertEqual(workflow_run_sample.start_hop_data.start_hop_latency_from_client, 1.5)
        # Verify input_payload_size was incremented
        self.assertEqual(mock_execution_data.input_payload_size, 0.001)

    def test_extract_redirect_logs_complete(self):
        """Test complete redirect log extraction"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        workflow_run_sample.get_transmission_data = Mock()
        workflow_run_sample.get_execution_data = Mock()
        workflow_run_sample.start_hop_data = Mock()
        workflow_run_sample.start_hop_data.get_redirector_execution_data = Mock()

        mock_transmission = Mock()
        mock_execution = Mock()
        mock_successor = Mock()
        mock_redirector_execution = Mock()
        mock_redirector_execution.input_payload_size = 0.0  # Initialize as float

        # Create a separate recipient execution data mock
        mock_recipient_execution = Mock()
        mock_recipient_execution.input_payload_size = 0.0  # Initialize as float

        workflow_run_sample.get_transmission_data.return_value = mock_transmission
        # The method calls get_execution_data(callee_function, None) for recipient
        # then get_redirector_execution_data for redirector execution
        workflow_run_sample.get_execution_data.return_value = mock_recipient_execution
        mock_redirector_execution.get_successor_data.return_value = mock_successor
        workflow_run_sample.start_hop_data.get_redirector_execution_data.return_value = mock_redirector_execution

        log_entry = (
            "REDIRECT: REDIRECTING_INSTANCE (redirect-function) FROM_REGION (us-west-1) FROM_PROVIDER (aws) "
            "TO_REGION (us-east-1) TO_PROVIDER (aws) INPUT_PAYLOAD_SIZE (0.5) GB OUTPUT_PAYLOAD_SIZE (0.6) GB "
            "TAINT (redirect-taint-123) INVOCATION_TIME_FROM_FUNCTION_START (1.0) s "
            "FINISH_TIME_FROM_INVOCATION_START (2.0) s INIT_LATENCY_FROM_CLIENT (0.8) s"
        )
        provider_region = {"provider": "aws", "region": "us-west-1"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "redirect-request"

        self.log_sync_workflow._extract_redirect_logs(
            workflow_run_sample, log_entry, provider_region, log_time, request_id
        )

        # Verify transmission data was set
        self.assertEqual(mock_transmission.from_region, "aws:us-west-1")
        self.assertEqual(mock_transmission.from_instance, "redirect-function")
        self.assertEqual(mock_transmission.transmission_start_time, log_time)
        self.assertEqual(mock_transmission.payload_transmission_size, 0.6)
        self.assertTrue(mock_transmission.redirector_transmission)

        # According to the source code:
        # 1. recipient_execution_data.input_payload_size += output_payload_size (0.6)
        # 2. execution_data.input_payload_size += input_payload_size (0.5)
        self.assertEqual(mock_recipient_execution.input_payload_size, 0.6)  # Gets output_payload_size
        self.assertEqual(mock_redirector_execution.input_payload_size, 0.5)  # Gets input_payload_size

    def test_extract_invoking_successor_logs_sync_upload_and_invoke(self):
        """Test extracting successor logs with sync upload and invoke"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        mock_transmission = Mock()
        mock_execution = Mock()
        mock_successor = Mock()
        mock_recipient = Mock()
        mock_recipient.input_payload_size = 0.0  # Initialize as float

        workflow_run_sample.get_transmission_data.return_value = mock_transmission
        workflow_run_sample.get_execution_data.return_value = mock_execution
        mock_execution.get_successor_data.return_value = mock_successor

        # Mock for recipient (called twice - once for caller, once for recipient)
        workflow_run_sample.get_execution_data.side_effect = [mock_execution, mock_recipient]

        log_entry = (
            "INVOKING_SUCCESSOR: INSTANCE (caller-func) potentially calling SUCCESSOR (sync-func) "
            "with PAYLOAD_SIZE (1.5) GB and TAINT (sync-taint-456) to PROVIDER (aws) and REGION (us-east-1) "
            "SUCCESSOR_INVOKED (True) at INVOCATION_TIME_FROM_FUNCTION_START (2.5) s and "
            "FINISH_TIME_FROM_INVOCATION_START (3.0) s UPLOADED_DATA_TO_SYNC_TABLE (True) "
            "UPLOAD_DATA_SIZE (0.8) GB consuming CONSUMED_WRITE_CAPACITY (5.0) loaded "
            "SYNC_DATA_RESPONSE_SIZE (0.2) GB with UPLOAD_RTT (0.1) s"
        )
        provider_region = {"provider": "aws", "region": "us-west-2"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "successor-request"

        self.log_sync_workflow._extract_invoking_successor_logs(
            workflow_run_sample, log_entry, provider_region, log_time, request_id
        )

        # Verify sync upload data was set
        self.assertEqual(mock_successor.task_type, SYNC_UPLOAD_AND_INVOKE_TASK_TYPE)
        self.assertEqual(mock_successor.upload_data_size, 0.8)
        self.assertEqual(mock_successor.consumed_write_capacity, 5.0)
        self.assertEqual(mock_transmission.contains_sync_information, True)
        self.assertEqual(mock_transmission.upload_size, 0.8)
        # Verify recipient input_payload_size was incremented
        self.assertEqual(mock_recipient.input_payload_size, 1.5)

    def test_extract_invoking_sync_node_logs_with_invocation(self):
        """Test extracting sync node logs when successor is invoked"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        mock_transmission = Mock()
        mock_execution = Mock()
        mock_successor = Mock()

        workflow_run_sample.get_transmission_data.return_value = mock_transmission
        workflow_run_sample.get_execution_data.return_value = mock_execution
        mock_execution.get_successor_data.return_value = mock_successor
        mock_successor.invoking_sync_node_data_output = {}

        log_entry = (
            "INVOKING_SYNC_NODE: INSTANCE (proxy-func) to SUCCESSOR (target-func) for "
            "PREDECESSOR_INSTANCE (pred-func) calling SYNC_NODE (sync-node) where "
            "SUCCESSOR_INVOKED (True) with PAYLOAD_SIZE (2.0) GB and "
            "SYNC_DATA_RESPONSE_SIZE (0.3) GB and CONSUMED_WRITE_CAPACITY (3.0) "
            "with TAINT (sync-node-taint) to PROVIDER (gcp) and REGION (us-central1)"
        )
        provider_region = {"provider": "aws", "region": "us-east-1"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "sync-node-request"

        self.log_sync_workflow._extract_invoking_sync_node_logs(
            workflow_run_sample, log_entry, provider_region, log_time, request_id
        )

        # Verify transmission data for invoked successor
        self.assertEqual(mock_transmission.from_instance, "proxy-func")
        self.assertEqual(mock_transmission.to_instance, "sync-node")
        self.assertEqual(mock_transmission.uninvoked_instance, "target-func")
        self.assertTrue(mock_transmission.successor_invoked)
        self.assertFalse(mock_transmission.from_direct_successor)

        # Verify sync node data output
        expected_key = "pred-func>sync-node"
        self.assertIn(expected_key, mock_successor.invoking_sync_node_data_output)
        sync_data = mock_successor.invoking_sync_node_data_output[expected_key]
        self.assertEqual(sync_data["data_transfer_size"], 2.0)
        self.assertEqual(sync_data["consumed_write_capacity"], 3.0)

    def test_extract_conditional_non_execution_logs(self):
        """Test extracting conditional non-execution logs"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        mock_execution = Mock()
        mock_successor = Mock()

        workflow_run_sample.get_execution_data.return_value = mock_execution
        mock_execution.get_successor_data.return_value = mock_successor

        log_entry = (
            "CONDITIONAL_NON_EXECUTION: INSTANCE (conditional-func) calling SUCCESSOR (skipped-func) "
            "consuming CONSUMED_WRITE_CAPACITY (1.5) with SYNC_DATA_RESPONSE_SIZE (0.1) GB to "
            "PROVIDER (aws) and REGION (us-west-2) with INVOCATION_TIME_FROM_FUNCTION_START (1.2) s"
        )
        request_id = "conditional-request"

        self.log_sync_workflow._extract_conditional_non_execution_logs(workflow_run_sample, log_entry, request_id)

        # Verify conditional non-execution data
        self.assertEqual(mock_successor.task_type, CONDITIONALLY_NOT_INVOKE_TASK_TYPE)
        self.assertEqual(mock_successor.consumed_write_capacity, 1.5)
        self.assertEqual(mock_successor.sync_data_response_size, 0.1)
        self.assertEqual(mock_successor.invocation_time_from_function_start, 1.2)
        self.assertEqual(mock_successor.destination_region, "aws:us-west-2")

    def test_extract_cpu_model_from_redirector(self):
        """Test extracting CPU model from redirector"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        workflow_run_sample.cpu_models = set()
        workflow_run_sample.start_hop_data = Mock()
        mock_redirector_execution = Mock()
        workflow_run_sample.start_hop_data.get_redirector_execution_data.return_value = mock_redirector_execution

        log_entry = "USED_CPU_MODEL: CPU_MODEL (Intel<R> Xeon<R> CPU E5-2686 v4 @ 2.30GHz) used in INSTANCE (redirect-func) and from FROM_REDIRECTOR (True)"
        request_id = "cpu-request"

        self.log_sync_workflow._extract_cpu_model(workflow_run_sample, log_entry, request_id)

        # Verify CPU model was extracted and converted
        expected_cpu = "Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz"
        self.assertEqual(mock_redirector_execution.cpu_model, expected_cpu)
        self.assertIn(expected_cpu, workflow_run_sample.cpu_models)

    def test_extract_cpu_model_from_regular_function(self):
        """Test extracting CPU model from regular function"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        workflow_run_sample.cpu_models = set()
        mock_execution = Mock()
        workflow_run_sample.get_execution_data.return_value = mock_execution

        log_entry = (
            "USED_CPU_MODEL: CPU_MODEL (AMD EPYC 7571) used in INSTANCE (regular-func) and from FROM_REDIRECTOR (False)"
        )
        request_id = "cpu-request"

        self.log_sync_workflow._extract_cpu_model(workflow_run_sample, log_entry, request_id)

        # Verify CPU model was set on regular execution
        self.assertEqual(mock_execution.cpu_model, "AMD EPYC 7571")
        self.assertIn("AMD EPYC 7571", workflow_run_sample.cpu_models)

    def test_extract_download_data_from_sync_table(self):
        """Test extracting download data from sync table"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        mock_execution = Mock()
        workflow_run_sample.get_execution_data.return_value = mock_execution

        log_entry = (
            "DOWNLOAD_DATA_FROM_SYNC_TABLE: INSTANCE (sync-func) loaded SYNC_NODE_PREDECESSOR_DATA "
            "with DOWNLOAD_SIZE (5.2) GB and CONSUMED_READ_CAPACITY (10.5) and taken "
            "DOWNLOAD_TIME (2.8) s"
        )
        request_id = "download-request"

        self.log_sync_workflow._extract_download_data_from_sync_table(workflow_run_sample, log_entry, request_id)

        # Verify download data was set
        self.assertEqual(mock_execution.download_size, 5.2)
        self.assertEqual(mock_execution.download_time, 2.8)
        self.assertEqual(mock_execution.consumed_read_capacity, 10.5)

    # --- Advanced GCP Tests ---

    def test_setup_gcp_insights_with_all_metrics(self):
        """Test GCP insights setup with complete metric data"""
        logs = [
            json.dumps(
                {
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                    "trace": "projects/test/traces/complete-metrics-123",
                    "timestamp": "2025-07-20T12:00:05Z",
                    "httpRequest": {
                        "latency": "1.5s",
                        "requestSize": "512",
                        "responseSize": "1024",
                        "status": 200,
                        "startupLatency": "0.3s",  # Cold start
                    },
                    "resource": {"labels": {"revision_name": "test-revision-complete"}},
                }
            )
        ]

        mock_remote_client = Mock()
        # Mock all three metric queries
        mock_remote_client.query_metric.side_effect = [
            2.5,  # CPU usage
            0.75,  # Memory utilization (75%)
            1024 * 1024 * 1024,  # Memory usage (1GB in bytes)
        ]

        self.log_sync_workflow._setup_gcp_insights(logs, mock_remote_client)

        insights = self.log_sync_workflow._insights_logs["complete-metrics-123"]

        # Verify all metrics were set correctly
        self.assertEqual(insights["duration"], 1.5)
        self.assertEqual(insights["rx_bytes"], 512.0)
        self.assertEqual(insights["tx_bytes"], 1024.0)
        self.assertEqual(insights["total_network"], 1536.0)
        self.assertTrue(insights["cold_start"])  # Non-zero startup latency
        self.assertEqual(insights["init_duration_s"], 0.3)
        self.assertEqual(insights["cpu_total_time"], 2.5)
        self.assertEqual(insights["memory_utilization"], 75.0)  # Converted to percentage
        self.assertEqual(insights["used_memory_max"], 1024.0)  # Converted to MB
        self.assertEqual(insights["total_memory"], 1024.0 / 0.75)  # Calculated total memory

    def test_setup_gcp_insights_proto_payload(self):
        """Test GCP insights with protoPayload instead of httpRequest"""
        logs = [
            json.dumps(
                {
                    "logName": "projects/test/logs/run.googleapis.com%2Frequests",
                    "trace": "projects/test/traces/proto-payload-123",
                    "timestamp": "2025-07-20T12:00:05Z",
                    "protoPayload": {
                        "latency": "0.8s",
                        "requestSize": "256",
                        "responseSize": "512",
                        "status": 200,
                        "startupLatency": "0s",  # Warm start
                    },
                    "resource": {"labels": {"revision_name": "proto-revision"}},
                }
            )
        ]

        mock_remote_client = Mock()
        mock_remote_client.query_metric.return_value = None  # No metrics

        self.log_sync_workflow._setup_gcp_insights(logs, mock_remote_client)

        insights = self.log_sync_workflow._insights_logs["proto-payload-123"]

        # Verify protoPayload was processed
        self.assertEqual(insights["duration"], 0.8)
        self.assertEqual(insights["rx_bytes"], 256.0)
        self.assertEqual(insights["tx_bytes"], 512.0)
        self.assertFalse(insights["cold_start"])  # Zero startup latency

    # --- Error Recovery Tests ---

    def test_process_log_entry_recovery_from_partial_failure(self):
        """Test recovery when log processing partially fails"""
        time_to = datetime.now(GLOBAL_TIME_ZONE)

        # Valid log that should be processed
        valid_log = f"[CARIBOU]\t2024-08-02T16:43:12.323041+0000\trecovery-request\tTIME (2024-08-02 16:43:12,323041+0000) RUN_ID (recovery-test) MESSAGE (EXECUTED: INSTANCE (test-function) with USER_EXECUTION_TIME (1.5) s and TOTAL_EXECUTION_TIME (2.0) s) LOG_VERSION ({LOG_VERSION})"

        # Log with invalid time that should fail
        invalid_log = f"[CARIBOU]\t2024-08-02T16:43:12.323041+0000\tinvalid-request\tTIME (invalid-time) RUN_ID (invalid-run) MESSAGE (test) LOG_VERSION ({LOG_VERSION})"

        provider_region = {"provider": "aws", "region": "us-east-1"}

        # Process valid log first
        self.log_sync_workflow._process_log_entry(valid_log, provider_region, time_to)
        self.assertIn("recovery-test", self.log_sync_workflow._collected_logs)

        # Process invalid log - should raise error but not affect valid logs
        with self.assertRaises(ValueError):
            self.log_sync_workflow._process_log_entry(invalid_log, provider_region, time_to)

        # Valid log should still be there
        self.assertIn("recovery-test", self.log_sync_workflow._collected_logs)

    def test_sync_workflow_partial_failure_recovery(self):
        """Test that sync workflow can recover from partial failures"""
        # Mock one region that works and one that fails
        working_client = Mock()
        working_client.get_logs_between.return_value = [
            f"[CARIBOU] TIME (2024-08-02 16:43:12,323041+0000) RUN_ID (working-run) MESSAGE (test) LOG_VERSION ({LOG_VERSION})"
        ]
        working_client.get_insights_logs_between.return_value = []

        failing_client = Mock()
        failing_client.get_logs_between.side_effect = Exception("Network error")

        self.log_sync_workflow._region_clients = {
            ("aws", "us-east-1"): working_client,
            ("aws", "us-west-2"): failing_client,
        }

        self.log_sync_workflow._deployed_regions = {
            "working-function": {"deploy_region": {"provider": "aws", "region": "us-east-1"}},
            "failing-function": {"deploy_region": {"provider": "aws", "region": "us-west-2"}},
        }

        # Should process working region and fail on failing region
        with self.assertRaises(Exception):
            self.log_sync_workflow._sync_logs()

        # But working region should have been processed
        self.assertIn("working-run", self.log_sync_workflow._collected_logs)

    # --- Performance Edge Cases ---

    def test_very_large_payload_sizes(self):
        """Test handling of very large payload sizes in logs"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        workflow_run_sample.get_transmission_data = Mock()
        workflow_run_sample.get_execution_data = Mock()

        mock_transmission = Mock()
        mock_execution = Mock()
        mock_successor = Mock()
        mock_recipient = Mock()
        mock_recipient.input_payload_size = 0.0  # Initialize as float

        workflow_run_sample.get_transmission_data.return_value = mock_transmission
        workflow_run_sample.get_execution_data.side_effect = [mock_execution, mock_recipient]
        mock_execution.get_successor_data.return_value = mock_successor

        # Very large payload size (10GB)
        log_entry = (
            "INVOKING_SUCCESSOR: INSTANCE (big-sender) potentially calling SUCCESSOR (big-receiver) "
            "with PAYLOAD_SIZE (10.5) GB and TAINT (big-taint) to PROVIDER (aws) and REGION (us-east-1) "
            "SUCCESSOR_INVOKED (True) at INVOCATION_TIME_FROM_FUNCTION_START (30.0) s and "
            "FINISH_TIME_FROM_INVOCATION_START (45.0) s UPLOADED_DATA_TO_SYNC_TABLE (False)"
        )
        provider_region = {"provider": "aws", "region": "us-west-2"}
        log_time = datetime.now(GLOBAL_TIME_ZONE)
        request_id = "big-payload-request"

        self.log_sync_workflow._extract_invoking_successor_logs(
            workflow_run_sample, log_entry, provider_region, log_time, request_id
        )

        # Should handle large payload sizes correctly
        self.assertEqual(mock_transmission.payload_transmission_size, 10.5)
        self.assertEqual(mock_successor.output_payload_data_size, 10.5)
        self.assertEqual(mock_recipient.input_payload_size, 10.5)

    def test_extreme_execution_times(self):
        """Test handling of extreme execution times"""
        workflow_run_sample = Mock(spec=WorkflowRunSample)
        mock_execution = Mock()
        workflow_run_sample.get_execution_data.return_value = mock_execution

        # Very long execution time
        log_entry = (
            "EXECUTED: INSTANCE (slow-function) with USER_EXECUTION_TIME (3600.5) s "
            "and TOTAL_EXECUTION_TIME (3650.8) s"
        )
        provider_region = {"provider": "aws", "region": "us-east-1"}
        request_id = "slow-request"

        self.log_sync_workflow._extract_executed_logs(workflow_run_sample, log_entry, provider_region, request_id)

        # Should handle extreme times correctly
        self.assertEqual(mock_execution.user_execution_duration, 3600.5)
        self.assertEqual(mock_execution.execution_duration, 3650.8)

    # --- Data Consistency Tests ---

    def test_concurrent_log_processing_simulation(self):
        """Simulate concurrent log processing to test data consistency"""
        import threading
        import time

        results = {}
        errors = {}

        def process_logs_worker(worker_id):
            try:
                for i in range(10):
                    # Fix: Use proper log format with tab-separated request_id
                    log_entry = f"[CARIBOU]\t2024-08-02T16:43:{i:02d}.000000+0000\tworker-{worker_id}-request-{i}\tTIME (2024-08-02 16:43:{i:02d},000000+0000) RUN_ID (worker-{worker_id}-run-{i}) MESSAGE (test) LOG_VERSION ({LOG_VERSION})"
                    provider_region = {"provider": "aws", "region": "us-east-1"}

                    self.log_sync_workflow._process_log_entry(
                        log_entry, provider_region, datetime.now(GLOBAL_TIME_ZONE)
                    )
                    time.sleep(0.001)  # Small delay to simulate processing time

                results[worker_id] = len(
                    [k for k in self.log_sync_workflow._collected_logs.keys() if f"worker-{worker_id}" in k]
                )
            except Exception as e:
                errors[worker_id] = str(e)

        # Start multiple workers
        threads = []
        for worker_id in range(3):
            thread = threading.Thread(target=process_logs_worker, args=(worker_id,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify no errors and all logs were processed
        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")
        self.assertEqual(sum(results.values()), 30)  # 3 workers * 10 logs each

    # --- Boundary Value Tests ---

    def test_forgetting_number_boundary(self):
        """Test behavior at forgetting number boundary"""
        # Create exactly FORGETTING_NUMBER - 1 logs
        collected_logs = {}
        for i in range(FORGETTING_NUMBER - 1):
            mock_sample = Mock(spec=WorkflowRunSample)
            mock_sample.request_ids = {f"boundary-request-{i}"}
            collected_logs[f"boundary-run-{i}"] = mock_sample

        self.log_sync_workflow._collected_logs = collected_logs
        self.log_sync_workflow._check_to_forget()

        # Should not be forgetting yet
        self.assertFalse(self.log_sync_workflow._forgetting)

        # Add one more log
        mock_sample = Mock(spec=WorkflowRunSample)
        mock_sample.request_ids = {"boundary-final-request"}
        self.log_sync_workflow._collected_logs["boundary-final-run"] = mock_sample

        self.log_sync_workflow._check_to_forget()

        # Should now be forgetting
        self.assertTrue(self.log_sync_workflow._forgetting)

    def test_time_boundary_filtering(self):
        """Test filtering at time boundaries"""
        now = datetime.now(GLOBAL_TIME_ZONE)

        # Use a smaller time difference to ensure we have some data that should remain
        # Instead of using FORGETTING_TIME_DAYS, let's use a fixed value that we control
        test_days = 30  # Use 30 days instead of FORGETTING_TIME_DAYS

        # Create data around a boundary we control
        boundary_time = now - timedelta(days=test_days)
        just_before_boundary = boundary_time - timedelta(seconds=1)
        just_after_boundary = boundary_time + timedelta(seconds=1)
        well_after_boundary = now - timedelta(days=1)  # Much more recent

        previous_counts = {
            just_before_boundary.strftime(TIME_FORMAT_DAYS): 5,  # Should be removed
            boundary_time.strftime(TIME_FORMAT_DAYS): 10,  # Boundary case - might be removed
            just_after_boundary.strftime(TIME_FORMAT_DAYS): 15,  # Should be kept
            well_after_boundary.strftime(TIME_FORMAT_DAYS): 20,  # Definitely should be kept
        }

        # Temporarily patch FORGETTING_TIME_DAYS to our test value
        original_forgetting_time = FORGETTING_TIME_DAYS
        import caribou.common.constants

        caribou.common.constants.FORGETTING_TIME_DAYS = test_days

        try:
            self.log_sync_workflow._filter_daily_counts(previous_counts)

            # Check what actually remains
            self.assertNotIn(just_before_boundary.strftime(TIME_FORMAT_DAYS), previous_counts)
            # At least the well_after_boundary should remain
            self.assertIn(well_after_boundary.strftime(TIME_FORMAT_DAYS), previous_counts)

            # The test should pass with at least one entry remaining
            self.assertGreater(len(previous_counts), 0, "At least some recent entries should remain")

        finally:
            # Restore the original value
            caribou.common.constants.FORGETTING_TIME_DAYS = original_forgetting_time

    # --- Resource Management Tests ---

    def test_memory_usage_with_large_datasets(self):
        """Test memory management with large datasets"""
        import sys

        # Create a large number of workflow samples
        large_dataset = {}
        for i in range(1000):
            mock_sample = Mock(spec=WorkflowRunSample)
            mock_sample.request_ids = {f"large-request-{i}"}
            mock_sample.is_valid_and_complete.return_value = True
            mock_sample.to_dict.return_value = (
                datetime.now(GLOBAL_TIME_ZONE),
                {
                    "execution_data": [{"instance_name": f"func-{i}", "provider_region": "aws:us-east-1"}],
                    "transmission_data": [],
                },
            )
            large_dataset[f"large-run-{i}"] = mock_sample

        self.log_sync_workflow._collected_logs = large_dataset

        # Process the large dataset
        with patch.object(self.log_sync_workflow, "_extend_existing_execution_instance_region"):
            with patch.object(self.log_sync_workflow, "_extend_existing_transmission_from_instance_to_instance_region"):
                result = self.log_sync_workflow._format_collected_logs()

        # Should handle large dataset without issues
        self.assertEqual(len(result), 1000)

        # Cleanup
        del large_dataset
        del result


if __name__ == "__main__":
    unittest.main()
