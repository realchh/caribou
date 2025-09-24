import os
import unittest
from unittest.mock import patch, MagicMock

from caribou.deployment.client.remote_cli.remote_cli_handler import caribou_cli
from caribou.deployment.client import __version__ as CARIBOU_VERSION


class TestRemoteCLIHandler(unittest.TestCase):
    def setUp(self):
        self.event = {
            "action": "",
            "workflow_id": "test_workflow_id",
            "collector": "provider",
            "deployment_metrics_calculator_type": "simple",
            "type": "check_workflow",
            "event": {
                "workflow_id": "test_workflow_id",
                "deployment_metrics_calculator_type": "simple",
                "solve_hours": ["1", "2"],
                "leftover_tokens": 10,
            },
        }
        self.context = {}

    def test_caribou_cli_no_action(self):
        self.event.pop("action")
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No action specified"})

    def test_caribou_cli_handle_run_deployment_migrator(self):
        self.event["action"] = "run_deployment_migrator"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.DeploymentMigrator") as MockMigrator:
            mock_instance = MockMigrator.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.check.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Deployment migrator started"})

    def test_caribou_cli_handle_remove_workflow(self):
        self.event["action"] = "remove"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.Client") as MockClient:
            mock_instance = MockClient.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.remove.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Workflow test_workflow_id removal started"})

    def test_caribou_cli_handle_manage_deployments(self):
        self.event["action"] = "manage_deployments"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.DeploymentManager") as MockManager:
            mock_instance = MockManager.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.check.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Deployment check started, using simple calculator"})

    def test_caribou_cli_handle_log_sync(self):
        self.event["action"] = "log_sync"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.LogSyncer") as MockSyncer:
            mock_instance = MockSyncer.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.sync.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Log sync started"})

    def test_caribou_cli_handle_data_collect(self):
        self.event["action"] = "data_collect"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.ProviderCollector") as MockCollector:
            mock_instance = MockCollector.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.run.assert_called_once()
            self.assertEqual(
                response, {"status": 200, "scheduled_collector": "provider", "workflow_id": "test_workflow_id"}
            )

    def test_caribou_cli_handle_list_caribou_version(self):
        self.event["action"] = "version"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 200, "version": CARIBOU_VERSION})

    def test_caribou_cli_handle_list_workflows(self):
        self.event["action"] = "list"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.Client") as MockClient:
            mock_instance = MockClient.return_value
            mock_instance.list_workflows.return_value = ["workflow1", "workflow2"]
            response = caribou_cli(self.event, self.context)
            mock_instance.list_workflows.assert_called_once()
            self.assertEqual(response, {"status": 200, "workflows": ["workflow1", "workflow2"]})

    def test_caribou_cli_handle_run(self):
        self.event["action"] = "run"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.Client") as MockClient:
            mock_instance = MockClient.return_value
            mock_instance.run.return_value = "run_id"
            response = caribou_cli(self.event, self.context)
            mock_instance.run.assert_called_once()
            self.assertEqual(response, {"status": 200, "run_id": "run_id"})

    def test_caribou_cli_handle_internal_action(self):
        self.event["action"] = "internal_action"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.handle_internal_action") as MockHandler:
            caribou_cli(self.event, self.context)
            MockHandler.assert_called_once_with(self.event)

    def test_handle_internal_action_no_type(self):
        self.event["action"] = "internal_action"
        self.event.pop("type")
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No special_action specified"})

    def test_handle_internal_action_unknown_type(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "unknown_type"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "Unknown special action"})

    # Test handle_remove_workflow with missing workflow_id
    def test_handle_remove_workflow_missing_workflow_id(self):
        self.event["action"] = "remove"
        self.event.pop("workflow_id")
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No workflow_id specified"})

    # Test handle_manage_deployments with invalid deployment_metrics_calculator_type
    def test_handle_manage_deployments_invalid_calculator_type(self):
        self.event["action"] = "manage_deployments"
        self.event["deployment_metrics_calculator_type"] = "invalid_type"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(
            response,
            {
                "status": 400,
                "message": "Invalid deployment_metrics_calculator_type specified. Allowed values are 'simple', 'go'",
            },
        )

    # Test handle_manage_deployments with GCP provider and go calculator
    @patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"})
    def test_handle_manage_deployments_gcp_with_go_calculator(self):
        self.event["action"] = "manage_deployments"
        self.event["deployment_metrics_calculator_type"] = "go"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(
            response, {"status": 400, "message": "Go deployment metrics calculator is not supported for GCP provider"}
        )

    # Test handle_manage_deployments with go calculator (valid case)
    def test_handle_manage_deployments_with_go_calculator(self):
        self.event["action"] = "manage_deployments"
        self.event["deployment_metrics_calculator_type"] = "go"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.DeploymentManager") as MockManager:
            mock_instance = MockManager.return_value
            response = caribou_cli(self.event, self.context)
            MockManager.assert_called_once_with("go", deployed_remotely=True)
            mock_instance.check.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Deployment check started, using go calculator"})

    # Test handle_data_collect with missing collector
    def test_handle_data_collect_missing_collector(self):
        self.event["action"] = "data_collect"
        self.event.pop("collector")
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No collector specified"})

    # Test handle_data_collect with invalid collector
    def test_handle_data_collect_invalid_collector(self):
        self.event["action"] = "data_collect"
        self.event["collector"] = "invalid_collector"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(
            response,
            {
                "status": 400,
                "message": "Invalid collector specified, Allowed values are provider, carbon, performance, workflow, all",
            },
        )

    # Test handle_data_collect with carbon collector
    def test_handle_data_collect_carbon_collector(self):
        self.event["action"] = "data_collect"
        self.event["collector"] = "carbon"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.CarbonCollector") as MockCollector:
            mock_instance = MockCollector.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.run.assert_called_once()
            self.assertEqual(
                response, {"status": 200, "scheduled_collector": "carbon", "workflow_id": "test_workflow_id"}
            )

    # Test handle_data_collect with performance collector
    def test_handle_data_collect_performance_collector(self):
        self.event["action"] = "data_collect"
        self.event["collector"] = "performance"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.PerformanceCollector") as MockCollector:
            mock_instance = MockCollector.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.run.assert_called_once()
            self.assertEqual(
                response, {"status": 200, "scheduled_collector": "performance", "workflow_id": "test_workflow_id"}
            )

    # Test handle_data_collect with workflow collector
    def test_handle_data_collect_workflow_collector(self):
        self.event["action"] = "data_collect"
        self.event["collector"] = "workflow"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.WorkflowCollector") as MockCollector:
            mock_instance = MockCollector.return_value
            response = caribou_cli(self.event, self.context)
            mock_instance.run_on_workflow.assert_called_once_with("test_workflow_id")
            self.assertEqual(
                response, {"status": 200, "scheduled_collector": "workflow", "workflow_id": "test_workflow_id"}
            )

    # Test handle_data_collect with workflow collector but missing workflow_id
    def test_handle_data_collect_workflow_collector_missing_workflow_id(self):
        self.event["action"] = "data_collect"
        self.event["collector"] = "workflow"
        self.event.pop("workflow_id")
        response = caribou_cli(self.event, self.context)
        self.assertEqual(
            response, {"status": 400, "message": "Workflow_id must be provided for the workflow collector."}
        )

    # Test handle_data_collect with "all" collector
    def test_handle_data_collect_all_collectors(self):
        self.event["action"] = "data_collect"
        self.event["collector"] = "all"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.ProviderCollector") as MockProvider, patch(
            "caribou.deployment.client.remote_cli.remote_cli_handler.CarbonCollector"
        ) as MockCarbon, patch(
            "caribou.deployment.client.remote_cli.remote_cli_handler.PerformanceCollector"
        ) as MockPerformance:
            mock_provider = MockProvider.return_value
            mock_carbon = MockCarbon.return_value
            mock_performance = MockPerformance.return_value

            response = caribou_cli(self.event, self.context)

            mock_provider.run.assert_called_once()
            mock_carbon.run.assert_called_once()
            mock_performance.run.assert_called_once()

            self.assertEqual(response, {"status": 200, "scheduled_collector": "all", "workflow_id": "test_workflow_id"})

    # Test handle_run with argument
    def test_handle_run_with_argument(self):
        self.event["action"] = "run"
        self.event["argument"] = "test_argument"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.Client") as MockClient:
            mock_instance = MockClient.return_value
            mock_instance.run.return_value = "run_id_with_arg"
            response = caribou_cli(self.event, self.context)
            mock_instance.run.assert_called_once_with("test_argument")
            self.assertEqual(response, {"status": 200, "run_id": "run_id_with_arg"})

    # Test handle_run without workflow_id
    def test_handle_run_without_workflow_id(self):
        self.event["action"] = "run"
        self.event.pop("workflow_id")
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 200, "run_id": None})

    # Test unknown action
    def test_handle_unknown_action(self):
        self.event["action"] = "unknown_action"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "Unknown action"})

    # Test internal action - check_workflow
    def test_internal_action_check_workflow(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "check_workflow"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.DeploymentManager") as MockManager:
            mock_instance = MockManager.return_value
            response = caribou_cli(self.event, self.context)
            MockManager.assert_called_once_with("simple", deployed_remotely=True)
            mock_instance.check_workflow.assert_called_once_with("test_workflow_id")
            self.assertEqual(response, {"status": 200, "message": "Workflow test_workflow_id checked"})

    # Test internal action - check_workflow missing workflow_id
    def test_internal_action_check_workflow_missing_workflow_id(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "check_workflow"
        self.event["event"]["workflow_id"] = None
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No workflow_id specified"})

    # Test internal action - check_workflow missing deployment_metrics_calculator_type
    def test_internal_action_check_workflow_missing_calculator_type(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "check_workflow"
        self.event["event"]["deployment_metrics_calculator_type"] = None
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No deployment_metrics_calculator_type specified"})

    # Test internal action - run_deployment_algorithm
    def test_internal_action_run_deployment_algorithm(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "run_deployment_algorithm"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.DeploymentManager") as MockManager:
            mock_instance = MockManager.return_value
            response = caribou_cli(self.event, self.context)
            MockManager.assert_called_once_with("simple", deployed_remotely=True)
            mock_instance.run_deployment_algorithm.assert_called_once_with("test_workflow_id", ["1", "2"], 10)
            self.assertEqual(response, {"status": 200, "message": "Deployment algorithm performed on test_workflow_id"})

    # Test internal action - run_deployment_algorithm missing parameters
    def test_internal_action_run_deployment_algorithm_missing_params(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "run_deployment_algorithm"
        self.event["event"]["solve_hours"] = None
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No solve_hours specified"})

    def test_internal_action_run_deployment_algorithm_missing_leftover_tokens(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "run_deployment_algorithm"
        self.event["event"]["leftover_tokens"] = None
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 400, "message": "No leftover_tokens specified"})

    # Test internal action - re_deploy_workflow
    def test_internal_action_re_deploy_workflow(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "re_deploy_workflow"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.ReDeploymentServer") as MockServer:
            mock_instance = MockServer.return_value
            response = caribou_cli(self.event, self.context)
            MockServer.assert_called_once_with("test_workflow_id")
            mock_instance.run.assert_called_once()
            self.assertEqual(response, {"status": 200, "message": "Workflow test_workflow_id re-deployed"})

    # Test internal action - sync_workflow
    def test_internal_action_sync_workflow(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "sync_workflow"
        with patch("caribou.deployment.client.remote_cli.remote_cli_handler.LogSyncer") as MockSyncer:
            mock_instance = MockSyncer.return_value
            response = caribou_cli(self.event, self.context)
            MockSyncer.assert_called_once_with(deployed_remotely=True)
            mock_instance.sync_workflow.assert_called_once_with("test_workflow_id")
            self.assertEqual(response, {"status": 200, "message": "Workflow test_workflow_id synced"})

    # Test GCP request handling with Flask request
    @patch.dict(os.environ, {"K_SERVICE": "test_service"})
    def test_gcp_request_handling_flask(self):
        import flask

        mock_flask_request = MagicMock(spec=flask.Request)
        mock_flask_request.get_json.return_value = {"action": "version"}

        response = caribou_cli(mock_flask_request, self.context)
        mock_flask_request.get_json.assert_called_once()
        self.assertEqual(response, {"status": 200, "version": CARIBOU_VERSION})

    # Test GCP request handling with invalid request format
    @patch.dict(os.environ, {"K_SERVICE": "test_service"})
    def test_gcp_request_handling_invalid_format(self):
        # Pass a regular dict instead of Flask request to trigger AttributeError
        invalid_request = {"action": "version"}
        response = caribou_cli(invalid_request, self.context)
        self.assertEqual(response, {"status": 400, "message": "Invalid GCP request format"})

    # Test AWS Lambda request handling (default case)
    def test_aws_lambda_request_handling(self):
        # Ensure K_SERVICE is not in environment
        if "K_SERVICE" in os.environ:
            del os.environ["K_SERVICE"]

        self.event["action"] = "version"
        response = caribou_cli(self.event, self.context)
        self.assertEqual(response, {"status": 200, "version": CARIBOU_VERSION})

    # Test internal action with missing event
    def test_internal_action_missing_event(self):
        self.event["action"] = "internal_action"
        self.event["type"] = "check_workflow"
        self.event["event"] = None
        with self.assertRaises(AttributeError):
            caribou_cli(self.event, self.context)


if __name__ == "__main__":
    unittest.main()
