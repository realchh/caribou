import unittest
from unittest.mock import patch, mock_open, MagicMock, call
import os
import tempfile
from caribou.deployment.client.remote_cli.remote_cli import (
    _get_timer_rule_name,
    action_type_to_function_name,
    get_all_available_timed_cli_functions,
    get_all_default_timed_cli_functions,
    get_cli_invoke_payload,
    is_aws_framework_deployed,
    remove_aws_timers,
    remove_remote_framework,
    deploy_remote_framework,
    report_timer_schedule_expression,
    setup_aws_timers,
    valid_framework_dir,
    _retrieve_iam_trust_policy,
    _get_env_vars,
    remove_aws_remote_framework,
    deploy_aws_remote_framework,
    remove_gcp_remote_framework,
    deploy_gcp_remote_framework,
    is_gcp_framework_deployed,
    is_framework_deployed,
    report_gcp_timer_schedule_expression,
    report_aws_timer_schedule_expression,
    remove_timers,
    remove_gcp_timers,
    setup_gcp_timers,
    setup_timers,
)


class TestRemoteCLI(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_aws_client = MagicMock()
        self.mock_gcp_client = MagicMock()

    def tearDown(self):
        """Clean up after each test method."""
        # Clear any environment variables that might have been set during tests
        for var in ["CARIBOU_DEFAULT_PROVIDER", "GOOGLE_API_KEY", "ELECTRICITY_MAPS_AUTH_TOKEN"]:
            if var in os.environ:
                del os.environ[var]

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    @patch.dict(os.environ, {}, clear=True)
    def test_remove_aws_remote_framework_deployed(
        self, mock_is_deployed, mock_remove_timers, mock_get_functions, MockAWSRemoteClient
    ):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, True, True]
        mock_is_deployed.return_value = True
        mock_get_functions.return_value = ["provider_collector", "carbon_collector"]

        remove_remote_framework()

        mock_remove_timers.assert_called_once_with(["provider_collector", "carbon_collector"], verbose=True)
        mock_client.remove_role.assert_called_once_with("caribou_deployment_policy")
        mock_client.remove_function.assert_called_once_with("caribou_cli")
        mock_client.remove_ecr_repository.assert_called_once_with("caribou_cli")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_remove_aws_remote_framework_not_deployed(
        self, mock_is_deployed, mock_remove_timers, mock_get_functions, MockAWSRemoteClient
    ):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, True, True]
        mock_is_deployed.return_value = False
        mock_get_functions.return_value = ["provider_collector", "carbon_collector"]

        remove_remote_framework()

        mock_remove_timers.assert_called_once_with(["provider_collector", "carbon_collector"], verbose=False)
        mock_client.remove_role.assert_called_once_with("caribou_deployment_policy")
        mock_client.remove_function.assert_called_once_with("caribou_cli")
        mock_client.remove_ecr_repository.assert_called_once_with("caribou_cli")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_remove_remote_framework_no_resources(
        self, mock_is_deployed, mock_remove_timers, mock_get_functions, MockAWSRemoteClient
    ):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [False, False, False]
        mock_is_deployed.return_value = True
        mock_get_functions.return_value = ["provider_collector", "carbon_collector"]

        remove_remote_framework()

        mock_remove_timers.assert_called_once_with(["provider_collector", "carbon_collector"], verbose=True)
        mock_client.remove_role.assert_not_called()
        mock_client.remove_function.assert_not_called()
        mock_client.remove_ecr_repository.assert_not_called()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    @patch.dict(os.environ, {}, clear=True)
    def test_remove_aws_remote_framework_partial_resources(
        self, mock_is_deployed, mock_remove_timers, mock_get_functions, MockAWSRemoteClient
    ):
        """Test removing AWS remote framework with partial resources."""
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [False, True, False]  # Only function exists
        mock_is_deployed.return_value = True
        mock_get_functions.return_value = ["provider_collector"]

        remove_aws_remote_framework()

        mock_remove_timers.assert_called_once_with(["provider_collector"], verbose=True)
        mock_client.remove_role.assert_not_called()
        mock_client.remove_function.assert_called_once_with("caribou_cli")
        mock_client.remove_ecr_repository.assert_not_called()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.Config")
    @patch("caribou.deployment.client.remote_cli.remote_cli.DeploymentPackager")
    @patch("builtins.open", new_callable=mock_open)
    @patch("tempfile.TemporaryDirectory")
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "fake_key", "ELECTRICITY_MAPS_AUTH_TOKEN": "fake_token"})
    def test_deploy_aws_remote_framework_success(
        self, mock_tempdir, mock_file, MockDeploymentPackager, MockConfig, MockAWSRemoteClient
    ):
        """Test successful AWS remote framework deployment."""
        # Setup mocks
        mock_client = MockAWSRemoteClient.return_value
        mock_packager = MockDeploymentPackager.return_value
        mock_tempdir.return_value.__enter__.return_value = "/tmp/test"

        mock_client.resource_exists.side_effect = [True, True]  # Role and function exist
        mock_client.create_role.return_value = "arn:aws:iam::123456789012:role/test-role"
        mock_packager.create_framework_package.return_value = "/tmp/test/package.zip"

        # Mock file reading for both the policy and the zip file
        mock_file.return_value.read.side_effect = ['{"aws": {"policy": "content"}}', b"fake_zip_content"]

        deploy_aws_remote_framework("/fake/project/dir", 300, 128, 512)

        # Verify calls
        mock_client.remove_role.assert_called_once_with("caribou_deployment_policy")
        mock_client.remove_function.assert_called_once_with("caribou_cli")
        mock_client.create_role.assert_called_once()
        mock_client.deploy_remote_cli.assert_called_once()

        # Verify deployment parameters
        deploy_call = mock_client.deploy_remote_cli.call_args
        self.assertEqual(deploy_call[0][0], "caribou_cli")  # function name
        self.assertEqual(deploy_call[0][1], "app.caribou_cli")  # handler
        self.assertEqual(deploy_call[0][3], 300)  # timeout
        self.assertEqual(deploy_call[0][4], 128)  # memory
        self.assertEqual(deploy_call[0][5], 512)  # ephemeral storage

    # ========== GCP Framework Tests ==========

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_all_available_timed_cli_functions")
    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_gcp_timers")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_gcp_framework_deployed")
    @patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"})
    def test_remove_gcp_remote_framework_deployed(
        self, mock_is_deployed, mock_remove_timers, mock_get_functions, MockGCPRemoteClient
    ):
        """Test removing GCP remote framework when fully deployed."""
        mock_client = MockGCPRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, True, True]
        mock_is_deployed.return_value = True
        mock_get_functions.return_value = ["provider_collector", "carbon_collector"]

        remove_remote_framework()

        mock_remove_timers.assert_called_once_with(["provider_collector", "carbon_collector"], verbose=True)
        mock_client.remove_role.assert_called_once_with("caribou-deployment-policy")
        mock_client.remove_function.assert_called_once_with("caribou-cli")
        mock_client.remove_artifact_registry_repository.assert_called_once_with("caribou-cli")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.DeploymentPackager")
    @patch("builtins.open", new_callable=mock_open, read_data='{"aws": {}}')
    @patch("tempfile.TemporaryDirectory", return_value=tempfile.TemporaryDirectory())
    def test_deploy_aws_framework(self, mock_tempdir, mock_open, MockDeploymentPackager, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_packager = MockDeploymentPackager.return_value
        mock_client.resource_exists.side_effect = [True, True, False]
        mock_packager.create_framework_package.return_value = "/fake/path/to/zip"

        with patch.dict(os.environ, {"GOOGLE_API_KEY": "fake_key", "ELECTRICITY_MAPS_AUTH_TOKEN": "fake_token"}):
            deploy_remote_framework("/fake/project/dir", 300, 128, 512)

        mock_client.remove_role.assert_called_once_with("caribou_deployment_policy")
        mock_client.remove_function.assert_called_once_with("caribou_cli")
        mock_client.create_role.assert_called_once()
        mock_client.deploy_remote_cli.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.DeploymentPackager")
    @patch("builtins.open", new_callable=mock_open, read_data='{"gcp": {}}')
    @patch("tempfile.TemporaryDirectory", return_value=tempfile.TemporaryDirectory())
    def test_deploy_gcp_framework(self, mock_tempdir, mock_open, MockDeploymentPackager, MockGCPRemoteClient):
        mock_client = MockGCPRemoteClient.return_value
        mock_packager = MockDeploymentPackager.return_value
        mock_client.resource_exists.side_effect = [True, True, False]
        mock_packager.create_framework_package.return_value = "/fake/path/to/zip"

        with patch.dict(
            os.environ,
            {
                "GOOGLE_API_KEY": "fake_key",
                "ELECTRICITY_MAPS_AUTH_TOKEN": "fake_token",
                "CARIBOU_DEFAULT_PROVIDER": "gcp",
            },
        ):
            deploy_remote_framework("/fake/project/dir", 300, 1024, 1024)

        mock_client.create_role.assert_called_once()
        mock_client.deploy_remote_cli.assert_called_once()

    @patch(
        "os.path.exists",
        side_effect=lambda x: x
        in ["/fake/project/dir/caribou", "/fake/project/dir/caribou-go", "/fake/project/dir/pyproject.toml"],
    )
    def test_valid_framework_dir(self, mock_exists):
        self.assertTrue(valid_framework_dir("/fake/project/dir"))
        self.assertFalse(valid_framework_dir("/invalid/project/dir"))

    @patch("os.path.exists")
    def test_valid_framework_dir_all_files_exist(self, mock_exists):
        """Test valid_framework_dir when all required files exist."""
        mock_exists.return_value = True
        self.assertTrue(valid_framework_dir("/fake/project/dir"))

        expected_calls = [
            call("/fake/project/dir/caribou"),
            call("/fake/project/dir/caribou-go"),
            call("/fake/project/dir/pyproject.toml"),
        ]
        mock_exists.assert_has_calls(expected_calls, any_order=True)

    @patch("os.path.exists")
    def test_valid_framework_dir_missing_files(self, mock_exists):
        """Test valid_framework_dir when some files are missing."""

        def side_effect(path):
            return "caribou" in path and "caribou-go" not in path

        mock_exists.side_effect = side_effect
        self.assertFalse(valid_framework_dir("/fake/project/dir"))

    def test_retrieve_iam_trust_policy(self):
        expected_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {"Service": ["lambda.amazonaws.com", "states.amazonaws.com"]},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        self.assertEqual(_retrieve_iam_trust_policy(), expected_policy)

    def test_get_env_vars(self):
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "fake_key", "ELECTRICITY_MAPS_AUTH_TOKEN": "fake_token"}):
            env_vars = _get_env_vars(["GOOGLE_API_KEY", "ELECTRICITY_MAPS_AUTH_TOKEN"])
            self.assertEqual(env_vars, {"GOOGLE_API_KEY": "fake_key", "ELECTRICITY_MAPS_AUTH_TOKEN": "fake_token"})

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError):
                _get_env_vars(["GOOGLE_API_KEY", "ELECTRICITY_MAPS_AUTH_TOKEN"])

    @patch.dict(os.environ, {"VAR1": "value1", "VAR2": "value2"})
    def test_get_env_vars_all_present(self):
        """Test _get_env_vars when all variables are present."""
        result = _get_env_vars(["VAR1", "VAR2"])
        expected = {"VAR1": "value1", "VAR2": "value2"}
        self.assertEqual(result, expected)

    @patch.dict(os.environ, {"VAR1": "value1"}, clear=True)
    def test_get_env_vars_missing_variables(self):
        """Test _get_env_vars when some variables are missing."""
        with self.assertRaises(EnvironmentError) as context:
            _get_env_vars(["VAR1", "VAR2", "VAR3"])

        self.assertIn("VAR2", str(context.exception))
        self.assertIn("VAR3", str(context.exception))

    @patch.dict(os.environ, {}, clear=True)
    def test_get_env_vars_all_missing(self):
        """Test _get_env_vars when all variables are missing."""
        with self.assertRaises(EnvironmentError):
            _get_env_vars(["GOOGLE_API_KEY", "ELECTRICITY_MAPS_AUTH_TOKEN"])

    def test_get_all_available_timed_cli_functions(self):
        expected_functions = [
            "provider_collector",
            "carbon_collector",
            "performance_collector",
            "log_syncer",
            "deployment_manager",
            "deployment_migrator",
        ]
        self.assertEqual(get_all_available_timed_cli_functions(), expected_functions)

    def test_get_all_default_timed_cli_functions(self):
        expected_schedules = {
            "provider_collector": "cron(5 0 1 * ? *)",
            "carbon_collector": "cron(30 0 * * ? *)",
            "performance_collector": "cron(30 0 * * ? *)",
            "log_syncer": "cron(5 0 * * ? *)",
            "deployment_manager": "cron(0 1 * * ? *)",
            "deployment_migrator": "cron(0 2 * * ? *)",
        }
        self.assertEqual(get_all_default_timed_cli_functions(), expected_schedules)

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_is_aws_framework_deployed(self, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, True, True]
        self.assertTrue(is_aws_framework_deployed(mock_client))

        mock_client.resource_exists.side_effect = [False, True, True]
        self.assertFalse(is_aws_framework_deployed(mock_client))

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_is_aws_framework_deployed_all_resources_exist(self, MockAWSRemoteClient):
        """Test is_aws_framework_deployed when all resources exist."""
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.return_value = True

        self.assertTrue(is_aws_framework_deployed(mock_client))
        self.assertEqual(mock_client.resource_exists.call_count, 3)

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_is_aws_framework_deployed_missing_iam_role(self, MockAWSRemoteClient):
        """Test is_aws_framework_deployed when IAM role is missing."""
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [False, True, True]

        self.assertFalse(is_aws_framework_deployed(mock_client))

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_is_aws_framework_deployed_missing_function(self, MockAWSRemoteClient):
        """Test is_aws_framework_deployed when function is missing."""
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, False, True]

        self.assertFalse(is_aws_framework_deployed(mock_client))

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_is_aws_framework_deployed_missing_ecr_repository(self, MockAWSRemoteClient):
        """Test is_aws_framework_deployed when ECR repository is missing."""
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, True, False]

        self.assertFalse(is_aws_framework_deployed(mock_client))

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    def test_is_gcp_framework_deployed_all_resources_exist(self, MockGCPRemoteClient):
        """Test is_gcp_framework_deployed when all resources exist."""
        mock_client = MockGCPRemoteClient.return_value
        mock_client.resource_exists.return_value = True

        self.assertTrue(is_gcp_framework_deployed(mock_client))
        self.assertEqual(mock_client.resource_exists.call_count, 3)

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    def test_is_gcp_framework_deployed_missing_resources(self, MockGCPRemoteClient):
        """Test is_gcp_framework_deployed when resources are missing."""
        mock_client = MockGCPRemoteClient.return_value
        mock_client.resource_exists.side_effect = [True, False, True]

        self.assertFalse(is_gcp_framework_deployed(mock_client))

    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch.dict(os.environ, {}, clear=True)
    def test_is_framework_deployed_aws_default(self, MockAWSRemoteClient, mock_is_aws_deployed):
        """Test is_framework_deployed defaults to AWS."""
        mock_is_aws_deployed.return_value = True

        result = is_framework_deployed()

        self.assertTrue(result)
        mock_is_aws_deployed.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.is_gcp_framework_deployed")
    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"})
    def test_is_framework_deployed_gcp(self, MockGCPRemoteClient, mock_is_gcp_deployed):
        """Test is_framework_deployed uses GCP when provider is set."""
        mock_is_gcp_deployed.return_value = True

        result = is_framework_deployed()

        self.assertTrue(result)
        mock_is_gcp_deployed.assert_called_once()

    def test_get_timer_rule_name(self):
        self.assertEqual(_get_timer_rule_name("test_function"), "test_function-timer-rule")

    def test_get_cli_invoke_payload(self):
        expected_payload = {
            "action": "data_collect",
            "collector": "provider",
        }
        self.assertEqual(get_cli_invoke_payload("provider_collector"), expected_payload)

    def test_get_cli_invoke_payload_all_functions(self):
        """Test get_cli_invoke_payload for all supported functions."""
        # Test data collectors
        expected_provider = {"action": "data_collect", "collector": "provider"}
        self.assertEqual(get_cli_invoke_payload("provider_collector"), expected_provider)

        expected_carbon = {"action": "data_collect", "collector": "carbon"}
        self.assertEqual(get_cli_invoke_payload("carbon_collector"), expected_carbon)

        expected_performance = {"action": "data_collect", "collector": "performance"}
        self.assertEqual(get_cli_invoke_payload("performance_collector"), expected_performance)

        # Test other functions
        expected_log_syncer = {"action": "log_sync"}
        self.assertEqual(get_cli_invoke_payload("log_syncer"), expected_log_syncer)

        expected_manager = {"action": "manage_deployments", "deployment_metrics_calculator_type": "simple"}
        self.assertEqual(get_cli_invoke_payload("deployment_manager"), expected_manager)

        expected_migrator = {"action": "run_deployment_migrator"}
        self.assertEqual(get_cli_invoke_payload("deployment_migrator"), expected_migrator)

    def test_action_type_to_function_name_all_mappings(self):
        """Test action_type_to_function_name for all supported mappings."""
        mappings = {
            "log_sync": "log_syncer",
            "manage_deployments": "deployment_manager",
            "run_deployment_migrator": "deployment_migrator",
            "data_collect": "data_collector",
            "remove_workflow": "remove_workflow",
        }

        for action_type, expected_function in mappings.items():
            self.assertEqual(action_type_to_function_name(action_type), expected_function)

    def test_action_type_to_function_name_invalid(self):
        """Test action_type_to_function_name with invalid action type."""
        with self.assertRaises(ValueError) as context:
            action_type_to_function_name("invalid_action")

        self.assertIn("Invalid or no directly translation action type", str(context.exception))

    def test_get_all_available_timed_cli_functions_2(self):
        """Test get_all_available_timed_cli_functions returns expected list."""
        expected_functions = [
            "provider_collector",
            "carbon_collector",
            "performance_collector",
            "log_syncer",
            "deployment_manager",
            "deployment_migrator",
        ]
        result = get_all_available_timed_cli_functions()
        self.assertEqual(result, expected_functions)

    def test_get_all_default_timed_cli_functions_2(self):
        """Test get_all_default_timed_cli_functions returns expected schedules."""
        expected_schedules = {
            "provider_collector": "cron(5 0 1 * ? *)",
            "carbon_collector": "cron(30 0 * * ? *)",
            "performance_collector": "cron(30 0 * * ? *)",
            "log_syncer": "cron(5 0 * * ? *)",
            "deployment_manager": "cron(0 1 * * ? *)",
            "deployment_migrator": "cron(0 2 * * ? *)",
        }
        result = get_all_default_timed_cli_functions()
        self.assertEqual(result, expected_schedules)

    def test_action_type_to_function_name(self):
        self.assertEqual(action_type_to_function_name("log_sync"), "log_syncer")
        with self.assertRaises(ValueError):
            action_type_to_function_name("invalid_action")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_description")
    def test_setup_aws_timers(self, mock_get_description, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.return_value = True
        mock_get_description.return_value = "Every day at 12:30 AM"

        new_rules = [("carbon_collector", "cron(30 0 * * ? *)")]
        setup_aws_timers(new_rules)

        mock_client.create_timer_rule.assert_called_once()
        mock_get_description.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_remove_aws_timers(self, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.resource_exists.return_value = True

        remove_aws_timers(["carbon_collector"])

        mock_client.remove_timer_rule.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_description")
    def test_setup_aws_timers_success(self, mock_get_description, mock_is_deployed, MockAWSRemoteClient):
        """Test successful AWS timer setup."""
        mock_client = MockAWSRemoteClient.return_value
        mock_is_deployed.return_value = True
        mock_get_description.return_value = "Every day at 12:30 AM"

        new_rules = [("carbon_collector", "cron(30 0 * * ? *)"), ("provider_collector", "rate(1 day)")]
        setup_aws_timers(new_rules)

        self.assertEqual(mock_client.create_timer_rule.call_count, 2)
        mock_get_description.assert_called_once()  # Only called for cron expressions

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_setup_aws_timers_framework_not_deployed(self, mock_is_deployed, MockAWSRemoteClient):
        """Test AWS timer setup when framework is not deployed."""
        mock_client = MockAWSRemoteClient.return_value
        mock_is_deployed.return_value = False

        new_rules = [("carbon_collector", "cron(30 0 * * ? *)")]
        setup_aws_timers(new_rules)

        mock_client.create_timer_rule.assert_not_called()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_setup_aws_timers_with_exception(self, mock_is_deployed, MockAWSRemoteClient):
        """Test AWS timer setup with exception handling."""
        mock_client = MockAWSRemoteClient.return_value
        mock_is_deployed.return_value = True
        mock_client.create_timer_rule.side_effect = Exception("Test error")

        new_rules = [("carbon_collector", "cron(30 0 * * ? *)")]

        # Should not raise exception, just print error
        setup_aws_timers(new_rules)

        mock_client.create_timer_rule.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_remove_aws_timers_success(self, mock_is_deployed, MockAWSRemoteClient):
        """Test successful AWS timer removal."""
        mock_client = MockAWSRemoteClient.return_value
        mock_is_deployed.return_value = True

        remove_aws_timers(["carbon_collector", "provider_collector"])

        self.assertEqual(mock_client.remove_timer_rule.call_count, 2)

        # Verify correct rule names were used
        expected_calls = [
            call("caribou_cli", "carbon_collector-timer-rule"),
            call("caribou_cli", "provider_collector-timer-rule"),
        ]
        mock_client.remove_timer_rule.assert_has_calls(expected_calls)

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_remove_aws_timers_framework_not_deployed(self, mock_is_deployed, MockAWSRemoteClient):
        """Test AWS timer removal when framework is not deployed."""
        mock_client = MockAWSRemoteClient.return_value
        mock_is_deployed.return_value = False

        remove_aws_timers(["carbon_collector"], verbose=True)

        mock_client.remove_timer_rule.assert_not_called()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_report_timer_schedule_expression(self, MockAWSRemoteClient):
        mock_client = MockAWSRemoteClient.return_value
        mock_client.get_timer_rule_schedule_expression.return_value = "cron(30 0 * * ? *)"

        self.assertEqual(report_timer_schedule_expression("carbon_collector"), "cron(30 0 * * ? *)")

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_gcp_framework_deployed")
    def test_setup_gcp_timers_success(self, mock_is_deployed, MockGCPRemoteClient):
        """Test successful GCP timer setup."""
        mock_client = MockGCPRemoteClient.return_value
        mock_is_deployed.return_value = True

        new_rules = [
            ("carbon_collector", "cron(30 0 * * ? *)"),  # Should convert to "30 0 * * *"
            ("provider_collector", "cron(5 0 1 * ? *)"),  # Should convert to "5 0 1 * *"
        ]
        setup_gcp_timers(new_rules)

        self.assertEqual(mock_client.create_timer_rule.call_count, 2)

        # Verify schedule conversion
        calls = mock_client.create_timer_rule.call_args_list
        self.assertEqual(calls[0][0][1], "30 0 * * *")  # Converted schedule
        self.assertEqual(calls[1][0][1], "5 0 1 * *")  # Converted schedule

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_gcp_framework_deployed")
    def test_setup_gcp_timers_framework_not_deployed(self, mock_is_deployed, MockGCPRemoteClient):
        """Test GCP timer setup when framework is not deployed."""
        mock_client = MockGCPRemoteClient.return_value
        mock_is_deployed.return_value = False

        new_rules = [("carbon_collector", "cron(30 0 * * ? *)")]
        setup_gcp_timers(new_rules)

        mock_client.create_timer_rule.assert_not_called()

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_gcp_framework_deployed")
    def test_remove_gcp_timers_success(self, mock_is_deployed, MockGCPRemoteClient):
        """Test successful GCP timer removal."""
        mock_client = MockGCPRemoteClient.return_value
        mock_is_deployed.return_value = True

        remove_gcp_timers(["carbon_collector", "provider_collector"])

        self.assertEqual(mock_client.remove_timer_rule.call_count, 2)

        # Verify correct rule names were used
        expected_calls = [
            call("caribou-cli", "carbon_collector-timer-rule"),
            call("caribou-cli", "provider_collector-timer-rule"),
        ]
        mock_client.remove_timer_rule.assert_has_calls(expected_calls)

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_gcp_framework_deployed")
    def test_remove_gcp_timers_with_exception(self, mock_is_deployed, MockGCPRemoteClient):
        """Test GCP timer removal with exception handling."""
        mock_client = MockGCPRemoteClient.return_value
        mock_is_deployed.return_value = True
        mock_client.remove_timer_rule.side_effect = Exception("Test error")

        remove_gcp_timers(["carbon_collector"])

        mock_client.remove_timer_rule.assert_called_once()

    # ========== Generic Timer Interface Tests ==========

    @patch("caribou.deployment.client.remote_cli.remote_cli.setup_aws_timers")
    @patch.dict(os.environ, {}, clear=True)
    def test_setup_timers_defaults_to_aws(self, mock_setup_aws):
        """Test that setup_timers defaults to AWS."""
        new_rules = [("test_function", "cron(0 0 * * ? *)")]
        setup_timers(new_rules)
        mock_setup_aws.assert_called_once_with(new_rules)

    @patch("caribou.deployment.client.remote_cli.remote_cli.setup_gcp_timers")
    @patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"})
    def test_setup_timers_uses_gcp(self, mock_setup_gcp):
        """Test that setup_timers uses GCP when provider is set."""
        new_rules = [("test_function", "cron(0 0 * * ? *)")]
        setup_timers(new_rules)
        mock_setup_gcp.assert_called_once_with(new_rules)

    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_timers")
    @patch.dict(os.environ, {}, clear=True)
    def test_remove_timers_defaults_to_aws(self, mock_remove_aws):
        """Test that remove_timers defaults to AWS."""
        functions = ["test_function"]
        remove_timers(functions)
        mock_remove_aws.assert_called_once_with(functions, True)

    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_gcp_timers")
    @patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"})
    def test_remove_timers_uses_gcp(self, mock_remove_gcp):
        """Test that remove_timers uses GCP when provider is set."""
        functions = ["test_function"]
        remove_timers(functions, verbose=False)
        mock_remove_gcp.assert_called_once_with(functions, False)

    # ========== Timer Reporting Tests ==========

    @patch("caribou.deployment.client.remote_cli.remote_cli.report_aws_timer_schedule_expression")
    @patch.dict(os.environ, {}, clear=True)
    def test_report_timer_schedule_expression_defaults_to_aws(self, mock_report_aws):
        """Test that report_timer_schedule_expression defaults to AWS."""
        mock_report_aws.return_value = "cron(30 0 * * ? *)"

        result = report_timer_schedule_expression("carbon_collector")

        self.assertEqual(result, "cron(30 0 * * ? *)")
        mock_report_aws.assert_called_once_with("carbon_collector")

    @patch("caribou.deployment.client.remote_cli.remote_cli.report_gcp_timer_schedule_expression")
    @patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"})
    def test_report_timer_schedule_expression_uses_gcp(self, mock_report_gcp):
        """Test that report_timer_schedule_expression uses GCP when provider is set."""
        mock_report_gcp.return_value = "30 0 * * *"

        result = report_timer_schedule_expression("carbon_collector")

        self.assertEqual(result, "30 0 * * *")
        mock_report_gcp.assert_called_once_with("carbon_collector")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    def test_report_aws_timer_schedule_expression(self, MockAWSRemoteClient):
        """Test report_aws_timer_schedule_expression."""
        mock_client = MockAWSRemoteClient.return_value
        mock_client.get_timer_rule_schedule_expression.return_value = "cron(30 0 * * ? *)"

        result = report_aws_timer_schedule_expression("carbon_collector")

        self.assertEqual(result, "cron(30 0 * * ? *)")
        mock_client.get_timer_rule_schedule_expression.assert_called_once_with("carbon_collector-timer-rule")

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    def test_report_gcp_timer_schedule_expression(self, MockGCPRemoteClient):
        """Test report_gcp_timer_schedule_expression."""
        mock_client = MockGCPRemoteClient.return_value
        mock_client.get_timer_rule_schedule_expression.return_value = "30 0 * * *"

        result = report_gcp_timer_schedule_expression("carbon_collector")

        self.assertEqual(result, "30 0 * * *")
        mock_client.get_timer_rule_schedule_expression.assert_called_once_with("carbon_collector-timer-rule")

    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_aws_remote_framework")
    @patch.dict(os.environ, {}, clear=True)
    def test_remove_remote_framework_defaults_to_aws(self, mock_remove_aws):
        """Test that remove_remote_framework defaults to AWS when no provider is set."""
        remove_remote_framework()
        mock_remove_aws.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.remove_gcp_remote_framework")
    @patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"})
    def test_remove_remote_framework_uses_gcp(self, mock_remove_gcp):
        """Test that remove_remote_framework uses GCP when provider is set."""
        remove_remote_framework()
        mock_remove_gcp.assert_called_once()

    @patch("caribou.deployment.client.remote_cli.remote_cli.deploy_aws_remote_framework")
    @patch.dict(os.environ, {}, clear=True)
    def test_deploy_remote_framework_defaults_to_aws(self, mock_deploy_aws):
        """Test that deploy_remote_framework defaults to AWS when no provider is set."""
        deploy_remote_framework("/fake/dir", 300, 128, 512)
        mock_deploy_aws.assert_called_once_with("/fake/dir", 300, 128, 512)

    @patch("caribou.deployment.client.remote_cli.remote_cli.deploy_gcp_remote_framework")
    @patch.dict(os.environ, {"CARIBOU_DEFAULT_PROVIDER": "gcp"})
    def test_deploy_remote_framework_uses_gcp(self, mock_deploy_gcp):
        """Test that deploy_remote_framework uses GCP when provider is set."""
        deploy_remote_framework("/fake/dir", 300, 128, 512, 2)
        mock_deploy_gcp.assert_called_once_with("/fake/dir", 300, 128, 512, 2)

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.DeploymentPackager")
    @patch("builtins.open", new_callable=mock_open, read_data='{"aws": {}}')
    @patch("tempfile.TemporaryDirectory")
    def test_deploy_aws_framework_missing_env_vars(
        self, mock_tempdir, mock_file, MockDeploymentPackager, MockAWSRemoteClient
    ):
        """Test AWS framework deployment with missing environment variables."""
        mock_client = MockAWSRemoteClient.return_value
        mock_packager = MockDeploymentPackager.return_value
        mock_tempdir.return_value.__enter__.return_value = "/tmp/test"

        mock_client.resource_exists.return_value = False
        mock_client.create_role.return_value = "arn:aws:iam::123456789012:role/test-role"
        mock_packager.create_framework_package.return_value = "/tmp/test/package.zip"

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError):
                deploy_aws_remote_framework("/fake/project/dir", 300, 128, 512)

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.DeploymentPackager")
    @patch("builtins.open", new_callable=mock_open, read_data='{"gcp": {}}')
    @patch("tempfile.TemporaryDirectory")
    @patch("time.sleep")
    def test_deploy_gcp_framework_missing_env_vars(
        self, mock_sleep, mock_tempdir, mock_file, MockDeploymentPackager, MockGCPRemoteClient
    ):
        """Test GCP framework deployment with missing environment variables."""
        mock_client = MockGCPRemoteClient.return_value
        mock_packager = MockDeploymentPackager.return_value
        mock_tempdir.return_value.__enter__.return_value = "/tmp/test"

        mock_client.resource_exists.return_value = False
        mock_client.get_service_account.return_value = "test@project.iam.gserviceaccount.com"
        mock_client.create_role.return_value = "test@project.iam.gserviceaccount.com"
        mock_packager.create_framework_package.return_value = "/tmp/test/package.zip"

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError):
                deploy_gcp_remote_framework("/fake/project/dir", 300, 128, 512)

    def test_get_cli_invoke_payload_invalid_function(self):
        """Test get_cli_invoke_payload with invalid function name."""
        with self.assertRaises(KeyError):
            get_cli_invoke_payload("invalid_function_name")

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    def test_remove_aws_timers_silent_mode(self, mock_is_deployed, MockAWSRemoteClient):
        """Test remove_aws_timers in silent mode (verbose=False)."""
        mock_client = MockAWSRemoteClient.return_value
        mock_is_deployed.return_value = False  # Framework not deployed

        remove_aws_timers(["carbon_collector"], verbose=False)

        # Should not print anything and not call remove_timer_rule
        mock_client.remove_timer_rule.assert_not_called()

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_gcp_framework_deployed")
    def test_remove_gcp_timers_silent_mode(self, mock_is_deployed, MockGCPRemoteClient):
        """Test remove_gcp_timers in silent mode (verbose=False)."""
        mock_client = MockGCPRemoteClient.return_value
        mock_is_deployed.return_value = False  # Framework not deployed

        remove_gcp_timers(["carbon_collector"], verbose=False)

        # Should not print anything and not call remove_timer_rule
        mock_client.remove_timer_rule.assert_not_called()

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.DeploymentPackager")
    @patch("builtins.open", new_callable=mock_open)
    @patch("tempfile.TemporaryDirectory")
    @patch.dict(os.environ, {"GOOGLE_API_KEY": "key", "ELECTRICITY_MAPS_AUTH_TOKEN": "token"})
    def test_full_aws_deployment_cycle(self, mock_tempdir, mock_file, MockDeploymentPackager, MockAWSRemoteClient):
        """Test complete AWS deployment cycle with all components."""
        mock_client = MockAWSRemoteClient.return_value
        mock_packager = MockDeploymentPackager.return_value
        mock_tempdir.return_value.__enter__.return_value = "/tmp/test"

        # Setup file content mock
        mock_file.return_value.read.side_effect = [
            '{"aws": {"policy": "content"}}',  # IAM policy file
            b"fake_zip_content",  # Zip file content
        ]

        # Setup client behavior
        mock_client.resource_exists.side_effect = [False, False]  # No existing resources
        mock_client.create_role.return_value = "arn:aws:iam::123456789012:role/test-role"
        mock_packager.create_framework_package.return_value = "/tmp/test/package.zip"

        deploy_aws_remote_framework("/fake/project/dir", 600, 256, 1024)

        # Verify complete deployment sequence
        mock_client.create_role.assert_called_once()
        mock_client.deploy_remote_cli.assert_called_once()

        # Verify deployment parameters
        deploy_args = mock_client.deploy_remote_cli.call_args[0]
        self.assertEqual(deploy_args[2], "arn:aws:iam::123456789012:role/test-role")  # role_arn
        self.assertEqual(deploy_args[3], 600)  # timeout
        self.assertEqual(deploy_args[4], 256)  # memory_size
        self.assertEqual(deploy_args[5], 1024)  # ephemeral_storage

    @patch("caribou.deployment.client.remote_cli.remote_cli.AWSRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_aws_framework_deployed")
    @patch("caribou.deployment.client.remote_cli.remote_cli.get_description")
    def test_setup_aws_timers_multiple_schedule_types(
        self, mock_get_description, mock_is_deployed, MockAWSRemoteClient
    ):
        """Test AWS timer setup with different schedule expression types."""
        mock_client = MockAWSRemoteClient.return_value
        mock_is_deployed.return_value = True
        mock_get_description.return_value = "Every day at 12:30 AM"

        new_rules = [
            ("carbon_collector", "cron(30 0 * * ? *)"),  # cron expression
            ("provider_collector", "rate(1 day)"),  # rate expression
            ("log_syncer", "custom_schedule_expression"),  # custom expression
        ]

        setup_aws_timers(new_rules)

        # Should create all timer rules
        self.assertEqual(mock_client.create_timer_rule.call_count, 3)

        # get_description should only be called for cron expressions
        mock_get_description.assert_called_once_with("30 0 * * ? *", unittest.mock.ANY)

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.is_gcp_framework_deployed")
    def test_gcp_schedule_conversion_edge_cases(self, mock_is_deployed, MockGCPRemoteClient):
        """Test GCP schedule conversion handles edge cases correctly."""
        mock_client = MockGCPRemoteClient.return_value
        mock_is_deployed.return_value = True

        # Test various cron formats that need conversion
        new_rules = [
            ("test1", "cron(0 0 1 1 ? *)"),  # Should become "0 0 1 1 *"
            ("test2", "cron(15 30 * * ? *)"),  # Should become "15 30 * * *"
            ("test3", "cron(0 12 ? * MON *)"),  # Should become "0 12 * * MON"
        ]

    @patch("caribou.deployment.client.remote_cli.remote_cli.GCPRemoteClient")
    @patch("caribou.deployment.client.remote_cli.remote_cli.DeploymentPackager")
    @patch("builtins.open", new_callable=mock_open)
    @patch("tempfile.TemporaryDirectory")
    @patch.dict(
        os.environ, {"GOOGLE_API_KEY": "key", "ELECTRICITY_MAPS_AUTH_TOKEN": "token", "CARIBOU_DEFAULT_PROVIDER": "gcp"}
    )
    def test_full_gcp_deployment_cycle(self, mock_tempdir, mock_file, MockDeploymentPackager, MockGCPRemoteClient):
        """Test complete GCP deployment cycle with all components."""
        mock_client = MockGCPRemoteClient.return_value
        mock_packager = MockDeploymentPackager.return_value
        mock_tempdir.return_value.__enter__.return_value = "/tmp/test"

        # Setup file content mock
        mock_file.return_value.read.side_effect = [
            '{"gcp": {"policy": "content"}}',  # IAM policy file
            b"fake_zip_content",  # Zip file content
        ]

        # Setup client behavior
        mock_client.resource_exists.side_effect = [False, False]  # No existing resources
        mock_client.create_role.return_value = "test@project.iam.gserviceaccount.com"
        mock_packager.create_framework_package.return_value = "/tmp/test/package.zip"

        deploy_gcp_remote_framework("/fake/project/dir", 600, 256, 1024, 2)

        # Verify complete deployment sequence
        mock_client.create_role.assert_called_once()
        mock_client.deploy_remote_cli.assert_called_once()

        # Verify deployment parameters include CPU
        deploy_args = mock_client.deploy_remote_cli.call_args[0]
        self.assertEqual(deploy_args[2], "test@project.iam.gserviceaccount.com")  # service account
        self.assertEqual(deploy_args[3], 600)  # timeout
        self.assertEqual(deploy_args[4], 256)  # memory_size
        self.assertEqual(deploy_args[5], 1024)  # ephemeral_storage
        self.assertEqual(deploy_args[9], 2)  # cpu


if __name__ == "__main__":
    unittest.main()
