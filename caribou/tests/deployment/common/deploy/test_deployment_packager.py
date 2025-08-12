import unittest
from unittest.mock import call, patch, MagicMock, Mock, mock_open
import tempfile
from caribou.deployment.common.deploy.deployment_packager import (
    DeploymentPackager,
    pip_import_string,
)
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.models.workflow import Workflow
import zipfile
import os
import shutil


class TestDeploymentPackager(unittest.TestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = Config({"home_region": {"provider": "aws", "region": "us-east-1"}}, self.test_dir)
        self.packager = DeploymentPackager(self.config)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("os.path.exists")
    @patch("tempfile.TemporaryDirectory")
    @patch("zipfile.ZipFile")
    def test_create_deployment_package(self, mock_zipfile, mock_temp_dir, mock_exists):
        mock_exists.return_value = True
        mock_temp_dir.return_value.__enter__.return_value = self.test_dir
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        # Make requirements.txt
        with open(os.path.join(self.test_dir, "requirements.txt"), "w") as f:
            f.write("requests\n")

        # Make project structure
        os.mkdir(os.path.join(self.test_dir, ".caribou"))

        config = MagicMock()
        config.project_dir = self.test_dir
        config.python_version = "3.8"

        packager = DeploymentPackager(config)
        result = packager._create_deployment_package(config.project_dir, config.python_version)

        self.assertEqual(result, packager._get_package_filename(config.project_dir, config.python_version))

    @patch("os.path.exists")
    def test_create_deployment_package_no_requirements(self, mock_exists):
        mock_exists.return_value = False

        config = MagicMock()
        config.project_dir = self.test_dir
        config.python_version = "3.8"

        packager = DeploymentPackager(config)
        with self.assertRaises(RuntimeError):
            packager._create_deployment_package(config.project_dir, config.python_version)

    @patch("os.walk")
    @patch("zipfile.ZipFile")
    def test_add_py_dependencies(self, mock_zipfile, mock_os_walk):
        mock_os_walk.return_value = [("/deps_dir", [], ["file1.py", "file2.py"])]
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        config = MagicMock()
        packager = DeploymentPackager(config)
        packager._add_py_dependencies(mock_zipfile, "/deps_dir")

        self.assertEqual(mock_zipfile.write.call_count, 2)

    @patch("caribou.deployment.common.deploy.deployment_packager.pip_execute")
    def test__build_dependencies(self, mock_pip_execute):
        config = MagicMock()
        packager = DeploymentPackager(config)
        tmp_dir = self.test_dir
        # Make requirements.txt
        with open(os.path.join(tmp_dir, "requirements.txt"), "w") as f:
            f.write("requests\n")
        packager._build_dependencies(os.path.join(tmp_dir, "requirements.txt"), tmp_dir)

        mock_pip_execute.assert_called_once()

    def test_pip_import_string(self):
        result = pip_import_string()

        self.assertEqual(result, "from pip._internal.cli.main import main")

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    @patch("os.walk")
    @patch("zipfile.ZipFile")
    def test__add_application_files(self, mock_zipfile, mock_os_walk, mock_exists, mock_open):
        mock_os_walk.return_value = [("/app_dir", [], ["src/file1.py", "src/file2.py"])]
        mock_zipfile.return_value.__enter__.return_value = MagicMock()
        mock_exists.return_value = True

        config = MagicMock()
        packager = DeploymentPackager(config)
        packager._add_application_files(mock_zipfile, "/app_dir")  # 2 files + 1 generic handler

        self.assertEqual(mock_zipfile.write.call_count, 3)

    @patch("zipfile.ZipFile")
    def test__add_multi_x_serverless_dependency(self, mock_zipfile):
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        config = MagicMock()
        packager = DeploymentPackager(config)
        packager._add_caribou_dependency(mock_zipfile)

        self.assertEqual(mock_zipfile.write.call_count, 23)

    @patch.object(DeploymentPackager, "_download_deployment_package", return_value="test.zip")
    def test_re_build(self, mock_download_deployment_package):
        config = Config({}, self.test_dir)
        workflow = Workflow("test_workflow", "0.0.1", [], [], [], config)
        remote_client = Mock()
        packager = DeploymentPackager(config)

        packager.re_build(workflow, remote_client)

        mock_download_deployment_package.assert_called_once_with(remote_client)
        for deployment_package in workflow.get_deployment_packages():
            self.assertEqual(deployment_package.filename, "test.zip")

    @patch.object(DeploymentPackager, "_create_deployment_package", return_value="test.zip")
    def test_build(self, mock_create_deployment_package):
        config = Config({}, self.test_dir)
        workflow = Workflow("test_workflow", "0.0.1", [], [], [], config)
        packager = DeploymentPackager(config)

        packager.build(config, workflow)

        mock_create_deployment_package.assert_called_once_with(self.test_dir, config.python_version)
        for deployment_package in workflow.get_deployment_packages():
            self.assertEqual(deployment_package.filename, "test.zip")

    @patch("tempfile.mktemp", return_value="test.zip")
    @patch("builtins.open", new_callable=mock_open)
    def test__download_deployment_package(self, mock_open, mock_mktemp):
        config = Config({}, self.test_dir)
        remote_client = Mock()
        remote_client.download_resource.return_value = b"test_content"
        packager = DeploymentPackager(config)

        result = packager._download_deployment_package(remote_client)

        self.assertEqual(result, "test.zip")
        mock_open.assert_called_once_with("test.zip", "wb")
        file_handle = mock_open()
        file_handle.write.assert_called_once_with(b"test_content")

    @patch("tempfile.mktemp", return_value="test.zip")
    @patch("builtins.open", new_callable=mock_open)
    def test__download_deployment_package_with_none_content(self, mock_open, mock_mktemp):
        config = Config({}, self.test_dir)
        remote_client = Mock()
        remote_client.download_resource.return_value = None
        packager = DeploymentPackager(config)

        with self.assertRaises(RuntimeError, msg="Could not download deployment package"):
            packager._download_deployment_package(remote_client)

    @patch("os.path.exists")
    @patch("zipfile.ZipFile")
    @patch.object(DeploymentPackager, "_create_deployment_package_dir")
    @patch.object(DeploymentPackager, "_add_framework_deployment_files")
    @patch.object(DeploymentPackager, "_add_framework_files")
    @patch.object(DeploymentPackager, "_add_framework_go_files")
    def test_create_framework_package(
        self,
        mock_add_framework_go_files,
        mock_add_framework_files,
        mock_add_framework_deployment_files,
        mock_create_deployment_package_dir,
        mock_zipfile,
        mock_exists,
    ):
        mock_exists.return_value = False
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        packager = DeploymentPackager(MagicMock())

        tmpdirname = self.test_dir
        project_dir = "/path/to/project"

        result = packager.create_framework_package(project_dir, tmpdirname)

        package_filename = os.path.join(tmpdirname, ".caribou", "deployment-packages", "caribou_framework_cli.zip")

        mock_create_deployment_package_dir.assert_called_once_with(package_filename)
        mock_zipfile.assert_called_once_with(package_filename, "w", zipfile.ZIP_DEFLATED)
        mock_add_framework_deployment_files.assert_called_once_with(
            mock_zipfile.return_value.__enter__.return_value, project_dir
        )
        mock_add_framework_files.assert_called_once_with(mock_zipfile.return_value.__enter__.return_value, project_dir)
        mock_add_framework_go_files.assert_called_once_with(
            mock_zipfile.return_value.__enter__.return_value, project_dir
        )
        self.assertEqual(result, package_filename)

    @patch("os.walk")
    @patch("os.path.join", side_effect=lambda *args: "/".join(args))
    def test_add_framework_deployment_files(self, mock_path_join, mock_os_walk):
        mock_os_walk.return_value = [
            ("/path/to/project", [], ["app.py", "file.py", "file.pyo", "poetry.lock", "src/file1.py"]),
            ("/path/to/project/src", [], ["file2.py"]),
            ("/path/to/project/caribou/deployment/client/remote_cli", [], ["remote_cli_handler.py"]),
        ]

        zip_file = MagicMock()
        zip_file.write = MagicMock()

        packager = DeploymentPackager(MagicMock())

        project_dir = "/path/to/project"
        packager._add_framework_deployment_files(zip_file, project_dir)

        expected_calls = [
            call("/path/to/project/app.py", "app.py"),
            call("/path/to/project/poetry.lock", "poetry.lock"),
            call("/path/to/project/src/file1.py", "src/file1.py"),
            call("/path/to/project/src/file2.py", "src/file2.py"),
            call("/path/to/project/caribou/deployment/client/remote_cli/remote_cli_handler.py", "app.py"),
        ]
        zip_file.write.assert_has_calls(expected_calls, any_order=True)

    @patch("os.walk")
    @patch("os.path.join", side_effect=lambda *args: "/".join(args))
    def test_add_framework_files(self, mock_path_join, mock_os_walk):
        mock_os_walk.return_value = [
            ("/path/to/project/caribou", [], ["file.py", "test_file.py", "another.py"]),
            ("/path/to/project/another", [], ["some_other.py", "test_other.py"]),
        ]

        zip_file = MagicMock()
        zip_file.write = MagicMock()

        packager = DeploymentPackager(MagicMock())

        project_dir = "/path/to/project"
        packager._add_framework_files(zip_file, project_dir)

        expected_calls = [
            call("/path/to/project/caribou/file.py", "caribou/file.py"),
            call("/path/to/project/caribou/another.py", "caribou/another.py"),
        ]
        zip_file.write.assert_has_calls(expected_calls, any_order=True)

    @patch("os.walk")
    @patch("os.path.join", side_effect=lambda *args: "/".join(args))
    def test_add_framework_go_files(self, mock_path_join, mock_os_walk):
        mock_os_walk.return_value = [
            ("/path/to/project/caribou-go", [], ["file.go", "file.py", "file.sh", "file_test.go"]),
            ("/path/to/project/caribou-go/subdir", [], ["file.mod", "file.so", "file.sum", "not_allowed.txt"]),
        ]

        zip_file = MagicMock()
        zip_file.write = MagicMock()

        packager = DeploymentPackager(MagicMock())

        project_dir = "/path/to/project"
        packager._add_framework_go_files(zip_file, project_dir)

        expected_calls = [
            call("/path/to/project/caribou-go/file.go", "caribou-go/file.go"),
            call("/path/to/project/caribou-go/file.py", "caribou-go/file.py"),
            call("/path/to/project/caribou-go/file.sh", "caribou-go/file.sh"),
            call("/path/to/project/caribou-go/subdir/file.mod", "caribou-go/subdir/file.mod"),
            call("/path/to/project/caribou-go/subdir/file.so", "caribou-go/subdir/file.so"),
            call("/path/to/project/caribou-go/subdir/file.sum", "caribou-go/subdir/file.sum"),
        ]
        zip_file.write.assert_has_calls(expected_calls, any_order=True)

    # --- NEW AND EXTENDED TESTS ---

    def test_determine_home_provider(self):
        """Tests that the home provider is correctly determined from various config formats."""
        # Test with dictionary format (AWS)
        config_aws_dict = Config({"home_region": {"provider": "aws", "region": "us-east-1"}}, self.test_dir)
        packager_aws = DeploymentPackager(config_aws_dict)
        self.assertEqual(packager_aws._determine_home_provider(), "aws")

        # Test with dictionary format (GCP)
        config_gcp_dict = Config({"home_region": {"provider": "gcp", "region": "us-central1"}}, self.test_dir)
        packager_gcp = DeploymentPackager(config_gcp_dict)
        self.assertEqual(packager_gcp._determine_home_provider(), "gcp")

        # Test with string format (legacy or simple format)
        config_aws_str = Config({"home_region": "aws:us-west-2"}, self.test_dir)
        packager_aws_str = DeploymentPackager(config_aws_str)
        self.assertEqual(packager_aws_str._determine_home_provider(), "aws")

    @patch("boto3.__version__", "1.2.3")
    def test_ensure_requirements_adds_boto3_for_aws(self):
        """Tests that boto3 is added to requirements for an AWS provider."""
        # Setup a config for AWS
        config_aws = Config({"home_region": {"provider": "aws", "region": "us-east-1"}}, self.test_dir)
        packager = DeploymentPackager(config_aws)

        req_path = os.path.join(self.test_dir, "requirements.txt")

        # Use mock_open to simulate reading and writing to the file
        m = mock_open(read_data="some-package==1.0.0")
        with patch("builtins.open", m):
            packager._ensure_requirements_filename_complete(req_path)

        # Check that the file was opened for reading, then for appending
        m.assert_any_call(req_path, "r", encoding="utf-8")
        m.assert_any_call(req_path, "a", encoding="utf-8")

        # Check that boto3 with the correct version was written to the file
        handle = m()
        # Get all the arguments from all calls to write()
        written_content = "".join(c.args[0] for c in handle.write.call_args_list)
        self.assertIn("boto3==1.2.3", written_content)
        # Also check for another common package to be sure
        self.assertIn("pyyaml==6.0.2", written_content)

    @patch("google.cloud.storage.__version__", "2.0.0")
    @patch("zstandard.__version__", "0.20.0")
    @patch.object(DeploymentPackager, "_pytz_version", "2024.1")
    @patch.object(DeploymentPackager, "_get_opentelemetry_version", return_value="1.0.0")
    def test_ensure_requirements_adds_gcp_packages(self, mock_otel):
        """Tests that all necessary GCP packages are added for a GCP provider."""
        config_gcp = Config({"home_region": {"provider": "gcp", "region": "us-central1"}}, self.test_dir)
        packager = DeploymentPackager(config_gcp)

        req_path = os.path.join(self.test_dir, "requirements.txt")

        m = mock_open(read_data="")
        with patch("builtins.open", m):
            packager._ensure_requirements_filename_complete(req_path)

        # Check that a few key GCP packages were written
        handle = m()
        written_content = "".join(c.args[0] for c in handle.write.call_args_list)
        self.assertIn("google-cloud-storage", written_content)
        self.assertIn("google-cloud-run", written_content)
        self.assertIn("functions-framework==3.*", written_content)
        self.assertIn("opentelemetry-api==1.0.0", written_content)

    def test_ensure_requirements_does_not_add_existing(self):
        """Tests that existing packages are not re-added to requirements.txt."""
        config_aws = Config({"home_region": {"provider": "aws", "region": "us-east-1"}}, self.test_dir)
        packager = DeploymentPackager(config_aws)

        req_path = os.path.join(self.test_dir, "requirements.txt")

        # Simulate a requirements file that already contains boto3
        m = mock_open(
            read_data="boto3==1.2.3\nsome-other-package==1.0\npyyaml==6.0.2\npytz==2024.1\nzstandard==0.23.0"
            "\nnumpy==2.2.1"
        )
        with patch("builtins.open", m):
            packager._ensure_requirements_filename_complete(req_path)

        # Assert that the file was opened for reading, but NOT for appending
        m.assert_any_call(req_path, "r", encoding="utf-8")
        handle = m()
        self.assertNotIn(call(req_path, "a", encoding="utf-8"), m.mock_calls)
        handle.write.assert_not_called()

    @patch("subprocess.check_output")
    def test_pytz_version_caching(self, mock_subprocess):
        """Tests that the _pytz_version property correctly caches its result."""
        mock_subprocess.return_value = b"Version: 2024.1"

        # Access the property for the first time
        version1 = self.packager._pytz_version
        self.assertEqual(version1, "2024.1")
        # The subprocess should have been called
        mock_subprocess.assert_called_once()

        # Access the property for the second time
        version2 = self.packager._pytz_version
        self.assertEqual(version2, "2024.1")
        # The subprocess should NOT have been called again; the result is cached
        mock_subprocess.assert_called_once()  # Call count is still 1

    def test_hash_project_dir_consistency(self):
        """Tests that the project hash is consistent for the same content."""
        # Create a dummy project structure
        src_dir = os.path.join(self.test_dir, "src")
        os.makedirs(src_dir)
        with open(os.path.join(self.test_dir, "app.py"), "w") as f:
            f.write("print('hello')")
        with open(os.path.join(src_dir, "utils.py"), "w") as f:
            f.write("def helper(): pass")

        req_file = os.path.join(self.test_dir, "requirements.txt")
        with open(req_file, "w") as f:
            f.write("requests")

        # Calculate hash the first time
        hash1 = self.packager._hash_project_dir(req_file, self.test_dir)

        # Calculate hash the second time with identical content
        hash2 = self.packager._hash_project_dir(req_file, self.test_dir)

        self.assertEqual(hash1, hash2)
        self.assertIsInstance(hash1, str)
        self.assertEqual(len(hash1), 64)  # sha256 hexdigest length

        # Change a file and check that the hash changes
        with open(os.path.join(src_dir, "utils.py"), "w") as f:
            f.write("def helper_modified(): pass")

        hash3 = self.packager._hash_project_dir(req_file, self.test_dir)
        self.assertNotEqual(hash1, hash3)


if __name__ == "__main__":
    unittest.main()
