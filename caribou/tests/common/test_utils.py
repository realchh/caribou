import hashlib
import unittest
from caribou.common.utils import (
    decompress_json_str,
    get_function_source,
    generate_workflow_gcp_function_name,
    get_region_abbreviation,
    get_country_abbreviation,
    generate_workflow_service_account_id,
)
from caribou.common.utils import compress_json_str
import zstandard as zstd


class TestGetFunctionSource(unittest.TestCase):
    def test_get_function_source(self):
        def test_function():
            print("Hello, world!")

        source_code = get_function_source(test_function)

        source_code = "".join(source_code.split())

        self.assertIn('print("Hello,world!")', source_code)

    def test_get_function_source_with_called_function(self):
        def called_function():
            print("Hello from called function!")

        def test_function():
            print("Hello, world!")
            called_function()

        source_code = get_function_source(test_function)

        source_code = "".join(source_code.split())

        self.assertIn('print("Hello,world!")', source_code)

    def test_compress_json_str(self):
        json_str = '{"key": "value"}'
        compressed_bytes = compress_json_str(json_str)

        # Decompress to verify
        dctx = zstd.ZstdDecompressor()
        decompressed_bytes = dctx.decompress(compressed_bytes)
        decompressed_str = decompressed_bytes.decode("utf-8")

        self.assertEqual(json_str, decompressed_str)

    def test_compress_json_str_with_different_compression_level(self):
        json_str = '{"key": "value"}'
        compressed_bytes = compress_json_str(json_str, compression_level=10)

        # Decompress to verify
        dctx = zstd.ZstdDecompressor()
        decompressed_bytes = dctx.decompress(compressed_bytes)
        decompressed_str = decompressed_bytes.decode("utf-8")

        self.assertEqual(json_str, decompressed_str)

    def test_decompress_json_str(self):
        json_str = '{"key": "value"}'
        compressed_bytes = compress_json_str(json_str)
        decompressed_str = decompress_json_str(compressed_bytes)

        self.assertEqual(json_str, decompressed_str)

    def test_decompress_json_str_with_different_compression_level(self):
        json_str = '{"key": "value"}'
        compressed_bytes = compress_json_str(json_str, compression_level=10)
        decompressed_str = decompress_json_str(compressed_bytes)

        self.assertEqual(json_str, decompressed_str)

    def test_decompress_json_str_with_invalid_data(self):
        invalid_data = b"invalid compressed data"
        with self.assertRaises(zstd.ZstdError):
            decompress_json_str(invalid_data)

    ## Tests for generate_workflow_service_account_id ##

    def test_generate_sa_id_standard(self):
        """Tests standard workflow name and version."""
        name = "my-awesome-workflow"
        ver = "v1"
        expected_hash = hashlib.md5(f"{name}-{ver}".encode("utf-8")).hexdigest()[:12]
        result = generate_workflow_service_account_id(name, ver)
        self.assertEqual(result, f"my-a-flow-v1-{expected_hash}")
        self.assertTrue(len(result) <= 30)

    def test_generate_sa_id_short_name(self):
        """Tests a workflow name shorter than 4 characters."""
        name = "etl"
        ver = "v2"
        expected_hash = hashlib.md5(f"{name}-{ver}".encode("utf-8")).hexdigest()[:12]
        result = generate_workflow_service_account_id(name, ver)
        self.assertEqual(result, f"etl-etl-v2-{expected_hash}")
        self.assertTrue(len(result) <= 30)

    def test_generate_sa_id_with_special_chars(self):
        """Tests that underscores and periods are replaced."""
        name = "my_project.etl"
        ver = "0.0.1"
        expected_name = "my-project-etl"
        expected_ver = "0-0-1"
        full_name = f"{expected_name}-{expected_ver}"
        expected_hash = hashlib.md5(full_name.encode("utf-8")).hexdigest()[:9]
        result = generate_workflow_service_account_id(name, ver)
        self.assertEqual(result, f"my-p-etl-{expected_ver}-{expected_hash}")
        self.assertTrue(len(result) <= 30)

    ## Tests for get_country_abbreviation ##

    def test_get_country_abbreviation(self):
        """Tests all country abbreviation mappings."""
        self.assertEqual(get_country_abbreviation("us"), "us")
        self.assertEqual(get_country_abbreviation("europe"), "eu")
        self.assertEqual(get_country_abbreviation("asia"), "as")
        self.assertEqual(get_country_abbreviation("africa"), "af")
        self.assertEqual(get_country_abbreviation("australia"), "au")
        self.assertEqual(get_country_abbreviation("northamerica"), "na")
        self.assertEqual(get_country_abbreviation("southamerica"), "sa")
        self.assertEqual(get_country_abbreviation("me"), "me")
        self.assertEqual(get_country_abbreviation("canada"), "canada")  # Unhandled case

    ## Tests for get_region_abbreviation ##

    def test_get_region_abbreviation(self):
        """Tests various region abbreviation mappings."""
        self.assertEqual(get_region_abbreviation("central"), "ce")
        self.assertEqual(get_region_abbreviation("eastus2"), "eaus2")
        self.assertEqual(get_region_abbreviation("north"), "no")
        self.assertEqual(get_region_abbreviation("southwest"), "sw")
        self.assertEqual(get_region_abbreviation("northeast1"), "ne1")
        self.assertEqual(get_region_abbreviation("california"), "california")  # Unhandled case

    ## Tests for generate_workflow_gcp_function_name ##

    def test_generate_gcp_function_name_standard(self):
        """Tests standard inputs for GCP function name generation."""
        w_name = "billing-workflow"
        w_ver = "v1-0"
        f_name = "process-invoices"
        region_info = {"provider": "gcp", "region": "us-central1"}

        expected_hash_str = f"billing-workflow-{w_ver}-process-invoices"
        expected_hash = hashlib.md5(expected_hash_str.encode("utf-8")).hexdigest()

        # Expected hash length = 22 - len("v1-0") - len("us") - len("ce1") = 22 - 4 - 2 - 3 = 13
        truncated_hash = expected_hash[:13]

        expected = f"bill-flow-v1-0-proc-ices-gcp-us-ce1-{truncated_hash}"
        result = generate_workflow_gcp_function_name(w_name, w_ver, f_name, region_info)
        self.assertEqual(result, expected)

    def test_generate_gcp_function_name_short_names(self):
        """Tests short workflow and function names."""
        w_name = "etl"
        w_ver = "v1"
        f_name = "run"
        region_info = {"provider": "gcp", "region": "europe-west4"}

        expected_hash_str = f"etl-{w_ver}-run"
        expected_hash = hashlib.md5(expected_hash_str.encode("utf-8")).hexdigest()

        # Expected hash length = 22 - len("v1") - len("eu") - len("we4") = 22 - 2 - 2 - 3 = 15
        truncated_hash = expected_hash[:15]

        expected = f"etl-etl-v1-run-run-gcp-eu-we4-{truncated_hash}"
        result = generate_workflow_gcp_function_name(w_name, w_ver, f_name, region_info)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
