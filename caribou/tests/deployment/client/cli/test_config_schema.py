import unittest
import os
from pathlib import Path
import yaml
import pytest

from pydantic import ValidationError

from caribou.deployment.common.config.config_schema import (
    ConfigSchema,
    RegionAndProviders,
    Provider,
    Constraint,
    Constraints,
    EnvironmentVariable,
    ProviderRegion,
)


class TestConfigSchema(unittest.TestCase):
    def test_config_conforms_to_schema(self):
        current_dir = Path(__file__).parent.parent.parent.parent.parent
        config_file = os.path.join(
            current_dir,
            "deployment/client/cli/aws_template/.caribou/config.yml",
        )

        with open(config_file, "r") as f:
            config_file = f.read()

        if not config_file:
            self.assertFalse(True)

        config_dict = yaml.safe_load(config_file)

        try:
            ConfigSchema(**config_dict)
        except ValidationError as e:
            self.assertFalse(True, e)

    def test_validate_config_provider1_missing_memory(self):
        with pytest.raises(ValueError, match="The 'config' dictionary must contain 'memory' key with an integer value"):
            RegionAndProviders(providers={"provider1": Provider(config={"timeout": 10})})

    def test_validate_config_provider1_missing_timeout(self):
        with pytest.raises(
            ValueError, match="The 'config' dictionary must contain 'timeout' key with an integer value"
        ):
            RegionAndProviders(providers={"provider1": Provider(config={"memory": 512})})

    def test_validate_config_provider1_valid_config(self):
        RegionAndProviders(providers={"provider1": Provider(config={"memory": 512, "timeout": 10})})

    def test_validate_config_gcp_valid_config(self):
        RegionAndProviders(providers={"gcp": Provider(config={"memory": 1024, "timeout": 10, "vcpu": 1.0})})

    def test_validate_config_gcp_missing_timeout(self):
        with pytest.raises(
            ValueError, match="The 'config' dictionary must contain 'timeout' key with an integer value"
        ):
            RegionAndProviders(providers={"gcp": Provider(config={"memory": 512, "vcpu": 1.0})})

    def test_validate_config_gcp_missing_memory(self):
        with pytest.raises(ValueError, match="The 'config' dictionary must contain 'memory' key with an integer value"):
            RegionAndProviders(providers={"gcp": Provider(config={"timeout": 300, "vcpu": 1.0})})

    def test_validate_config_gcp_missing_vcpu(self):
        with pytest.raises(ValueError, match="The 'config' dictionary must contain 'vcpu' key with a float value"):
            RegionAndProviders(providers={"gcp": Provider(config={"timeout": 300, "memory": 512})})

    def test_validate_config_gcp_invalid_vcpu_lower(self):
        with pytest.raises(
            ValueError, match="The 'vcpu' value must be between 0.08 and 1.0 inclusive, or one of: 2.0, 4.0, 6.0, 8.0"
        ):
            RegionAndProviders(providers={"gcp": Provider(config={"memory": 1024, "timeout": 10, "vcpu": 0.01})})

    def test_validate_config_gcp_invalid_vcpu_not_in_list(self):
        with pytest.raises(
            ValueError, match="The 'vcpu' value must be between 0.08 and 1.0 inclusive, or one of: 2.0, 4.0, 6.0, 8.0"
        ):
            RegionAndProviders(providers={"gcp": Provider(config={"memory": 1024, "timeout": 10, "vcpu": 3.0})})

    def test_constraint_invalid_type(self):
        with pytest.raises(ValueError, match="Constraint type invalid is not supported"):
            Constraint(value=1.0, type="invalid")

    def test_constraint_absolute_type(self):
        constraint = Constraint(value=1.0, type="absolute")
        self.assertEqual(constraint.value, 1.0)
        self.assertEqual(constraint.type, "absolute")

    def test_constraint_relative_type(self):
        constraint = Constraint(value=1.0, type="relative")
        self.assertEqual(constraint.value, 1.0)
        self.assertEqual(constraint.type, "relative")

    def test_constraints_invalid_priority_order(self):
        with pytest.raises(ValueError, match="Priority order value invalid is not supported"):
            Constraints(
                hard_resource_constraints={"cost": Constraint(value=1.0, type="absolute")},
                soft_resource_constraints={"runtime": Constraint(value=1.0, type="relative")},
                priority_order=["invalid"],
            )

    def test_constraints_invalid_hard_resource_constraint(self):
        with pytest.raises(ValueError, match="Hard resource constraint invalid is not supported"):
            Constraints(
                hard_resource_constraints={"invalid": Constraint(value=1.0, type="absolute")},
                soft_resource_constraints={"runtime": Constraint(value=1.0, type="relative")},
                priority_order=["cost", "runtime"],
            )

    def test_constraints_invalid_soft_resource_constraint(self):
        with pytest.raises(ValueError, match="Soft resource constraint invalid is not supported"):
            Constraints(
                hard_resource_constraints={"cost": Constraint(value=1.0, type="absolute")},
                soft_resource_constraints={"invalid": Constraint(value=1.0, type="relative")},
                priority_order=["cost", "runtime"],
            )

    def test_validate_config_unsupported_provider(self):
        with pytest.raises(ValueError, match="Provider non-provider is not supported"):
            RegionAndProviders(providers={"non-provider": Provider(config={})})


if __name__ == "__main__":
    unittest.main()
