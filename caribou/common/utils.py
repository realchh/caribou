import ast
import hashlib
import importlib
import inspect
import textwrap
from typing import Any, Callable

import zstandard as zstd


def str_to_bool(s: str) -> bool:
    return s.lower() in ["true", "1", "t", "y", "yes"]


def get_function_source(function_callable: Callable[..., Any]) -> str:
    module_name = inspect.getmodule(function_callable).__name__  # type: ignore
    module = importlib.import_module(module_name)

    context = vars(module)
    source_code = ""

    # Get the source of the initial function
    source = inspect.getsource(function_callable)
    source = textwrap.dedent(source)  # Remove leading whitespace
    source_code += source + "\n\n# Called functions:\n\n"

    # Parse the source code of the function
    tree = ast.parse(source)

    # Dictionary to hold the functions we've already included
    included_functions = {"invoke_serverless_function": True, "get_predecessor_data": True}

    # Function to process each node in the AST
    def process_node(node: ast.AST, context: dict) -> None:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            # Assuming the function is defined in the same file or imported directly
            name = node.func.id
            if name not in included_functions:  # Check if function was not already included
                try:
                    # Retrieve the function object by name
                    # WARNING: This is not safe but since this is only ever executed
                    # at a client, it is safe to use eval here.
                    f = eval(name, context)  # pylint: disable=eval-used
                    # Add the source code of the called function
                    func_source = inspect.getsource(f)
                    included_functions[name] = True
                    nonlocal source_code
                    source_code += f"\n# Source of {name}:\n{func_source}\n"

                    # Parse the AST of the called function and process it
                    func_tree = ast.parse(func_source)

                    # Get the module name of the function
                    module_name = inspect.getmodule(f).__name__  # type: ignore
                    module = importlib.import_module(module_name)

                    # Get the context of the module
                    new_context = vars(module)

                    for child in ast.walk(func_tree):
                        process_node(child, new_context)
                except Exception:  # pylint: disable=broad-except
                    pass

    # Process each node in the AST of the source code
    for node in ast.walk(tree):
        process_node(node, context)

    return source_code


def compress_json_str(json_str: str, compression_level: int = 21) -> bytes:
    # Compress the JSON string using zstandard
    json_bytes = json_str.encode("utf-8")
    cctx = zstd.ZstdCompressor(level=compression_level)
    compressed_bytes = cctx.compress(json_bytes)

    return compressed_bytes


def decompress_json_str(compressed_bytes: bytes) -> str:
    # Decompress the bytes using zstandard
    dctx = zstd.ZstdDecompressor()
    json_bytes = dctx.decompress(compressed_bytes)
    json_str = json_bytes.decode("utf-8")

    return json_str

def generate_workflow_service_account_id(workflow_name: str, workflow_ver: str) -> str:
    # GCP service account has a max length of 30 characters. We use a hash to shorten the name. The SA format will
    # be <first 4 chars of workflow name>-<last 4 chars of workflow name>-<workflow id>-<hash truncated to 9 chars>
    workflow_name = workflow_name.lower().replace("_", "-").replace(".", "-")
    workflow_ver = workflow_ver.lower().replace("_", "-").replace(".", "-")

    workflow_full_name = f"{workflow_name}-{workflow_ver}"
    workflow_hash = hashlib.md5(workflow_full_name.encode("utf-8")).hexdigest()

    if len(workflow_name) < 4:
        workflow_name_prefix = workflow_name.strip("-")
        workflow_name_suffix = workflow_name.strip("-")
    else:
        workflow_name_prefix = workflow_name[:4].strip("-")
        workflow_name_suffix = workflow_name[-4:].strip("-")

    return f"{workflow_name_prefix}-{workflow_name_suffix}-{workflow_ver}-{workflow_hash[:14-len(workflow_ver)]}"


def get_country_abbreviation(country: str) -> str:
    if country == "us":
        pass
    elif country == "africa":
        country = "af"
    elif country == "asia":
        country = "as"
    elif country == "europe":
        country = "eu"
    elif country == "australia":
        country = "au"
    elif country == "me":
        pass
    elif country == "northamerica":
        country = "na"
    elif country == "southamerica":
        country = "sa"

    return country


def get_region_abbreviation(region: str) -> str:
    if region.startswith("northeast"):
        region = "ne" + region[9:]
    elif region.startswith("southeast"):
        region = "se" + region[9:]
    elif region.startswith("southwest"):
        region = "sw" + region[9:]
    elif region.startswith("northwest"):
        region = "nw" + region[9:]
    elif region.startswith("north"):
        region = "no" + region[5:]
    elif region.startswith("east"):
        region = "ea" + region[4:]
    elif region.startswith("south"):
        region = "so" + region[5:]
    elif region.startswith("west"):
        region = "we" + region[4:]
    elif region.startswith("central"):
        region = "ce" + region[7:]

    return region


def generate_workflow_gcp_function_name(
        workflow_name: str, workflow_ver: str, function_name: str, region: dict[str, str]
) -> str:
    # GCP cloud run name has a max length of 49 characters. We use a hash to shorten the name. The name format will
    # be <first 4 chars of workflow name>-<last 4 chars of workflow name>-<workflow version>-
    # <first 4 chars of function name>-<last 4 chars of function name>-<hash truncated to 9 chars>
    workflow_name = workflow_name.lower().replace("_", "-").replace(".", "-")
    workflow_ver = workflow_ver.lower().replace("_", "-").replace(".", "-")
    function_name = function_name.lower().replace("_", "-").replace(".", "-")

    provider = region["provider"]
    country = region["region"].split("-")[0]
    region = region["region"].split("-")[1]

    country = get_country_abbreviation(country)
    region = get_region_abbreviation(region)

    workflow_full_name = f"{workflow_name}-{workflow_ver}-{function_name}"
    workflow_hash = hashlib.md5(workflow_full_name.encode("utf-8")).hexdigest()

    if len(workflow_name) < 4:
        workflow_name_prefix = workflow_name.strip("-")
        workflow_name_suffix = workflow_name.strip("-")
    else:
        workflow_name_prefix = workflow_name[:4].strip("-")
        workflow_name_suffix = workflow_name[-4:].strip("-")

    if len(function_name) < 4:
        function_name_prefix = function_name.strip("-")
        function_name_suffix = function_name.strip("-")
    else:
        function_name_prefix = function_name[:4].strip("-")
        function_name_suffix = function_name[-4:].strip("-")

    return (
        f"{workflow_name_prefix}-"
        f"{workflow_name_suffix}-"
        f"{workflow_ver}-"
        f"{function_name_prefix}-"
        f"{function_name_suffix}-"
        f"{provider}-{country}-{region}-"
        f"{workflow_hash[:22-len(workflow_ver)-len(country)-len(region)]}"
    )