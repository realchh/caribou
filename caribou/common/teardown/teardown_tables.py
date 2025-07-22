import os
from typing import Any

import boto3
from botocore.exceptions import ClientError
from google.cloud import firestore
from google.api_core import exceptions as google_api_exceptions

from caribou.common import constants
from caribou.common.models.endpoints import Endpoints
from caribou.common.provider import Provider
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def remove_aws_table(dynamodb: Any, table_name: str, verbose: bool = True) -> None:
    # Check if the table already exists (If not skip deletion)
    try:
        dynamodb.describe_table(TableName=table_name)

        # If the table exists, delete it
        dynamodb.delete_table(TableName=table_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            # If the error is not ResourceNotFoundException, raise the exception and notify the user
            raise
        if verbose:
            logger.warning("Table '%s' does not exists (Or already removed)", table_name)


def remove_bucket(s3: Any, s3_resource: Any, bucket_name: str) -> None:
    # Check if the bucket already exists (If not skip deletion)
    try:
        s3.head_bucket(Bucket=bucket_name)

        # If the bucket exists, delete it
        ## We need to first empty the bucket before deleting it
        bucket = s3_resource.Bucket(bucket_name)
        bucket.objects.all().delete()

        ## Finally delete the bucket
        s3.delete_bucket(Bucket=bucket_name)

        logger.info("Removed legacy bucket: %s", bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "404" and e.response["Error"]["Code"] != "403":
            # If the error is not 403 forbidden or 404 not found,
            # raise the exception and notify the user
            raise

def teardown_framework_tables(provider: str) -> None:
    if provider == Provider.GCP.value:
        teardown_gcp_framework_tables()
    else:
        teardown_aws_framework_tables()

def teardown_aws_framework_tables() -> None:
    dynamodb = boto3.client("dynamodb", region_name=constants.GLOBAL_SYSTEM_REGION)

    # Get all attributes of the constants module
    for attr in dir(constants):
        # If the attribute name ends with '_TABLE', create a DynamoDB table
        if attr.endswith("_TABLE"):
            table_name = getattr(constants, attr)

            if table_name in [constants.SYNC_MESSAGES_TABLE, constants.SYNC_PREDECESSOR_COUNTER_TABLE]:
                # Skip the sync tables (They are removed in a separate function)
                continue

            logger.info("Removing table: %s", table_name)
            try:
                remove_aws_table(dynamodb, table_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.error("Error remove table %s: %s", table_name, e)

def teardown_gcp_framework_tables() -> None:
    """Handles deletion of all Firestore collections."""
    try:
        firestore_db = firestore.Client()
    except Exception as e:
        logger.error("Failed to create Firestore client. Ensure GCP authentication is configured. Error: %s", e)
        return

    for attr in dir(constants):
        if attr.endswith("_TABLE"):
            collection_name = getattr(constants, attr)

            if collection_name in [constants.SYNC_MESSAGES_TABLE, constants.SYNC_PREDECESSOR_COUNTER_TABLE]:
                # Skip the sync tables (They are removed in a separate function)
                continue

            logger.info("Removing collection: %s", collection_name)
            try:
                remove_gcp_collection(firestore_db, collection_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.error("Error removing collection %s: %s", collection_name, e)

def teardown_framework_buckets(provider: str) -> None:
    # Only used for legacy buckets
    if provider == Provider.AWS.value:
        teardown_aws_framework_buckets()

def teardown_aws_framework_buckets() -> None:
    # Only used for legacy buckets
    s3 = boto3.client("s3", region_name=constants.GLOBAL_SYSTEM_REGION)
    s3_resource = boto3.resource("s3", region_name=constants.GLOBAL_SYSTEM_REGION)

    # Get all attributes of the constants module
    for attr in dir(constants):
        # If the attribute name ends with '_BUCKET', create an S3 bucket
        if attr.endswith("_BUCKET"):
            # Allow for the bucket name to be overridden by an environment variable
            bucket_name = os.environ.get(f"CARIBOU_OVERRIDE_{attr}", getattr(constants, attr))

            try:
                remove_bucket(s3, s3_resource, bucket_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.error("Error remove bucket %s: %s", bucket_name, e)

def remove_sync_tables_all_regions(provider: str) -> None:
    if provider == Provider.GCP.value:
        remove_sync_tables_gcp_all_regions()
    else:
        remove_sync_tables_aws_all_regions()

def remove_sync_tables_aws_all_regions() -> None:
    # First get all the regions
    all_available_regions: set[str] = set()
    try:
        available_regions_data = (
            Endpoints().get_data_collector_client().get_all_values_from_table(constants.AVAILABLE_REGIONS_TABLE)
        )
        for region_key_raw in available_regions_data.keys():
            # Keys are in forms of 'aws:eu-south-1' (For AWS regions)
            if region_key_raw.startswith("aws:"):
                region_key_aws = region_key_raw.split(":")[1]
                all_available_regions.add(region_key_aws)
    except Exception as e:  # pylint: disable=broad-except
        print(f"Error getting available regions: {e}")

    # Add the global region to the set
    all_available_regions.add(constants.GLOBAL_SYSTEM_REGION)

    sync_tables = [constants.SYNC_MESSAGES_TABLE, constants.SYNC_PREDECESSOR_COUNTER_TABLE]
    print(f"Removing sync tables in the following regions: {all_available_regions}")
    error_regions: set[str] = set()
    for region in all_available_regions:
        dynamodb = boto3.client("dynamodb", region_name=region)

        for table_name in sync_tables:
            try:
                remove_aws_table(dynamodb, table_name, verbose=False)
            except ClientError as e:
                # If not UnrecognizedClientException, log the error
                # As exception also appears if the user does not have a region enabled
                # Which means that there are no tables to remove anyways
                if e.response["Error"]["Code"] != "UnrecognizedClientException":
                    print(f"Error removing table {table_name}: {e}")
                    error_regions.add(region)
            except Exception as e:  # pylint: disable=broad-except
                print(f"Unexpected error removing table {table_name}: {e}")
                error_regions.add(region)
    if len(error_regions) > 0:
        print(f"Removed from all applicable listed regions except: {error_regions}")

def remove_sync_tables_gcp_all_regions() -> None:
    """Remove sync tables from GCP Firestore. Note: Firestore is global within a project."""
    try:
        firestore_client = firestore.Client()
    except Exception as e:
        logger.error("Failed to create Firestore client. Error: %s", e)
        return

    sync_tables = [constants.SYNC_MESSAGES_TABLE, constants.SYNC_PREDECESSOR_COUNTER_TABLE]
    logger.info("Removing sync tables: %s", sync_tables)

    error_collections: set[str] = set()

    for table_name in sync_tables:
        try:
            remove_gcp_collection(firestore_client, table_name)
        except google_api_exceptions.GoogleAPICallError as e:
            logger.error("Error removing collection %s: %s", table_name, e)
            error_collections.add(table_name)
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Unexpected error removing collection %s: %s", table_name, e)
            error_collections.add(table_name)

    if len(error_collections) > 0:
        logger.error("Failed to remove collections: %s", error_collections)
    else:
        logger.info("Successfully removed all sync tables")

def remove_gcp_collection(firestore_client: firestore.Client, collection_name: str, batch_size: int = 200) -> None:
    """
    Deletes all documents in a Firestore collection. This effectively deletes the collection.
    """
    try:
        coll_ref = firestore_client.collection(collection_name)
        docs = coll_ref.limit(batch_size).stream()
        deleted = 0

        batch = firestore_client.batch()
        for doc in docs:
            batch.delete(doc.reference)
            deleted += 1

        if deleted > 0:
            logger.info("Deleting %d documents from GCP collection '%s'...", deleted, collection_name)
            batch.commit()
            # Recurse to delete the next batch
            return remove_gcp_collection(firestore_client, collection_name, batch_size)
        else:
            logger.info("GCP Collection '%s' is now empty.", collection_name)
            return None

    except google_api_exceptions.NotFound:
        logger.info("GCP Collection '%s' not found or already empty.", collection_name)
        return None
    except Exception as e:
        logger.error("Error deleting from collection '%s': %s", collection_name, e)
        raise

def main() -> None:
    provider = os.environ.get("CARIBOU_DEFAULT_PROVIDER", Provider.AWS.value)
    # Remove any and all sync tables in all regions
    remove_sync_tables_all_regions(provider)

    # Remove the core framework tables
    teardown_framework_tables(provider)

    # Remove framework buckets
    ## This is targetting legacy buckets that are not used anymore
    ## Current iteration of the framework does not use any buckets
    teardown_framework_buckets(provider)


if __name__ == "__main__":
    main()
