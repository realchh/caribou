#  Installation

The following instructions will guide you through setting up the project and running a workflow.

## Prerequisites

We are working with [poetry](https://python-poetry.org) for our dependency management, so you must install it first to run any part of our Python package.

```bash
pip install poetry
```

**Note:** for Linux Users

If this does not work for you and you are using a Linux machine, you will need to install poetry with:

```bash
apt install python3-poetry
```

Alternatively, you need to reinstall poetry:

```bash
curl -sSL https://install.python-poetry.org | python3 -
poetry self-update
```

Then, install the dependencies (run this in the root directory of the project) with:

```bash
poetry install
```

Run any file with the following command:

```bash
poetry run <executable> <args>
```

### Why do we use poetry?

Poetry is a tool for dependency management and packaging in Python.
It allows you to declare the libraries your project depends on, and it will manage (install/update) them for you.

For more information, see the [poetry documentation](https://python-poetry.org/docs/).

## Install dependencies

To install the dependencies, run the following command:

```bash
poetry install
```

This will install all the dependencies required to run the framework. To check the dependencies, you can run:

```bash
poetry show
```

To open a shell with the dependencies installed, you can run:

```bash
poetry shell
```

If you opened a shell, you can run all the caribou commands without the `poetry run` prefix.

```bash
caribou --help
```

### Ensure the poetry environment is running Python 3.12+

Poetry will ensure that you are using Python 3.12 or higher based on the configuration in the `pyproject.toml` file. If you do not have the correct Python version, Poetry will notify you and you will need to install Python 3.12 or higher.

To verify your Python version manually, you can use:

```bash
python --version
```

If your version is lower than 3.12, follow the instructions for your operating system to install Python 3.12 or higher.

## AWS Account Access

To run the framework, you need an AWS account and the necessary permissions to create and manage the required resources.
In [IAM Policies](docs/iam_policies.md) we list the required permissions for any user wanting to interact with a deployed framework.

The fastest way to set up the necessary permissions is to [create a new AWS user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) under your account with the necessary permissions and use the access key and secret key to [login the AWS CLI](https://docs.aws.amazon.com/signin/latest/userguide/command-line-sign-in.html) of this user to interact with the framework.

### Setup AWS Environment

First of all, make sure to have [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) installed.
To set up the required tables in AWS required for the framework to run, you can use the following command:

```bash
poetry run caribou setup_tables
```

**Note:** The bucket that Caribou uses to store the resources (a feature for future provider compatibility) needs to be manually created.
Since AWS bucket names need to be unique, the currently configured bucket might already exist and be used by another version of the framework deployed somewhere else.
In this case, adapt the bucket name for the variable `DEPLOYMENT_RESOURCES_BUCKET` in the `caribou/common/constants.py` file.

## Docker

The Deployment Utility has an additional dependency on `docker`.
To install it, follow the instructions on the [docker website](https://docs.docker.com/engine/install/).
Ensure you have the docker daemon running before running the deployment utility.

To verify that Docker is installed correctly, you can try running:

```bash
docker --version
```

## Google Maps API Key

For the server side component, more specifically the data collectors, we use the Google Maps Geocoding API to resolve the location of the data centers.
To use this API, you need to have a Google Maps API key.
You can get one by following the instructions on the [Google Maps Platform](https://developers.google.com/maps/documentation/geocoding/get-api-key) website.

## Electricity Map API Key

For the server side component, more specifically the data collectors, we use the Electricity Map API to get the carbon intensity of the electricity in the regions.
To use this API, you need to have an Electricity Map API key.
You can get one by following the instructions on the [Electricity Map](https://api-portal.electricitymaps.com) website.

## Other dependencies

Since the AWS lambda environment restricts us from using Docker, we have to migrate the workflows using [crane](https://github.com/google/go-containerregistry/tree/main/cmd/crane). If you plan on running the framework locally instead of deploying it to the cloud, please install the crane as described in the [crane documentation](https://github.com/google/go-containerregistry/blob/main/cmd/crane/README.md).

# GCP Support
## GCP Account Access
To run the framework, you first need to setup a [GCP project](console.cloud.google.com).

Then, install the [gcloud CLI](https://cloud.google.com/sdk/docs/install#linux) using these commands:
- Download the linux archive files:
```
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz
```
- Extract the contents of the file:
```
tar -xf google-cloud-cli-linux-x86_64.tar.gz
```
- Run the gcloud CLI installation script:
```
./google-cloud-sdk/install.sh
```
- Initialize the gcloud CLI:
```
gcloud init
```

## GCP APIs that needs to be enabled for the service

- [Cloud Run](https://console.developers.google.com/apis/api/run.googleapis.com/)
- [Firestore](https://console.developers.google.com/apis/api/firestore.googleapis.com/)
- [IAM](https://console.developers.google.com/apis/api/iam.googleapis.com/)
- [Cloud Resource Manager](https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/)
- [Cloud Scheduler](https://console.developers.google.com/apis/api/cloudscheduler.googleapis.com/)
- [Cloud Billing](https://console.developers.google.com/apis/api/cloudbilling.googleapis.com/)
- [Cloud Monitoring](https://console.developers.google.com/apis/api/monitoring.googleapis.com/)
- [Cloud Logging](https://console.developers.google.com/apis/api/logging.googleapis.com/)

If gcloud CLI has been properly installed, you can run this command to enable all of the required APIs
```
gcloud services enable \
  run.googleapis.com \
  firestore.googleapis.com \
  iam.googleapis.com \
  cloudresourcemanager.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudbilling.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com
```

## Gcloud authentication for the framework
`gcloud init` only sets up the authentication for the command line. For the python scripts and framework, you need to
use a different method to log in and generate a credential file required to run our framework.
```
# Create the service account
gcloud iam service-accounts create caribou-framework-sa \
    --description="Service account for Caribou framework operations" \
    --display-name="Caribou Framework Service Account"

# Grant all necessary roles
ROLES=(
    "roles/run.admin"
    "roles/run.invoker"
    "roles/artifactregistry.admin"
    "roles/storage.admin"
    "roles/datastore.owner"
    "roles/firebase.admin"
    "roles/pubsub.admin"
    "roles/cloudscheduler.admin"
    "roles/iam.serviceAccountAdmin"
    "roles/resourcemanager.projectIamAdmin"
    "roles/iam.serviceAccountTokenCreator"
    "roles/logging.viewer"
    "roles/monitoring.viewer"
    "roles/browser"
    "roles/cloudbuild.builds.builder"
)

for role in "${ROLES[@]}"; do
    gcloud projects add-iam-policy-binding caribou-460422 \
        --member="serviceAccount:caribou-framework-sa@caribou-460422.iam.gserviceaccount.com" \
        --role="$role"
done
```
After the script has finished running, you can download the service account key to your machine to then be used for authenticating to google cloud services. Keep this key in a secure place, treat it like a password.
```
# Download the service account key
gcloud iam service-accounts keys create ~/caribou-framework-sa-key.json \
    --iam-account=caribou-framework-sa@caribou-460422.iam.gserviceaccount.com

# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/caribou-framework-sa-key.json"
```

## Default key-value store database
For GCP, we are using firestore to store our key-value pairs. Please create a default firestore database.

- Go to [firestore page](https://console.cloud.google.com/firestore/databases) in the Google Cloud Console.
- Click on "CREATE A FIRESTORE DATABASE".
- Select Native Mode.
- Use the default database id "(default)". Using this id allows you to take advantage of firestore's [free quota](https://firebase.google.com/docs/firestore/quotas#free-quota).
- Choose your default system region (e.g., us-east1) to be the database region.
- Click on "CREATE DATABASE". Your firestore database will then be successfully created. This may take a while.

To do this step, you could also use the following command. Change the region here to your preferred default system region. Here, we are using `us-east1` as our default system region.
```
gcloud firestore databases create --location=us-east1
```

## Configuring Caribou to use GCP
Set the environment variable `CARIBOU_DEFAULT_PROVIDER` to GCP.
```
export CARIBOU_DEFAULT_PROVIDER=gcp
```

There is no need to run `caribou setup_tables`. Firestore automatically generates the table when a value is provided to the table.

## Other dependencies

We are using [gcrane](https://cloud.google.com/artifact-registry/docs/docker/copy-images#gcrane-local
) to copy artifact images to another artifact registry region.

For linux users:

Download gcrane using the command: 
```
curl -L \
https://github.com/google/go-containerregistry/releases/latest/download/go-containerregistry_Linux_x86_64.tar.gz \
-o go-containerregistry.tar.gz
```
And install it using:
```
tar -zxvf go-containerregistry.tar.gz
chmod +x gcrane
sudo mv gcrane /usr/local/bin/
```
To verify that gcrane is installed, you can try running:
```
gcrane version
```