# GCS Parquet to Mixpanel Dataflow Importer

This project provides a robust, production-ready solution for ingesting Parquet files from Google Cloud Storage (GCS), transforming them based on a dynamic JSON configuration, and sending the events to Mixpanel.

It includes two distinct, professional-grade Dataflow pipelines:
1.  **Streaming Pipeline (Flex Template):** Runs continuously to process new files as they arrive, triggered by GCS notifications via Pub/Sub.
2.  **Batch Backfill Pipeline:** A high-throughput, idempotent (restartable) batch job designed to process large volumes of historical files without creating duplicates.

## Features

- **Dynamic Configuration:** Transformation rules are defined in a JSON file in GCS, allowing for changes without redeploying the pipeline.
- **Dual-Mode Operation:** Separate, optimized pipelines for low-latency streaming and high-throughput batch backfills.
- **Idempotent Backfills:** The batch pipeline uses Firestore to track processed files, ensuring that a failed job can be safely restarted without causing data duplication.
- **Robust Error Handling:** Failed records from both transformation and API submission are routed to Dead-Letter Queues (DLQ) in Pub/Sub for analysis.
- **Memory-Safe Processing:** The batch pipeline is designed to handle arbitrarily large Parquet files (even those with single, massive row groups) without causing out-of-memory errors on workers.
- **Scalable Architecture:** Built on modern Dataflow patterns using self-contained `DoFn`s and efficient side inputs.

## Project Structure

```
.
├── main.py                   # Main entry point for the Streaming Flex Template pipeline.
├── main_batch.py             # Main entry point for the Batch Backfill pipeline.
├── gcs_utils.py              # Utility for loading the JSON config from GCS.
├── sources.json              # Example configuration file for mapping rules.
├── requirements.txt          # Python dependencies for the project.
├── setup.py                  # Setup file for packaging the pipeline for Dataflow.
├── mixpanel_importer_flex_template.json  # Metadata for the streaming Flex Template.
└── .gitignore                # Specifies files for git to ignore.
```

## Prerequisites

Before running this pipeline, you must have the following set up in your Google Cloud Project:

1.  **gcloud CLI & Docker:** The Google Cloud SDK and Docker must be installed and authenticated on your local machine.
2.  **IAM Permissions:** The service account used to run the Dataflow job (e.g., `mixpanel-import-sa@...`) needs the following IAM roles:
    - `Dataflow Worker`
    - `Pub/Sub Subscriber` (to read GCS notifications)
    - `Pub/Sub Publisher` (to write to DLQ topics)
    - `Storage Object Viewer` (to read Parquet files and configs)
    - `Cloud Datastore User` (to read/write state to Firestore)
3.  **Pub/Sub Topic & Subscription:**
    - A Pub/Sub topic that receives notifications from your GCS bucket.
    - A Pub/Sub subscription to that topic, which your streaming pipeline will read from. [Follow this guide](https://cloud.google.com/storage/docs/pubsub-notifications) to set it up.
4.  **Firestore Database:**
    - A Firestore database in **Native Mode**. If you haven't created one, you will be prompted to do so in the GCP console.
    - A collection named `dataflow_processed_files` to track the state of the batch backfill job.
5.  **GCS Bucket Structure:** A GCS bucket to hold your source data, configurations, and Dataflow staging files.

## Configuration (`sources.json`)

This file is the brain of the pipeline. It defines how to map data from your source Parquet files to Mixpanel events.

```json
[
  {
    "config_id": "hof_event_game_spin",
    "source_gcs_prefix": "gs://playtika-mixpanel-dev/house_of_fun/event_game_spin/",
    "file_type": "PARQUET",
    "mixpanel_event_name_from_field": "event_name",
    "field_mappings": [
      { "source_field": "bussiness_ts", "mixpanel_field": "time", "type": "unix_timestamp_auto" },
      { "source_field": "user_id", "mixpanel_field": "$user_id", "type": "string" },
      { "source_field": "did", "mixpanel_field": "$device_id", "type": "string" },
      { "source_field": "insert_id", "mixpanel_field": "$insert_id", "type": "string_or_uuid" },
      { "source_field": "*", "mixpanel_field": "*" }
    ]
  }
]
```

- **`config_id`**: A unique name for this configuration block.
- **`source_gcs_prefix`**: The pipeline will apply these rules to any file whose GCS path starts with this prefix.
- **`mixpanel_event_name_from_field`**: The name of the column in your Parquet file to use as the Mixpanel event name.
- **`field_mappings`**: An array of mapping rules:
    - **`source_field`**: The exact column name from your Parquet file.
    - **`mixpanel_field`**: The desired property name in Mixpanel. **Important:** Use the `$` prefix for special Mixpanel properties like `$user_id`, `$device_id`, and `$insert_id`.
    - **`type`**: The data type conversion to apply.
    - **`"source_field": "*"`**: A special rule that passes all unmapped columns from the source file directly into the Mixpanel event as custom properties.

## How to Run

### 1. Streaming Pipeline (for New Files)

This pipeline runs forever and processes new files as they arrive. It is deployed as a Dataflow Flex Template.

**Build and Deploy Script (`build_and_deploy_streaming.sh`)**
```bash
#!/bin/bash
set -e

# --- Configuration ---
export VERSION="1.0.0" # Change this for new builds
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export IMAGE_URI="us-central1-docker.pkg.dev/${PROJECT_ID}/dataflow-flex-templates/mixpanel-gcs-importer:${VERSION}"
export TEMPLATE_GCS_PATH="gs://your-staging-bucket/templates/mixpanel-gcs-importer-flex-${VERSION}.json"
export LATEST_TEMPLATE_PATH="gs://your-staging-bucket/templates/mixpanel-gcs-importer-flex-latest.json"

# --- Build and Push Docker Image ---
echo "Building Docker image..."
gcloud builds submit . --tag "${IMAGE_URI}" --project="${PROJECT_ID}"

# --- Build Flex Template ---
echo "Building Flex Template..."
gcloud dataflow flex-template build "${TEMPLATE_GCS_PATH}" \
    --image "${IMAGE_URI}" \
    --sdk-language PYTHON \
    --metadata-file mixpanel_importer_flex_template.json \
    --project="${PROJECT_ID}"

# --- Copy to 'latest' for easy access ---
echo "Updating latest template path..."
gsutil cp "${TEMPLATE_GCS_PATH}" "${LATEST_TEMPLATE_PATH}"

echo "Build and deploy complete. You can now run the job from the GCP Console or using the run command."
```

**Run the Streaming Job**
```bash
# --- Job Run Configuration ---
export JOB_NAME="mixpanel-streaming-importer-$(date +%Y%m%d-%H%M%S)"
export CONFIG_GCS_PATH="gs://your-config-bucket/configs/sources.json"
export WORKER_SA="your-dataflow-service-account@your-gcp-project-id.iam.gserviceaccount.com"
export MIXPANEL_TOKEN="your_mixpanel_project_token"
export MIXPANEL_SECRET="your_mixpanel_api_secret"
export INPUT_SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/your-gcs-notification-sub"
export DLQ_TRANSFORM_TOPIC="projects/${PROJECT_ID}/topics/your-transform-dlq-topic"
export DLQ_API_TOPIC="projects/${PROJECT_ID}/topics/your-api-dlq-topic"

# --- Run Command ---
gcloud dataflow flex-template run "${JOB_NAME}" \
  --template-file-gcs-location "${LATEST_TEMPLATE_PATH}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --enable-streaming-engine \
  --service-account-email "${WORKER_SA}" \
  --worker-machine-type "n2-standard-2" \
  --parameters \
"input_subscription=${INPUT_SUBSCRIPTION},source_configs_gcs_uri=${CONFIG_GCS_PATH},mixpanel_project_token=${MIXPANEL_TOKEN},mixpanel_api_secret=${MIXPANEL_SECRET},dlq_topic_transform_errors=${DLQ_TRANSFORM_TOPIC},dlq_topic_api_errors=${DLQ_API_TOPIC}"
```

### 2. Batch Backfill Pipeline (for Historical Files)

This pipeline runs once to process a large number of existing files and then shuts down. It is a classic Dataflow batch job.

**Run the Batch Job Script (`run_backfill.sh`)**
```bash
#!/bin/bash
set -e

# --- Configuration for the Batch Job ---
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export JOB_NAME="mixpanel-backfill-$(date +%Y%m%d-%H%M%S)"

# ** This is the most important part **
# Use a GCS wildcard to match ALL parquet files in the bucket.
# The double-star (**) is a recursive wildcard.
export INPUT_PATTERN="gs://your-data-bucket/**/*.parquet"

# Use the same configuration as your streaming job
export CONFIG_GCS_PATH="gs://your-config-bucket/configs/sources.json"
export WORKER_SA="your-dataflow-service-account@your-gcp-project-id.iam.gserviceaccount.com"
export MIXPANEL_TOKEN="your_mixpanel_project_token"
export MIXPANEL_SECRET="your_mixpanel_api_secret"
export DLQ_TRANSFORM_TOPIC="projects/${PROJECT_ID}/topics/your-transform-dlq-topic"
export DLQ_API_TOPIC="projects/${PROJECT_ID}/topics/your-api-dlq-topic"
export STAGING_LOCATION="gs://your-staging-bucket/staging"
export TEMP_LOCATION="gs://your-staging-bucket/temp"

# --- The gcloud Command ---
python3 -m main_batch \
  --runner DataflowRunner \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --job_name "${JOB_NAME}" \
  --staging_location "${STAGING_LOCATION}" \
  --temp_location "${TEMP_LOCATION}" \
  --service_account_email "${WORKER_SA}" \
  --worker_machine_type "n2-highmem-4" \
  --max_num_workers 250 \
  --input_gcs_pattern "${INPUT_PATTERN}" \
  --source_configs_gcs_uri "${CONFIG_GCS_PATH}" \
  --mixpanel_project_token "${MIXPANEL_TOKEN}" \
  --mixpanel_api_secret "${MIXPANEL_SECRET}" \
  --dlq_topic_transform_errors "${DLQ_TRANSFORM_TOPIC}" \
  --dlq_topic_api_errors "${DLQ_API_TOPIC}" \
  --setup_file ./setup.py
```

### Performance & Scaling

- **Batch Job:** For the backfill, performance is key.
    - `--worker-machine-type "n2-highmem-4"`: Use high-memory machines to prevent out-of-memory errors when processing very large Parquet files.
    - `--max_num_workers 250`: Allow Dataflow to scale up aggressively to process many files in parallel. Adjust this based on your budget and urgency.
- **Streaming Job:** For the streaming pipeline, latency and stability are key.
    - `--worker-machine-type "n2-standard-2"` is usually sufficient for processing single files as they arrive.
    - `--enable-streaming-engine` is highly recommended for lower cost and better performance in streaming jobs.