import json
from google.cloud import storage
import logging
from typing import Dict, List, Union

logger = logging.getLogger(__name__)

_GCS_CLIENT = None


def get_gcs_client() -> storage.Client:
    global _GCS_CLIENT
    if _GCS_CLIENT is None:
        _GCS_CLIENT = storage.Client()
    return _GCS_CLIENT


def load_json_from_gcs(gcs_uri: str) -> Union[Dict, List]:
    try:
        client = get_gcs_client()
        if not gcs_uri.startswith("gs://"):
            logger.error(f"Invalid GCS URI: {gcs_uri}. Must start with gs://")
            raise ValueError(f"Invalid GCS URI: {gcs_uri}. Must start with gs://")

        parts = gcs_uri.replace("gs://", "").split("/", 1)
        if len(parts) < 2:
            logger.error(
                f"Invalid GCS URI format: {gcs_uri}. Expected gs://bucket/object_path"
            )
            raise ValueError(
                f"Invalid GCS URI format: {gcs_uri}. Expected gs://bucket/object_path"
            )

        bucket_name, blob_name = parts
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            logger.error(f"Configuration file not found at {gcs_uri}")
            raise FileNotFoundError(f"Configuration file not found at {gcs_uri}")

        return json.loads(blob.download_as_string())
    except Exception as e:
        logger.error(f"Failed to load JSON from GCS URI {gcs_uri}: {e}", exc_info=True)
        raise
