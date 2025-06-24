import logging
import json
import uuid
import math
from datetime import datetime, timezone
import dateutil.parser
import time
import random
import gzip
from typing import Iterable, Optional, List, Dict, Any, Tuple

from gcs_utils import load_json_from_gcs

import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import parquetio, WriteToPubSub
import apache_beam.transforms.window as window

logger = logging.getLogger(__name__)

MIXPANEL_IMPORT_ENDPOINT = "https://api.mixpanel.com/import"
MP_BATCH_SIZE_EVENTS = 2000
MP_BATCH_SIZE_BYTES = 2 * 1024 * 1024


class MixpanelImporterOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_subscription", required=True)
        parser.add_argument("--source_configs_gcs_uri", required=True)
        parser.add_argument("--mixpanel_project_token", required=True)
        parser.add_argument("--mixpanel_api_secret", required=True)
        parser.add_argument("--dlq_topic_transform_errors", default=None)
        parser.add_argument("--dlq_topic_api_errors", default=None)


class RouteFileToConfig(apache_beam.DoFn):
    def setup(self):
        import logging

        self.logging = logging
        import apache_beam as beam

        self.beam = beam

    def process(self, uri: str, configs: List[Dict[str, Any]]):
        self.logging.info(f"Routing attempt for URI: '{uri}'")
        for config in configs:
            prefix = config.get("source_gcs_prefix")
            if isinstance(prefix, str) and uri.startswith(prefix):
                config_id = config.get("config_id")
                self.logging.info(
                    f"SUCCESS: Routed URI '{uri}' to config_id '{config_id}'"
                )
                self.beam.metrics.Metrics.counter(
                    "Router", f"matched_{config_id}"
                ).inc()
                yield (uri, config_id)
                return
        self.logging.warning(
            f"NO MATCH: URI '{uri}' did not match any 'source_gcs_prefix'."
        )
        self.beam.metrics.Metrics.counter("Router", "unmatched_uri").inc()


class AssociateConfigAndUnbatch(apache_beam.DoFn):
    def setup(self):
        import logging

        self.logging = logging

    def process(self, element: Tuple[str, Any], configs_by_uri: Dict[str, str]):
        filename, batch = element
        config_id = configs_by_uri.get(filename)
        if not config_id:
            self.logging.warning(
                f"Could not find a config_id for file '{filename}' in side input."
            )
            return
        for row_dict in batch.to_pylist():
            yield (config_id, row_dict)


class MapToMixpanelEvent(apache_beam.DoFn):
    DLQ_TRANSFORMATION_ERROR = "dlq_transformation_error"

    def __init__(self, project_token: str):
        self.project_token = project_token

    def setup(self):
        import logging

        self.logging = logging
        import apache_beam as beam

        self.beam = beam
        import json

        self.json = json
        import uuid

        self.uuid = uuid
        import math

        self.math = math
        from datetime import datetime, timezone

        self.datetime, self.timezone = datetime, timezone
        import dateutil.parser

        self.dateutil_parser = dateutil.parser

    def _clean_nan_value(self, v: Any) -> Optional[Any]:
        return None if isinstance(v, float) and self.math.isnan(v) else v

    def _clean_nan_dict(self, obj: Dict[str, Any]) -> Dict[str, Optional[Any]]:
        if not isinstance(obj, dict):
            return {}
        return {k: self._clean_nan_value(v) for k, v in obj.items()}

    def _to_str(self, val: Any) -> Optional[str]:
        cleaned_val = self._clean_nan_value(val)
        return None if cleaned_val is None else str(cleaned_val)

    def _strip_none(self, d: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in d.items() if v is not None}

    def _now_unix(self) -> int:
        return int(self.datetime.now(self.timezone.utc).timestamp())

    def _parse_timestamp(self, ts_val: Any) -> Optional[int]:
        if ts_val is None:
            return None
        cleaned_val = self._clean_nan_value(ts_val)
        if cleaned_val is None:
            return None
        if isinstance(cleaned_val, self.datetime):
            dt = cleaned_val
        elif isinstance(cleaned_val, (int, float)):
            return int(cleaned_val)
        else:
            try:
                dt = self.dateutil_parser.parse(str(cleaned_val))
            except (ValueError, TypeError, OverflowError, dateutil.parser.ParserError):
                self.beam.metrics.Metrics.counter("TimestampParse", "error_parse").inc()
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self.timezone.utc)
        return int(dt.timestamp())

    def _dlq(
        self, error_type: str, context: Dict[str, Any], original_row: Dict[str, Any]
    ) -> apache_beam.pvalue.TaggedOutput:
        payload = {
            "error_type": error_type,
            "context": context,
            "original_row": original_row,
        }
        json_string = self.json.dumps(payload, default=str)
        return self.beam.pvalue.TaggedOutput(
            self.DLQ_TRANSFORMATION_ERROR, json_string.encode("utf-8")
        )

    def process(self, element: Tuple[str, Dict[str, Any]], configs_map: Dict[str, Any]):
        config_id, source_row_original = element
        config = configs_map.get(config_id)
        if not config:
            self.beam.metrics.Metrics.counter(
                "MapEvent", f"config_not_found_{config_id}"
            ).inc()
            return
        try:
            if not isinstance(source_row_original, dict):
                self.logging.error(
                    f"Invalid data type. Expected dict, got {type(source_row_original)}."
                )
                yield self._dlq(
                    "invalid_row_type",
                    {"config_id": config_id},
                    {"data": str(source_row_original)},
                )
                return
            source_row = self._clean_nan_dict(source_row_original)
            event_name_field = config.get("mixpanel_event_name_from_field")
            if event_name_field:
                event_name = source_row.get(event_name_field)
                if not event_name:
                    yield self._dlq(
                        "missing_dynamic_event_name",
                        {"config_id": config_id},
                        source_row_original,
                    )
                    return
            else:
                event_name = config.get("mixpanel_event_name", "generic_event")
            properties: Dict[str, Any] = {"token": self.project_token}
            mapped_source_fields: set[str] = set()
            field_mappings = config.get("field_mappings", [])
            wildcard_passthrough = any(
                fm.get("source_field") == "*" for fm in field_mappings
            )
            for fm in field_mappings:
                source_field, mixpanel_field = fm.get("source_field"), fm.get(
                    "mixpanel_field"
                )
                if source_field == "*":
                    continue
                value = source_row.get(source_field)
                mapped_source_fields.add(source_field)
                if value is None:
                    if fm.get("is_required_in_source", False):
                        yield self._dlq(
                            "missing_required_field",
                            {"config_id": config_id, "source_field": source_field},
                            source_row_original,
                        )
                        return
                    if not fm.get("include_if_none", False) and mixpanel_field not in [
                        "$user_id",
                        "$device_id",
                        "$insert_id",
                    ]:
                        continue
                field_type = fm.get("type", "passthrough")
                if field_type == "string":
                    properties[mixpanel_field] = self._to_str(value)
                elif field_type == "integer":
                    try:
                        properties[mixpanel_field] = (
                            int(value) if value is not None else None
                        )
                    except (ValueError, TypeError):
                        continue
                elif field_type == "float":
                    try:
                        properties[mixpanel_field] = (
                            float(value) if value is not None else None
                        )
                    except (ValueError, TypeError):
                        continue
                elif field_type == "boolean":
                    if isinstance(value, str):
                        properties[mixpanel_field] = value.lower() in [
                            "true",
                            "1",
                            "t",
                            "y",
                            "yes",
                        ]
                    else:
                        properties[mixpanel_field] = (
                            bool(value) if value is not None else None
                        )
                elif field_type == "unix_timestamp_auto":
                    ts = self._parse_timestamp(value)
                    if ts is not None:
                        properties[mixpanel_field] = ts
                    elif mixpanel_field == "time":
                        properties[mixpanel_field] = self._now_unix()
                elif field_type == "string_or_uuid" and mixpanel_field == "$insert_id":
                    properties[mixpanel_field] = self._to_str(value) or str(
                        self.uuid.uuid4()
                    )
                else:
                    properties[mixpanel_field] = value
            if wildcard_passthrough:
                for key, val in source_row.items():
                    if key not in mapped_source_fields:
                        properties[key] = val
            if "time" not in properties:
                properties["time"] = self._now_unix()
            if "$insert_id" not in properties:
                properties["$insert_id"] = str(self.uuid.uuid4())
            if not (properties.get("$user_id") or properties.get("$device_id")):
                self.beam.metrics.Metrics.counter(
                    config_id, "missing_distinct_id"
                ).inc()
            final_properties = self._strip_none(properties)
            mixpanel_event_json = self.json.dumps(
                {"event": event_name, "properties": final_properties},
                default=str,
                allow_nan=False,
            )
            yield mixpanel_event_json
        except Exception as e:
            self.logging.error(
                f"Critical error mapping event for {config_id}: {e}", exc_info=True
            )
            yield self._dlq(
                "critical_transformation_error",
                {"config_id": config_id, "exception": str(e)},
                source_row_original,
            )


class AddRandomKey(apache_beam.DoFn):
    def setup(self):
        import random

        self.random = random

    def process(self, element):
        yield (self.random.randint(0, 4999), element)


class BatchPostToMixpanel(apache_beam.DoFn):
    DLQ_FAILED_BATCH = "dlq_failed_mixpanel_batch"

    def __init__(
        self,
        mixpanel_api_secret_value: str,
        batch_size_events: int,
        batch_size_bytes: int,
    ):
        self.mixpanel_api_secret_value = mixpanel_api_secret_value
        self.batch_size_events = batch_size_events
        self.batch_size_bytes = batch_size_bytes

    def setup(self):
        import requests

        self.session = requests.Session()
        import apache_beam as beam

        self.beam = beam
        import json

        self.json = json
        import gzip

        self.gzip = gzip
        import time

        self.time = time
        import random

        self.random = random
        from apache_beam.utils.windowed_value import WindowedValue

        self.WindowedValue = WindowedValue
        # FIX: Import the correct class, IntervalWindow
        from apache_beam.transforms.window import IntervalWindow

        self.IntervalWindow = IntervalWindow

    def start_bundle(self):
        self.event_buffer: List[str] = []
        self.current_buffer_size_bytes: int = 0

    def _flush_buffer(self) -> Iterable[apache_beam.pvalue.TaggedOutput]:
        if not self.event_buffer:
            return
        payload_str = "\n".join(self.event_buffer)
        payload_compressed = self.gzip.compress(payload_str.encode("utf-8"))
        retries, max_retries, batch_copy = 0, 5, list(self.event_buffer)
        while retries < max_retries:
            try:
                response = self.session.post(
                    MIXPANEL_IMPORT_ENDPOINT,
                    params={"strict": "1"},
                    headers={
                        "Content-Type": "application/x-ndjson",
                        "Content-Encoding": "gzip",
                    },
                    auth=(self.mixpanel_api_secret_value, ""),
                    data=payload_compressed,
                    timeout=90,
                )
                if response.status_code == 200:
                    self.beam.metrics.Metrics.counter(
                        "MixpanelAPI", "events_sent_successfully"
                    ).inc(len(self.event_buffer))
                    break
                elif response.status_code == 429 or response.status_code >= 500:
                    retries += 1
                    self.time.sleep(min((2**retries) + self.random.uniform(0, 1), 60))
                else:
                    for event_str in batch_copy:
                        dlq_payload = self.json.dumps(
                            {
                                "reason": f"client_error_{response.status_code}",
                                "response": response.text[:500],
                                "payload": event_str,
                            }
                        )
                        yield self.beam.pvalue.TaggedOutput(
                            self.DLQ_FAILED_BATCH, dlq_payload.encode("utf-8")
                        )
                    break
            except Exception:
                retries += 1
                self.time.sleep(min((2**retries) + self.random.uniform(0, 1), 60))
        if retries == max_retries:
            for event_str in batch_copy:
                dlq_payload = self.json.dumps(
                    {"reason": "max_retries_reached", "payload": event_str}
                )
                yield self.beam.pvalue.TaggedOutput(
                    self.DLQ_FAILED_BATCH, dlq_payload.encode("utf-8")
                )
        self.event_buffer.clear()
        self.current_buffer_size_bytes = 0

    def process(self, element: Tuple[int, Iterable[str]]):
        _key, events = element
        for event_str in events:
            self.event_buffer.append(event_str)
            self.current_buffer_size_bytes += len(event_str.encode("utf-8"))
            if (
                len(self.event_buffer) >= self.batch_size_events
                or self.current_buffer_size_bytes >= self.batch_size_bytes
            ):
                yield from self._flush_buffer()

    def finish_bundle(self):
        window_size = 10
        for item in self._flush_buffer():
            ts = self.time.time()
            window_start = ts - (ts % window_size)
            correct_window = self.IntervalWindow(
                start=window_start, end=window_start + window_size
            )
            yield self.WindowedValue(value=item, timestamp=ts, windows=[correct_window])


def run(argv: Optional[List[str]] = None):
    pipeline_options = PipelineOptions(argv, streaming=True, save_main_session=False)
    custom_options = pipeline_options.view_as(MixpanelImporterOptions)

    try:
        source_configs = load_json_from_gcs(custom_options.source_configs_gcs_uri)
        configs_map = {c["config_id"]: c for c in source_configs}
    except Exception as e:
        logging.critical(
            f"CRITICAL: Failed to load source configurations: {e}", exc_info=True
        )
        return

    with apache_beam.Pipeline(options=pipeline_options) as p:
        configs_map_side_input = apache_beam.pvalue.AsDict(
            p | "CreateConfigsMap" >> apache_beam.Create(configs_map.items())
        )

        gcs_uris = (
            p
            | "ReadGCSNotifications"
            >> apache_beam.io.ReadFromPubSub(
                subscription=custom_options.input_subscription, with_attributes=True
            )
            | "ExtractGCSUri"
            >> apache_beam.Map(
                lambda msg: (
                    f"gs://{msg.attributes['bucketId']}/{msg.attributes['objectId']}"
                    if msg.attributes
                    and "bucketId" in msg.attributes
                    and "objectId" in msg.attributes
                    else None
                )
            )
            | "FilterOutNoneURIs" >> apache_beam.Filter(lambda uri: uri is not None)
            | "WindowForDeduplication"
            >> apache_beam.WindowInto(window.FixedWindows(300))
            | "DeduplicateURIs" >> apache_beam.Distinct()
        )

        configs_by_uri_side_input = apache_beam.pvalue.AsDict(
            gcs_uris
            | "RouteFiles"
            >> apache_beam.ParDo(
                RouteFileToConfig(),
                configs=apache_beam.pvalue.AsList(
                    p | "CreateConfigsList" >> apache_beam.Create(source_configs)
                ),
            )
            | "FilterParquet"
            >> apache_beam.Filter(
                lambda x: configs_map.get(x[1], {}).get("file_type", "").upper()
                == "PARQUET"
            )
        )

        record_batches = (
            gcs_uris
            | "ReadAllParquet"
            >> parquetio.ReadAllFromParquetBatched(with_filename=True)
        )

        rows = record_batches | "AssociateAndUnbatch" >> apache_beam.ParDo(
            AssociateConfigAndUnbatch(), configs_by_uri=configs_by_uri_side_input
        )

        map_results = rows | "MapEvents" >> apache_beam.ParDo(
            MapToMixpanelEvent(custom_options.mixpanel_project_token),
            configs_map=configs_map_side_input,
        ).with_outputs(MapToMixpanelEvent.DLQ_TRANSFORMATION_ERROR, main="main")

        if custom_options.dlq_topic_transform_errors:
            (
                map_results[MapToMixpanelEvent.DLQ_TRANSFORMATION_ERROR]
                | "DLQ_Transform"
                >> WriteToPubSub(custom_options.dlq_topic_transform_errors)
            )

        post_results = (
            map_results.main
            | "WindowForApiBatching" >> apache_beam.WindowInto(window.FixedWindows(10))
            | "AddRandomKeyForApi" >> apache_beam.ParDo(AddRandomKey())
            | "GroupForApiBatching" >> apache_beam.GroupByKey()
            | "PostToMixpanel"
            >> apache_beam.ParDo(
                BatchPostToMixpanel(
                    custom_options.mixpanel_api_secret,
                    batch_size_events=MP_BATCH_SIZE_EVENTS,
                    batch_size_bytes=MP_BATCH_SIZE_BYTES,
                )
            ).with_outputs(BatchPostToMixpanel.DLQ_FAILED_BATCH, main="ok")
        )

        if custom_options.dlq_topic_api_errors:
            (
                post_results[BatchPostToMixpanel.DLQ_FAILED_BATCH]
                | "DLQ_API" >> WriteToPubSub(custom_options.dlq_topic_api_errors)
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
