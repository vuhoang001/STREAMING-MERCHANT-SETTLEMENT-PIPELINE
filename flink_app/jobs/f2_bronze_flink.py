"""F2 — đọc payment_events, gán event-time/watermark, ghi raw (thô) xuống MinIO.

Raw = append THÔ (trùng khi job restart) -> dedup để dbt lo ở tầng intermediate.
Format: JSON-lines. Parquet là đích production; swap writer + thêm flink-parquet jar
ở slice sau khi hình dạng pipeline đã chắc.

Submit: make flink-run JOB=jobs/f2_bronze_sink.py
Verify: sau ~1 checkpoint (30s), file hiện trong bucket omnipay-bronze
        (MinIO Console localhost:9001).
"""


import json
from datetime import datetime, timezone

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.file_system import (
    FileSink, OutputFileConfig, RollingPolicy,
)

BOOTSTRAP = "redpanda:29092"          # listener INTERNAL (job chạy trong cluster)
TOPIC = "payment_events"
RAW_PATH = "s3://omnipay-bronze/payment_events"


class EventTimestampAssigner(TimestampAssigner):
    """Lấy event-time từ trường 'timestamp' (ISO 8601 UTC) -> epoch millis."""

    def extract_timestamp(self, value, record_timestamp):
        ts = json.loads(value)["timestamp"]          # '2026-08-15T09:15:03.221Z'
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

        
def main() -> None: 
    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(30_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("f2_raw")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    watermark = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))  # đến trễ ≤10s vẫn 'đúng giờ'
        .with_idleness(Duration.of_seconds(30))                 # partition im -> đừng giữ watermark
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    stream = env.from_source(source, watermark, "payment_events-source")

    sink = (
        FileSink
        .for_row_format(RAW_PATH, Encoder.simple_string_encoder("UTF-8"))
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("raw").with_part_suffix(".json").build()
        )
        .with_rolling_policy(
            RollingPolicy.default_rolling_policy(
                part_size=128 * 1024 * 1024,   # 128MB
                rollover_interval=60 * 1000,   # 1 phút
                inactivity_interval=60 * 1000,
            )
        )
        .build()
    )
    stream.sink_to(sink)

    env.execute("F2 - land payment_events to raw")


if __name__ == "__main__":
    main()
