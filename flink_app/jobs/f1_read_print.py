"""F1 — job PyFlink tối giản: đọc payment_events từ Kafka và in ra.

Mục tiêu duy nhất: chứng minh PyFlink đọc được Kafka + submit chạy được trong cluster.
Chưa parse JSON, chưa watermark, chưa Bronze — những thứ đó là F2+.

Submit:
    make flink-run JOB=jobs/f1_read_print.py
Xem output (KHÔNG hiện ở terminal của bạn — nằm trong log TaskManager):
    make logs-flink
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema


BOOTSTRAP = "redpanda:29092"
TOPIC = "payment_events"


BOOTSTRAP = "redpanda:29092"
TOPIC = "payment_events"


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("f1-read-print")
        # earliest: đọc cả data đã có sẵn trong topic -> thấy kết quả ngay.
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # F1 giữ value ở dạng chuỗi JSON thô. Parse thành field là F2.
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),   # F1 chưa cần watermark
        "payment_events-source",
    )

    stream.print()   # -> stdout TaskManager (xem bằng make logs-flink)

    env.execute("F1 - read payment_events and print")


if __name__ == "__main__":
    main()