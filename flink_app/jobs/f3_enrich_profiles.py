"""F3 — enrich payment_events với user_profiles qua broadcast state.

Đọc user_profiles (broadcast vào mọi worker) -> mỗi giao dịch tra profile theo user_id
-> gắn tier/kyc/risk. Không thấy profile -> fail-open + cờ profile_missing (docs/01 §1).

Submit: make flink-run JOB=jobs/f3_enrich_profiles.py
Xem:    make logs-flink   (output ở TaskManager, không phải terminal)
"""

import json 
from datetime import datetime, timezone

from pyflink.common import Duration, WatermarkStrategy, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.functions import KeyedBroadcastProcessFunction

PROFILE_STATE = MapStateDescriptor("user_profiles", Types.STRING(), Types.STRING())
BOOTSTRAP = "redpanda:29092"

# Danh bạ boardcast: user_id -> profiles JSON. Định nghĩa MỘT LẦN, dùng ở cả 2 nơi
# (.boardcast() và trong hàm) - phải cùng một descirptor
class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        ts = json.loads(value)["timestamp"]
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)


class EnrichFunction(KeyedBroadcastProcessFunction):

    def process_broadcast_element(self, value, ctx):
        # CỬA GHI: value là 1 profile JSON. Ghi vào danh bạ (được phép ghi ở đây).
        profile = json.loads(value)
        ctx.get_broadcast_state(PROFILE_STATE).put(profile["user_id"], value)

    def process_element(self, value, ctx):
        # CỬA ĐỌC: value là 1 giao dịch JSON. Tra danh bạ (read-only ở đây).
        txn = json.loads(value)
        profile_json = ctx.get_broadcast_state(PROFILE_STATE).get(txn["user_id"])

        if profile_json is not None:
            p = json.loads(profile_json)
            txn["account_tier"] = p["account_tier"]
            txn["kyc_status"] = p["kyc_status"]
            txn["risk_score_baseline"] = p["risk_score_baseline"]
            txn["profile_missing"] = False
        else:
            # fail-open: KHÔNG vứt, vẫn phát + gắn cờ để sau đếm cái giá
            txn["account_tier"] = None
            txn["kyc_status"] = None
            txn["risk_score_baseline"] = None
            txn["profile_missing"] = True

        yield json.dumps(txn)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()

    payment_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics("payment_events")
        .set_group_id("f3b-payments")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    profile_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics("user_profiles")
        .set_group_id("f3b-profiles")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    payment_wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_idleness(Duration.of_seconds(30))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    # Giao dịch: key theo user_id (cần cho F4 sau — impossible travel state theo user)
    payments = (
        env.from_source(payment_source, payment_wm, "payment_events-source")
        .key_by(lambda v: json.loads(v)["user_id"], key_type=Types.STRING())
    )
    # Profile: broadcast vào mọi worker
    profiles = env.from_source(
        profile_source, WatermarkStrategy.no_watermarks(), "user_profiles-source"
    )
    broadcast = profiles.broadcast(PROFILE_STATE)

    enriched = payments.connect(broadcast).process(
        EnrichFunction(), output_type=Types.STRING()
    )
    enriched.print()

    env.execute("F3 - enrich payment_events with user_profiles")


if __name__ == "__main__":
    main()