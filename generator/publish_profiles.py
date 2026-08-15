"""F3a — bơm user_profiles (§3.2) vào Kafka MỘT LẦN cho Flink broadcast enrich.

Lối tắt lab thay Debezium CDC. Profile tĩnh -> chạy một lần là đủ.
Production: nguồn thật là CDC; topic nên compacted (keyed, latest-wins) để broadcast
bootstrap lại state sau restart.

Chạy: python publish_profiles.py
"""


from producer import kafkaEventProducer
from profiles import make_profiles, to_user_profile

TOPIC = "user_profiles"
NUM_USERS = 500


def main() -> None: 
    producer = kafkaEventProducer()
    profiles = make_profiles(NUM_USERS)

    for profile in profiles.values():
        record = to_user_profile(profile)

        producer.send(topic=TOPIC, key=record["user_id"], event=record)

    remaining = producer.flush()
    print(f"Đã publish {len(profiles) - remaining}/{len(profiles)} profiles vào '{TOPIC}'")


if __name__ == "__main__":
    main()