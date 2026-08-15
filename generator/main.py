"""Vòng lặp: traffic nền + định kỳ tiêm gian lận có nhãn vào payment_events."""

import random
import time

from producer import kafkaEventProducer
from profiles import make_profiles
from normal import make_normal_event
from labels import LabelWriter
from scenarios import card_testing, impossible_travel, kyc_abuse

RATE = 20              # evt/s traffic nền
NUM_USERS = 500
TOPIC = "payment_events"
SCENARIO_EVERY = 10    # giây: cứ ~10s tiêm 1 gian lận ngẫu nhiên


def main() -> None:
    profiles = make_profiles(NUM_USERS)
    users = list(profiles.values())
    producer = kafkaEventProducer()
    labels = LabelWriter()
    rng = random.Random()

    scenarios = [
        lambda: card_testing.inject(producer, labels, rng),
        lambda: impossible_travel.inject(producer, labels, rng),
        lambda: kyc_abuse.inject(producer, labels, profiles, rng),
    ]

    print(f"Nền ~{RATE} evt/s + tiêm gian lận mỗi ~{SCENARIO_EVERY}s. Ctrl+C để dừng.")
    interval = 1.0 / RATE
    next_time = time.monotonic()
    next_scenario = time.monotonic() + SCENARIO_EVERY
    sent = 0
    try:
        while True:
            if time.monotonic() >= next_scenario:
                ids = rng.choice(scenarios)()          # burst chiếm ~vài giây
                print(f"💉 tiêm gian lận: {len(ids)} giao dịch")
                next_scenario = time.monotonic() + SCENARIO_EVERY
                next_time = time.monotonic()           # reset pacing sau burst
                continue

            profile = rng.choice(users)
            producer.send(TOPIC, key=profile["user_id"],
                          event=make_normal_event(profile, rng))
            sent += 1
            if sent % 100 == 0:
                print(f"... nền: {sent}")

            next_time += interval
            sleep_for = next_time - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\nĐang dừng...")
    finally:
        producer.flush()
        labels.close()
        print(f"Xong. Nền: {sent}. Nhãn tại: injected_labels.jsonl")

        
if __name__ == "__main__":
    main()