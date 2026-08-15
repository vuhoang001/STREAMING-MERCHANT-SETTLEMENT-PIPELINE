"""KYC abuse: user UNVERIFIED tiêu số lớn (~3M).

LƯU Ý: kyc_status KHÔNG nằm trong payment_events (đúng contract §3.1) — nó đến từ
user_profiles qua enrichment. Nên Flink chỉ BẮT được khi đã có user_profiles trong
Kafka (lát publish profiles / Debezium, chưa làm). Giờ vẫn tiêm + ghi nhãn để sẵn.
"""

import random
import uuid

from normal import now_iso_millis


def _event(user_id, amount):
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        user_id,
        "merchant_id":    "M0007",
        "amount":         amount,
        "currency":       "VND",
        "location":       {"lat": 21.0278, "lon": 105.8342},
        "ip_address":     "192.0.2.55",
        "device_id":      f"d-{user_id}",
        "channel":        "WEB",
        "timestamp":      now_iso_millis(),
        "status":         "SUCCESS",
        "failure_reason": None,
        "txn_type":       "PAYMENT",
        "original_txn_id": None,
        "experiment_group": "TREATMENT",
    }


def inject(producer, labels, profiles, rng=random, topic="payment_events"):
    # Dùng user UNVERIFIED CÓ THẬT trong tập profile -> khớp khi enrichment bật.
    unverified = [p for p in profiles.values() if p["kyc_status"] == "UNVERIFIED"]
    profile = rng.choice(unverified) if unverified else rng.choice(list(profiles.values()))

    e = _event(profile["user_id"], 3_000_000)
    producer.send(topic, key=e["user_id"], event=e)
    labels.write("KYC_ABUSE", [e["transaction_id"]],
                 {"user_id": profile["user_id"], "kyc_status": profile["kyc_status"]})
    return [e["transaction_id"]]