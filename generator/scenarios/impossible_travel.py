"""Impossible travel: cùng user, HN -> HCM (~1150km) trong vài giây thực (<10 phút)."""

import random
import time
import uuid

from normal import now_iso_millis

HANOI = (21.0278, 105.8342)
HCM = (10.7769, 106.7009)


def _event(user_id, lat, lon):
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        user_id,
        "merchant_id":    "M0050",
        "amount":         500000,
        "currency":       "VND",
        "location":       {"lat": lat, "lon": lon},
        "ip_address":     "198.51.100.22",
        "device_id":      f"d-{user_id}",
        "channel":        "APP",
        "timestamp":      now_iso_millis(),
        "status":         "SUCCESS",
        "failure_reason": None,
        "txn_type":       "PAYMENT",
        "original_txn_id": None,
        "experiment_group": "TREATMENT",
    }


def inject(producer, labels, rng=random, topic="payment_events"):
    user_id = f"TRAVELER-{uuid.uuid4().hex[:8]}"   # user chuyên dụng -> không bị nền chen
    ids = []

    e1 = _event(user_id, *HANOI)
    producer.send(topic, key=user_id, event=e1)     # key = user_id (Flink key theo user)
    ids.append(e1["transaction_id"])

    time.sleep(3)                                    # vài giây << 10 phút

    e2 = _event(user_id, *HCM)
    producer.send(topic, key=user_id, event=e2)
    ids.append(e2["transaction_id"])

    labels.write("IMPOSSIBLE_TRAVEL", ids, {"user_id": user_id, "approx_km": 1150})
    return ids