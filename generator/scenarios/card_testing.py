# 6 txn FAILED < 20k / 90s, rồi 1 txn 15 SUCCESS, cùng devide

import random 
import time 
import uuid



from normal import now_iso_millis

FAILURE_REASONS = ['CARD_DECLINED', 'INSUFFICIENT_FUND', 'DO_NOT_HONOR']
HANOI = (21.0278, 105.8342)



def _event(device_id, user_id, amount, status, failure_reason): 
    lat, lon = HANOI

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        user_id,
        "merchant_id":    "M0099",
        "amount":         amount,
        "currency":       "VND",
        "location":       {"lat": lat, "lon": lon},
        "ip_address":     "203.0.113.7",
        "device_id":      device_id,
        "channel":        "WEB",
        "timestamp":      now_iso_millis(),
        "status":         status,
        "failure_reason": failure_reason,
        "txn_type":       "PAYMENT",
        "original_txn_id": None,
        "experiment_group": "TREATMENT",
    }



def inject(producer, labels, rng=random, topic="payment_events"):
    device_id = f"d-attacker-{uuid.uuid4().hex[:4]}"   # device chuyên dụng -> CEP sạch
    ids = []

    # A: 6 giao dịch nhỏ FAILED, cùng device, trong ~9s thực (thỏa "<2 phút")
    for _ in range(6):
        victim = f"VICTIM-{uuid.uuid4().hex[:8]}"       # 1 device thử nhiều thẻ nạn nhân
        e = _event(device_id, victim, rng.randint(5000, 19000), "FAILED",
                   rng.choice(FAILURE_REASONS))
        # key = device_id: giữ cả chuỗi trong 1 partition -> đúng thứ tự cho CEP.
        producer.send(topic, key=device_id, event=e)
        ids.append(e["transaction_id"])
        time.sleep(1.5)

    # B: 1 giao dịch lớn SUCCESS ngay sau -> thẻ sống, rút mạnh
    victim = f"VICTIM-{uuid.uuid4().hex[:8]}"
    big = _event(device_id, victim, 15_000_000, "SUCCESS", None)
    producer.send(topic, key=device_id, event=big)
    ids.append(big["transaction_id"])

    labels.write("CARD_TESTING", ids, {"device_id": device_id})
    return ids