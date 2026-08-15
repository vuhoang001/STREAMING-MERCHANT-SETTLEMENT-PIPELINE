
"""_summary_
    Lát 1 - Gửi ĐÚNG 1 giao dịch normal vào topic payment_events, rồi xác nhận  giao hàng. 

    Mục tieeu duy nhất: chứng minh đường ống Python -> Kafka thông và schema đúng contract. 
    Chưa random, chưa trafic nền, chưa gian lận - những thứ đó là Lát 2+.
    
    Chạy: 
        cd generator 
        source .venv/bin/activate
        pip install -r requirements.txt 
        python send_one.py 
    Rồi mở http://localhost:8080 -> topic payment_events -> thất 1 message.
"""


import json     
import sys
import uuid 
from datetime import datetime, timezone

from confluent_kafka import Producer

# -- Cấu hình kết nối
# ----------------------------------
# Generator chạy trên HOST -> dùng listencer EXTERENAL của Redpanda = localhost:9092.
# KHÔNG dùng redpanda:29092 (đó là listener internal, chỉ gọi được giữa các container).

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "payment_events"

def now_iso_millis() -> str: 
    """Event time dạng ISO 8601 UTC, mili-giây, kết thúc bằng 'Z'"""

    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    
def build_normal_event() -> dict:
    """Một giao dịch 'sạch' hardcode, đủ mọi trường của contract §3.1."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        "U000123",
        "merchant_id":    "M0042",
        "amount":         350000,                       # VND, số nguyên
        "currency":       "VND",
        "location":       {"lat": 21.0278, "lon": 105.8342},   # Hà Nội
        "ip_address":     "115.78.10.20",               # PII -> mask ở Silver, không xử lý ở đây
        "device_id":      "d-9f2a1c",                   # PII -> hash ở Silver
        "channel":        "APP",
        "timestamp":      now_iso_millis(),             # event time

        # ── nhóm bổ sung bắt buộc (README §3.1) ──
        "status":           "SUCCESS",
        "failure_reason":   None,
        "txn_type":         "PAYMENT",
        "original_txn_id":  None,
        "experiment_group": "TREATMENT",
    }

def on_delivery(err, msg):
    """Delivery callback: xác nhận message THẬT SỰ tới Kafka, không 'bắn rồi quên'."""
    if err is not None:
        print(f"❌ Giao hàng THẤT BẠI: {err}", file=sys.stderr)
    else:
        print(f"✅ Đã tới {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    event = build_normal_event()
    key = event["user_id"]          # key = user_id -> cùng user vào cùng partition

    # produce() cần key/value là bytes, không phải dict/str.
    producer.produce(
        topic=TOPIC,
        key=key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=on_delivery,
    )

    # flush() BẮT BUỘC: produce() chỉ đẩy vào buffer. Thoát trước khi flush = message không tới.
    # flush() trả về số message còn tồn trong buffer; 0 nghĩa là đã đẩy hết.
    remaining = producer.flush(timeout=10)
    if remaining > 0:
        print(f"⚠️  Còn {remaining} message chưa gửi được (broker sai địa chỉ? chưa chạy?).",
              file=sys.stderr)
        sys.exit(1)

    print(f"Gửi xong transaction_id = {event['transaction_id']}")


if __name__ == "__main__":
    main()