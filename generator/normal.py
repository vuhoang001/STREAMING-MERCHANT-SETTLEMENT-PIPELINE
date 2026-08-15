# 92% trafic bình thường: amount log-normal, vị trí quanh nhà 
import random 
import uuid
from datetime import datetime, timezone

MERCHANTS = [f"M{n:04d}" for n in range(1, 51)]
CHANNELS = ["POS", "WEB", "APP"]

def now_iso_millis() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    
def _amount_vnd(rng: random.Random) -> int:
    # Log-normal: chi tiêu thật lệch phải (nhiều nhỏ, ít to). KHÔNG uniform.
    # median = e^mu ≈ 198k; sigma lớn -> đuôi phải dày, thi thoảng chạm vài triệu.
    return max(1000, int(rng.lognormvariate(12.2, 0.9)))



def make_normal_event(profile: dict, rng: random.Random = random) -> dict:
    home = profile["home"]
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        profile["user_id"],
        "merchant_id":    rng.choice(MERCHANTS),
        "amount":         _amount_vnd(rng),
        "currency":       "VND",
        "location": {
            # home ± nhiễu nhỏ -> đi lại quanh khu vực, không nhảy lung tung.
            "lat": round(home["lat"] + rng.gauss(0, 0.01), 6),
            "lon": round(home["lon"] + rng.gauss(0, 0.01), 6),
        },
        "ip_address":     f"115.78.{rng.randint(0, 255)}.{rng.randint(0, 255)}",
        "device_id":      f"d-{profile['user_id']}",   # traffic nền: 1 user 1 device
        "channel":        rng.choice(CHANNELS),
        "timestamp":      now_iso_millis(),
        "status":           "SUCCESS",     # Lát 2 gần như toàn SUCCESS; lỗi để Lát sau
        "failure_reason":   None,
        "txn_type":         "PAYMENT",
        "original_txn_id":  None,
        "experiment_group": profile["experiment_group"],
    }