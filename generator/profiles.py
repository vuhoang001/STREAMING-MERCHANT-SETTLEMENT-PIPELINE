"""Tập user CỐ ĐỊNH cho traffic nền + chiếu sang hợp đồng user_profiles §3.2."""

import random
import zlib

from normal import now_iso_millis   # tái dùng, không viết lại format thời gian

CITY_ANCHORS = {
    "HANOI":  (21.0278, 105.8342),
    "HCM":    (10.7769, 106.7009),
    "DANANG": (16.0544, 108.2022),
}
TIERS = ["BRONZE", "SILVER", "GOLD", "VIP"]
TIER_WEIGHTS = [0.50, 0.30, 0.15, 0.05]
KYC = ["VERIFIED", "PENDING", "UNVERIFIED"]
KYC_WEIGHTS = [0.85, 0.10, 0.05]

SURNAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui", "Do", "Ho"]
GIVEN = ["An", "Binh", "Chi", "Dung", "Giang", "Hoa", "Khanh", "Linh",
         "Minh", "Nam", "Phuong", "Quang", "Thao", "Tuan", "Yen"]


def experiment_group(user_id: str) -> str:
    """Holdout TẤT ĐỊNH: 10% -> CONTROL. crc32 (KHÔNG dùng hash() built-in)."""
    return "CONTROL" if zlib.crc32(user_id.encode()) % 10 == 0 else "TREATMENT"


def _risk_score(tier: str, kyc: str, rng: random.Random) -> float:
    """Rủi ro nền: UNVERIFIED cao, VIP/GOLD thấp — cho realistic một chút."""
    if kyc == "UNVERIFIED":
        return round(rng.uniform(0.40, 0.80), 2)
    if tier in ("GOLD", "VIP"):
        return round(rng.uniform(0.00, 0.15), 2)
    return round(rng.uniform(0.05, 0.35), 2)


def make_profiles(n: int = 500, seed: int = 42) -> dict:
    """Sinh n user tất định (seed). Giữ cả field NỘI BỘ (home, experiment_group)."""
    rng = random.Random(seed)
    profiles = {}
    for i in range(n):
        uid = f"U{i:06d}"
        city = rng.choice(list(CITY_ANCHORS))
        lat, lon = CITY_ANCHORS[city]
        tier = rng.choices(TIERS, TIER_WEIGHTS)[0]
        kyc = rng.choices(KYC, KYC_WEIGHTS)[0]
        profiles[uid] = {
            "user_id": uid,
            "home": {"lat": lat, "lon": lon, "city": city},   # nội bộ
            "account_tier": tier,
            "kyc_status": kyc,
            "experiment_group": experiment_group(uid),         # nội bộ
            "full_name": f"{rng.choice(SURNAMES)} {rng.choice(GIVEN)}",
            "risk_score_baseline": _risk_score(tier, kyc, rng),
        }
    return profiles


def to_user_profile(profile: dict) -> dict:
    """Chiếu profile NỘI BỘ sang đúng hợp đồng user_profiles §3.2.

    Cố tình BỎ home.lat/lon và experiment_group — chúng là chuyện nội bộ generator,
    không thuộc contract. Đây là lớp bảo vệ hợp đồng: thêm field nội bộ sau này
    cũng không rò ra topic user_profiles.
    """
    return {
        "user_id":             profile["user_id"],
        "full_name":           profile["full_name"],
        "kyc_status":          profile["kyc_status"],
        "account_tier":        profile["account_tier"],
        "risk_score_baseline": profile["risk_score_baseline"],
        "home_country":        "VN",              # 3 city đều VN
        "updated_at":          now_iso_millis(),  # thời điểm publish
    }
