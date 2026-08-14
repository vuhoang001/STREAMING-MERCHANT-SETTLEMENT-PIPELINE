# OmniPay — Advanced Streaming & Enterprise Analytics

Lab dựng lại nền tảng dữ liệu của một **cổng trung gian thanh toán**: một nửa là
**streaming nâng cao** (CEP, dynamic rule engine, health monitoring), một nửa là
**phân tích nghiệp vụ cấp điều hành** (fraud economics, loyalty ROI, biên lợi nhuận
merchant, vòng đời khách hàng).

Mục tiêu cuối cùng:

> Không dừng ở "chuyển được dữ liệu". Trả lời được câu hỏi mà ban điều hành thật sự hỏi:
> **"Rule chặn gian lận mới tiết kiệm được bao nhiêu tiền, và đổi lại làm mất bao nhiêu
> khách?"** — kèm con số bảo vệ được.

Đây là bài tập tự học. Mỗi bước có phần **thiết kế** (tại sao) trước phần **code** (thế nào).

---

## Trạng thái

| # | Bước | Sản phẩm | Trạng thái |
|---|---|---|---|
| 0 | Thiết kế kiến trúc & data contract | `README.md` + `docs/` | ✅ **DONE** |
| 1 | Hạ tầng local | `infra/docker-compose.yml` | ⬜ chưa làm |
| 2 | Data Generator (lỗi · gian lận · trễ mạng) | `generator/` | ⬜ |
| 3 | Flink App (CEP · Broadcast · Side Output · Metrics) | `flink_app/` | ⬜ |
| 4 | dbt Silver + 4 Gold Marts | `dbt_omnipay/` | ⬜ |
| 5 | Business Query Suite + Dashboard | `analytics/`, `dashboards/` | ⬜ |

Chỉ đánh ✅ khi thật sự chạy xanh, không đánh dấu theo ý định.

**Tài liệu thiết kế chi tiết**
- [`docs/01-streaming-design.md`](docs/01-streaming-design.md) — CEP, Dynamic Rule Engine, Metrics, State
- [`docs/02-analytics-design.md`](docs/02-analytics-design.md) — 4 Gold Mart, công thức + tư duy nghiệp vụ
- [`docs/03-dashboard-and-queries.md`](docs/03-dashboard-and-queries.md) — Dashboard spec + 12 câu hỏi cho Ban GĐ

---

## 1. Bối cảnh & nguyên tắc phân chia

Mỗi giao dịch phải trả lời 3 nhóm câu hỏi ở 3 khung thời gian khác nhau — đó là lý do
hệ thống có 3 mặt phẳng (plane), không phải 2:

| Câu hỏi | Deadline | Tầng trả lời |
|---|---|---|
| Chặn hay cho qua? Chuỗi này có phải card testing? | **< 1 giây** | Flink (streaming plane) |
| GMV phút này bao nhiêu? Có đang bị tấn công không? | **< 5 giây** | Serving DB (serving plane) |
| Tháng này ngành Travel lãi hay lỗ? Rule mới đáng tiền không? | **T+1** | Lakehouse + dbt (analytical plane) |

Nguyên tắc: **stream lo quyết định, serving lo hiển thị, lakehouse lo sự thật.**

Flink ưu tiên độ trễ nên chấp nhận sai số (at-least-once, state có TTL, không thấy toàn cục).
Lakehouse ưu tiên tính đúng nên được phép chậm (dedup, đối soát, tính lại khi tỷ giá đổi).
Nhồi cả hai vào một chỗ là cách hỏng hệ thống phổ biến nhất.

---

## 2. Sơ đồ luồng

```
  Risk Ops UI ──► dynamic_fraud_rules ─┐   (rule mới, KHÔNG restart job)
                                       │
  PostgreSQL ──CDC──► user_profiles ───┤
                                       ▼
  Generator ──────► payment_events ──► ┌──────────────────────────┐
  (100–500 evt/s)                      │      APACHE FLINK        │
                                       │ ──────────────────────── │
                                       │ ① Dynamic Rule Engine    │──► blocked_transactions
                                       │    (2 broadcast streams) │
                                       │ ② Impossible Travel      │──► fraud_alerts
                                       │    (ValueState+Haversine)│
                                       │ ③ CEP: Card Testing      │──► fraud_alerts
                                       │    (state machine)       │
                                       │ ④ Loyalty 15m sliding    │──► loyalty_points
                                       │ ⑤ Stream Health Metrics  │──► stream_metrics
                                       │    side output: late ────┼──► late_events
                                       └───────┬──────────┬───────┘
                                               │          │
                        JDBC sink (5s)         │          │  FileSink Parquet
                                               ▼          ▼  (commit on checkpoint)
                             ┌─────────────────────┐   ┌──────────────────────────┐
                             │  SERVING (Postgres) │   │  LAKEHOUSE (MinIO/S3)    │
                             │  ─────────────────  │   │  🥉 BRONZE  thô, có trùng│
                             │  KPI phút · alert   │   │       │ dbt              │
                             │  feed · geo heatmap │   │  🥈 SILVER  dedup·VND·PII│
                             └──────────┬──────────┘   │       │ dbt              │
                                        │              │  🥇 GOLD  4 data mart    │
                                   Grafana             └───────────┬──────────────┘
                              (Risk Command Center)             Metabase
                                                          (Executive Dashboard)
```

---

## 3. Hợp đồng dữ liệu (Data Contracts)

### 3.1 `payment_events` — stream giao dịch thô

```jsonc
{
  "transaction_id": "b7c1f0e2-...",
  "user_id":        "U000123",
  "merchant_id":    "M0042",
  "amount":         1250000,
  "currency":       "VND",                       // VND | USD | SGD
  "location":       { "lat": 21.0278, "lon": 105.8342 },
  "ip_address":     "115.78.x.x",                // PII → mask ở Silver
  "device_id":      "d-9f2a...",                 // PII → hash ở Silver
  "channel":        "APP",                       // POS | WEB | APP
  "timestamp":      "2026-08-14T09:15:03.221Z",  // event time

  // ── BỔ SUNG so với đề bài, bắt buộc phải có ──
  "status":         "SUCCESS",       // SUCCESS | FAILED | PENDING
  "failure_reason": null,            // INSUFFICIENT_FUND | CARD_DECLINED | TIMEOUT | ...
  "txn_type":       "PAYMENT",       // PAYMENT | REFUND
  "original_txn_id": null,           // trỏ về giao dịch gốc khi txn_type = REFUND
  "experiment_group": "TREATMENT"    // TREATMENT | CONTROL  (holdout cho loyalty)
}
```

### 3.2 `user_profiles` — CDC từ PostgreSQL (Debezium)

```jsonc
{
  "user_id": "U000123", "full_name": "Nguyen Van A",
  "kyc_status": "VERIFIED",          // VERIFIED | PENDING | UNVERIFIED
  "account_tier": "GOLD",            // BRONZE | SILVER | GOLD | VIP
  "risk_score_baseline": 0.12,
  "home_country": "VN", "updated_at": "2026-08-01T00:00:00Z"
}
```
> Debezium bọc payload trong `{"before":…,"after":…,"op":…}`. Job phải **unwrap** lấy `after`,
> và xử lý `op='d'` bằng cách **gỡ key khỏi broadcast state** — quên bước này thì user đã xóa
> vẫn được enrich vĩnh viễn.

### 3.3 `dynamic_fraud_rules` — broadcast stream từ Risk Ops

```jsonc
{
  "rule_id": "R-014",
  "rule_type": "AMOUNT_THRESHOLD",   // AMOUNT_THRESHOLD | COUNTRY_BLOCK | DEVICE_BLACKLIST
                                     // | VELOCITY | CHANNEL_BLOCK
  "scope": { "account_tier": "BRONZE", "channel": "WEB" },  // null = áp dụng toàn bộ
  "params": { "max_amount_vnd": 2000000 },
  "action": "BLOCK",                 // BLOCK | ALERT | REVIEW
  "priority": 100,                   // số lớn thắng khi nhiều rule cùng khớp
  "enabled": true,
  "effective_from": "2026-08-14T00:00:00Z",
  "version": 7,
  "updated_at": "2026-08-14T09:00:00Z"
}
```

### 3.4 Ba luồng bổ sung — *không có thì 4 chỉ số của bạn không tính được*

Đây là phần tôi thêm vào đề bài. Lý do ở [`docs/02`](docs/02-analytics-design.md), tóm tắt:

| Luồng | Vì sao bắt buộc |
|---|---|
| **`loyalty_redemptions`** (topic) — `redemption_id, user_id, points_spent, reward_type, timestamp` | Redemption Rate = điểm tiêu / điểm phát. Chỉ có mẫu số thì không ra tỷ lệ. Ngoài ra điểm chưa tiêu là **công nợ trên bảng cân đối** (loyalty liability) — CFO sẽ hỏi con số này |
| **`fraud_labels`** (topic, độ trễ T+7..T+45) — `transaction_id, is_fraud_confirmed, label_source, resolved_at` | Nguồn: chargeback từ ngân hàng, kết luận điều tra thủ công, khiếu nại khách. **Không có nhãn thì không có False Positive Rate** — chỉ có cảm giác. Đây là vòng phản hồi (feedback loop) mà đa số pipeline gian lận quên xây |
| **`experiment_group`** (trường trong `payment_events`) | 10% user bị giữ ở `CONTROL` — **không** được x2 điểm. Không có nhóm này thì "Incremental Revenue Lift" chỉ là tương quan đội lốt nhân quả |

**Chi phí của việc bỏ qua:** một báo cáo nói *"chính sách x2 điểm làm tăng 30% chi tiêu"*
trong khi sự thật là nhóm user vốn đã chi tiêu nhiều mới chạm được ngưỡng 5 triệu. Bạn sẽ
đề xuất tăng ngân sách điểm dựa trên một con số đảo ngược nhân quả.

---

## 4. Tám quyết định kiến trúc

**① Flink chỉ ghi Bronze ở dạng append thô.**
Sink là *at-least-once* theo checkpoint — restart job là ghi trùng. Dedup ở stream cần state
vô hạn (nhớ mọi `transaction_id` mãi mãi); ở batch chỉ là một câu `ROW_NUMBER()`.
**Đưa việc khó về nơi rẻ nhất.**

**② Tỷ giá và biểu phí thuộc về Silver, không thuộc Flink.**
Cả hai đều bị **sửa hồi tố**. Nếu Flink đóng cứng vào record, mọi con số sau này sai vĩnh viễn.
Silver join seed theo ngày → sửa seed, `dbt run` lại, số tự đúng.

**③ Broadcast State cho cả `user_profiles` lẫn `dynamic_fraud_rules`.**
Gọi Postgres mỗi giao dịch = +5–20ms/record và tự DDoS chính DB của mình. Broadcast nhân bản
vào memory từng TaskManager → lookup 0ms. Với rule engine, đây còn là cách **đổi luật lúc job
đang chạy mà không restart** — Risk Ops phản ứng trong 2 giây thay vì chờ deploy 20 phút.

**④ CEP viết tay bằng `KeyedProcessFunction`, không dùng FlinkCEP.**
Điểm kỹ thuật quan trọng: **FlinkCEP không có API cho Python** (chỉ Java/Scala). Từ PyFlink
còn cửa `MATCH_RECOGNIZE` trong Table API, nhưng nó **không đọc được ngưỡng từ broadcast state**
— mà cả bài toán của ta là ngưỡng phải động. Nên state machine viết tay là lựa chọn đúng, không
phải giải pháp tạm. Chi tiết ở [`docs/01`](docs/01-streaming-design.md).

**⑤ Event time + watermark `BoundedOutOfOrderness(10s)`, late data đi Side Output.**
Mobile mất sóng → sự kiện đến trễ. Tính cửa sổ theo giờ máy chủ thì một loạt giao dịch offline
đổ về cùng lúc rơi hết vào một cửa sổ → sai điểm thưởng và sai cả impossible travel.
Dữ liệu trễ **không vứt** — đẩy vào side output, ghi xuống Bronze, để dbt hòa giải ở T+1.

**⑥ RocksDB + incremental checkpoint 60s.**
State phải giữ giao dịch gần nhất của mọi user đang hoạt động + cửa sổ trượt + state machine
CEP theo `device_id`. Heap sẽ chết vì OOM/GC pause. RocksDB đổi ~µs lấy ~ms nhưng state lớn tùy ý.

**⑦ Serving plane riêng (Postgres) cho dashboard real-time.**
Grafana không query được Kafka; file Parquet không chịu nổi 50 người refresh 5 giây một lần.
Cần một bảng ghi-nhiều-đọc-nhiều. Lab dùng Postgres vì đủ ở 500 evt/s. **Nó gãy ở đâu:**
khoảng vài chục nghìn insert/s — production thật sẽ là ClickHouse / Pinot / Druid. Ghi rõ để
không nhầm lab với production.

**⑧ DuckDB + `httpfs` đọc Parquet trên MinIO làm engine dbt (lab); Trino + Iceberg (production).**
Bài toán ở đây là *logic mô hình hóa*, không phải vận hành cluster. Đường nâng cấp giữ nguyên
SQL: đổi adapter, đổi `read_parquet()` sang Iceberg table — model Silver/Gold gần như không sửa.

---

## 5. Bốn cái bẫy đã biết trước

| Bẫy | Triệu chứng | Xử lý |
|---|---|---|
| **Hot key** | 1 merchant chiếm 40% traffic → 1 subtask nghẽn | Key theo `user_id`/`device_id`, không key theo `merchant_id` |
| **Idle partition giữ watermark** | Watermark đứng im, cửa sổ không bao giờ đóng, dashboard "chết" | Bật `withIdleness(30s)` — bẫy kinh điển nhất của Flink |
| **State rò rỉ** | Chạy 3 ngày rồi OOM | State TTL: fraud 24h, CEP 10 phút |
| **Small files** | Bronze 200k file 4KB → Silver query 10 phút | Rolling 1 phút / 128MB, partition theo `dt` |

---

## 6. Cấu trúc thư mục

```
omnipay-streaming-lakehouse/
├── README.md                     ← đề bài + kiến trúc (file này)
├── docs/                         01-streaming · 02-analytics · 03-dashboard&queries
├── infra/docker-compose.yml      Redpanda · Flink JM/TM · MinIO · Postgres · Grafana · Metabase
├── generator/                    sinh tải + tiêm gian lận + mô phỏng trễ mạng
├── rules_publisher/              CLI đẩy rule vào dynamic_fraud_rules (giả lập Risk Ops)
├── flink_app/
│   ├── jobs/                     job chính + job metrics
│   ├── cep/                      state machine Card Testing
│   ├── udf/                      haversine, rule evaluator
│   └── common/                   schema, watermark, config
├── serving/                      DDL schema serving cho Postgres
├── lakehouse/                    bind-mount dữ liệu MinIO (xem file parquet trực tiếp)
├── dbt_omnipay/models/{staging,silver,gold}/ + seeds/ + schema.yml
├── analytics/                    Business Query Suite (12 câu hỏi của Ban GĐ)
├── dashboards/                   Grafana JSON + Metabase spec
└── scripts/                      chạy end-to-end
```

---

## 7. Cổng dịch vụ (local)

| Dịch vụ | Cổng | Ghi chú |
|---|---|---|
| Redpanda (Kafka API) | `9092` | trong mạng docker: `redpanda:29092` |
| Redpanda Console | `8080` | xem topic, đọc message |
| Flink JobManager UI | `8088` | tránh đụng `8080` |
| MinIO API / Console | `9000` / `9001` | `minioadmin` / `minioadmin` |
| PostgreSQL | `5433` | source CDC + schema `serving` |
| Grafana | `3000` | Risk Command Center |
| Metabase | `3001` | Executive Dashboard |

Credential trong lab để mặc định cho dễ đọc. **Không bê nguyên sang production.**

---

## 8. Lộ trình

### Bước 1 — Hạ tầng
Redpanda; Flink JM+TM (image tự build kèm PyFlink + connector jar); MinIO + job tạo bucket;
Postgres bật `wal_level=logical` (cho Debezium) và có sẵn schema `serving`; Grafana; Metabase.
**Đạt khi:** mọi container `healthy`, tạo được topic, mở được Flink UI.

### Bước 2 — Data Generator
Sinh 100–500 evt/s *có kiểm soát*, gồm:
- ~92% giao dịch bình thường (vị trí quanh nhà, số tiền theo phân phối log-normal — không phải uniform)
- **Card testing:** 1 `device_id` bắn 6 giao dịch < 20k đều FAILED trong 90 giây, rồi 1 giao dịch 15 triệu SUCCESS
- **Impossible travel:** cùng user, Hà Nội → TP.HCM (~1.150 km) cách 4 phút
- **KYC:** user `UNVERIFIED` tiêu 3 triệu
- **Lỗi tự nhiên:** ~6% FAILED rải đều (`INSUFFICIENT_FUND`, `TIMEOUT`…) — để FPR có nền so sánh
- **Trễ mạng:** 3% sự kiện timestamp lùi 5–60s (thử watermark), 0.5% lùi > 10 phút (thử side output)
- **Holdout:** hash `user_id` → 10% vào `CONTROL`

Không có dữ liệu vi phạm cố ý thì không chứng minh được rule đúng. Generator là **test fixture**,
không phải phần phụ.

### Bước 3 — Flink App
→ [`docs/01-streaming-design.md`](docs/01-streaming-design.md)

### Bước 4 — dbt Silver + 4 Gold Mart
→ [`docs/02-analytics-design.md`](docs/02-analytics-design.md)

### Bước 5 — Business Query Suite + Dashboard
→ [`docs/03-dashboard-and-queries.md`](docs/03-dashboard-and-queries.md)

---

## 9. Cách verify (xuyên suốt)

Truy vết **một** `transaction_id` card-testing đi đủ 6 chặng, số phải khớp từng chặng:

```
Kafka thô → fraud_alerts → serving.alert_feed (Grafana thấy < 5s)
          → file Bronze → dòng Silver (PII đã mask) → fct_fraud_performance
```

Và một bài kiểm tra khó hơn — **đối soát chéo**: tổng điểm dbt tự tính lại từ Silver so với
tổng điểm Flink đã phát ra. Chênh lệch phải giải thích được (late data? job restart ghi trùng?).
Chỗ *lệch số* mới là phần đáng học của lab này.

---

## 10. Yêu cầu môi trường

- Docker + Compose v2, cấp tối thiểu **10 GB RAM** cho Docker (Flink + RocksDB + Metabase)
- Python 3.10+ (generator, rules publisher, dbt chạy ngoài container)
- ~15 GB đĩa trống

---

**Bước tiếp theo:** dựng `infra/docker-compose.yml`.
