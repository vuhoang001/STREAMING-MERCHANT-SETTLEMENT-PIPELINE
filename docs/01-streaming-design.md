# 01 — Thiết kế Streaming nâng cao (Bước 3)

Tài liệu này chốt thiết kế cho Flink job **trước khi** viết dòng code nào. Mỗi mục có
*tư duy nghiệp vụ* → *thiết kế kỹ thuật* → *cạm bẫy*.

---

## 0. Cấu trúc job

Một job duy nhất, 5 toán tử nghiệp vụ nối tiếp. Không tách thành 5 job nhỏ — vì cả 5 đều
cần chung `user_profiles` broadcast, tách ra là nhân 5 lần chi phí broadcast và mất khả năng
áp thứ tự ưu tiên giữa các rule.

```
payment_events ──► [assign watermark] ──► [KeyedBy user_id]
                                              │
  user_profiles ──────────► broadcast ────────┤
  dynamic_fraud_rules ────► broadcast ────────┤
                                              ▼
                        ┌─────────────────────────────────────┐
                        │  KeyedBroadcastProcessFunction      │
                        │  ① Dynamic Rule Engine  (BLOCK?)    │──► blocked_transactions
                        │  ② Impossible Travel    (ValueState)│──► fraud_alerts
                        └──────────────┬──────────────────────┘
                                       │ main output: enriched
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
          [KeyedBy device_id]   [KeyedBy user_id]   [ProcessFunction]
           ③ CEP Card Testing    ④ Loyalty 15m       ⑤ Health Metrics
                    │             sliding window            │
                    ▼                  ▼                    ▼
              fraud_alerts      loyalty_points        stream_metrics
                                       │
                              side output: late_events
```

**Thứ tự áp dụng có ý nghĩa nghiệp vụ:** ① chặn trước → giao dịch bị chặn thì không tính
điểm ở ④, nhưng **vẫn** đi qua ② và ③ (một giao dịch bị chặn vẫn là bằng chứng gian lận,
vứt nó đi là làm mù hệ thống phát hiện).

---

## 1. Dynamic Rule Engine — Broadcast State Pattern

### Tư duy nghiệp vụ

Gian lận thay đổi theo giờ. Đội Risk Ops phát hiện một chiến dịch tấn công lúc 2h sáng và cần
chặn ngay. Nếu luật nằm trong code:

```
phát hiện → sửa code → review → build → deploy → restart job → khôi phục state
≈ 20–60 phút.  Kẻ tấn công cần 5 phút.
```

Với broadcast state: Risk Ops bấm nút → message vào Kafka → **2 giây** sau mọi TaskManager
đã áp luật mới, job không hề dừng, state không mất. Đây là khác biệt giữa "có hệ thống chống
gian lận" và "có hệ thống chống gian lận *dùng được*".

### Thiết kế

```python
RULES_DESC = MapStateDescriptor("fraud_rules", Types.STRING(), Types.STRING())
# key = rule_id, value = JSON rule

# Trong processBroadcastElement():
#   enabled=false hoặc đã bị xóa  → ctx.getBroadcastState().remove(rule_id)
#   ngược lại                     → put(rule_id, rule)
```

Đánh giá rule cho một giao dịch:

```
ứng_viên = [r for r in rules
            if r.enabled
            and r.effective_from <= event_time
            and khớp_scope(r.scope, txn, profile)]     # scope=null ⇒ khớp tất cả

nếu rỗng                    → PASS
ngược lại → thắng = max(ứng_viên, key=priority)  → áp thắng.action
```

**Vì sao cần `priority` mà không phải "rule nào cũng chặn":** sẽ có lúc tồn tại đồng thời
*"chặn mọi giao dịch WEB > 2 triệu"* và *"miễn trừ khách VIP"*. Không có thứ tự ưu tiên thì
hành vi phụ thuộc vào thứ tự duyệt map — tức là **không xác định**. Một hệ thống chống gian
lận không xác định thì không thể audit, và ngành thanh toán bắt buộc phải audit được.

### Cạm bẫy

| Bẫy | Hậu quả | Xử lý |
|---|---|---|
| **Broadcast state không được key** | Sửa nó trong `processElement` (phía non-broadcast) → mỗi task một bản khác nhau, mất tính nhất quán | Flink chỉ cho **ghi** trong `processBroadcastElement`, **đọc** ở `processElement`. Tôn trọng đúng như vậy |
| **Race lúc khởi động** | Giao dịch tới trước khi rule/profile kịp nạp → chặn nhầm hoặc bỏ lọt hàng loạt trong 30 giây đầu | Đệm giao dịch trong `ListState` cho tới khi nhận đủ rule đầu tiên, hoặc chấp nhận fail-open + gắn cờ `rules_not_ready` |
| **Rule sai đẩy lên production** | Chặn 100% giao dịch trong 3 phút | `version` + `effective_from` + một rule "kill switch" `action=ALERT` để hạ cấp nhanh |

### Quyết định: fail-open hay fail-close?

Không tìm thấy profile → **fail-open + gắn cờ `profile_missing`**.
Đây là quyết định *kinh doanh*, không phải kỹ thuật: chặn nhầm khách thật tốn tiền hơn hay
để lọt gian lận tốn tiền hơn? Với cổng thanh toán, một khách VIP bị chặn giữa siêu thị sẽ gọi
hotline và có thể rời bỏ; nên lab chọn fail-open, gắn cờ, để `fct_fraud_performance` đếm được
và định lượng cái giá của lựa chọn này.

---

## 2. CEP — Card Testing

### Tư duy nghiệp vụ

Kẻ gian mua một lô số thẻ đánh cắp, nhưng **không biết thẻ nào còn sống**. Chúng thử từng thẻ
bằng giao dịch cực nhỏ (5.000–19.000đ) — nhỏ để chủ thẻ không để ý, nhỏ để không chạm ngưỡng
cảnh báo. Thẻ nào SUCCESS là thẻ sống → lập tức quẹt một phát lớn để rút sạch.

> **Không một giao dịch nào trong chuỗi này trông đáng ngờ khi đứng riêng.**
> Giao dịch 8.000đ thất bại: hoàn toàn bình thường. Giao dịch 15 triệu thành công: bình thường.
> **Chỉ có trình tự mới là bằng chứng.** Đó chính là định nghĩa của CEP — và là lý do một
> hệ thống chỉ dùng ngưỡng (threshold) sẽ không bao giờ bắt được kiểu này.

### Pattern

```
Keyed by device_id
  A: status=FAILED AND amount_vnd < 20.000     ×  >= 5 lần liên tiếp, trong 2 phút
  B: status=SUCCESS AND amount_vnd > 10.000.000   ngay sau A
  → CARD_TESTING_ATTACK, severity = CRITICAL
```

### Vì sao viết tay chứ không dùng FlinkCEP

Ba lý do, xếp theo mức quan trọng:

1. **FlinkCEP không có API Python.** Thư viện CEP của Flink chỉ có Java/Scala. Dùng PyFlink
   thì lựa chọn còn lại là SQL `MATCH_RECOGNIZE` trong Table API.
2. **`MATCH_RECOGNIZE` không đọc được broadcast state.** Ngưỡng 20.000đ / 5 lần / 2 phút của ta
   phải **động** (Risk Ops chỉnh được). SQL pattern thì ngưỡng bị đóng cứng trong câu SQL.
3. **Cần side output và alert có cấu trúc.** Viết tay cho phép phát ra alert kèm toàn bộ chuỗi
   giao dịch làm bằng chứng — thứ đội điều tra thật sự cần.

Nên: `KeyedProcessFunction` + state machine. Đây là lựa chọn đúng, không phải giải pháp chữa cháy.

### State machine

```python
# State theo device_id:
failed_streak : ListState[(txn_id, ts, amount)]   # các giao dịch nhỏ FAILED liên tiếp
streak_start  : ValueState[long]

# Với mỗi giao dịch:
if FAILED and amount < ngưỡng_nhỏ:
      nếu streak rỗng → streak_start = ts
      thêm vào streak; đăng ký timer tại (streak_start + 2 phút) để dọn
elif SUCCESS and amount > ngưỡng_lớn:
      nếu len(streak) >= 5 và (ts - streak_start) <= 2 phút:
            → phát CARD_TESTING alert kèm toàn bộ streak làm bằng chứng
      xóa streak
else:
      xóa streak          # "liên tiếp" bị phá vỡ
```

### Cạm bẫy

- **"Liên tiếp" (consecutive) khác "trong khoảng" (within).** Nếu giữa chuỗi có 1 giao dịch
  bình thường thì chuỗi *đứt* — kẻ gian dùng chính điều này để né. Đây là đánh đổi
  precision/recall, phải ghi vào contract chứ không để implicit.
- **Timer là bắt buộc.** Không có timer dọn state, `device_id` chỉ xuất hiện một lần rồi biến
  mất sẽ giữ state mãi mãi → rò rỉ. TTL 10 phút.
- **Key theo `device_id` chứ không `user_id`.** Card testing dùng *một thiết bị* thử *nhiều thẻ
  của nhiều nạn nhân*. Key theo user là không bao giờ thấy pattern.

---

## 3. Impossible Travel

```
với cùng user_id:
  Δkhoảng_cách = haversine(vị_trí_trước, vị_trí_hiện_tại)
  Δthời_gian   = event_time_hiện_tại − event_time_trước
  nếu Δkhoảng_cách > 300 km VÀ Δthời_gian < 10 phút → alert
```

`ValueState[(lat, lon, ts)]`, TTL 24h. **Không dùng window** — đây là so sánh *cặp liền kề*;
window vừa đắt vừa sai ở biên (hai giao dịch cách nhau 30 giây nhưng rơi vào hai cửa sổ khác
nhau sẽ không bao giờ được so sánh).

**Vận tốc ngầm định:** 300km/10 phút = 1.800 km/h — nhanh hơn máy bay thương mại (~900 km/h).
Ngưỡng này cố tình *bảo thủ* để tránh báo động giả với người vừa xuống máy bay. Nâng độ nhạy
(ví dụ 500 km/h) sẽ bắt thêm gian lận nhưng làm phiền khách hay bay — chi phí này đo được
ở `fct_fraud_performance`, và đó chính là lý do mart đó tồn tại.

**Điểm yếu đã biết:** VPN/proxy làm sai lệch vị trí IP. Alert nên là `severity=MEDIUM` và
kết hợp thêm tín hiệu (đổi thiết bị, giờ bất thường) chứ không tự động BLOCK.

---

## 4. Loyalty — Sliding Window 15 phút

```
điểm_cơ_bản = floor(amount_vnd / 100.000)
tổng_15m    = SUM(amount_vnd) trong 15 phút gần nhất của user   (sliding, slide 1 phút)
hệ_số       = 2 nếu tổng_15m >= 5.000.000 VÀ experiment_group='TREATMENT', ngược lại 1
điểm_cuối   = điểm_cơ_bản × hệ_số
```

**Hai điểm phải chốt rõ trong contract, nếu không số Flink và số dbt sẽ lệch mà không ai hiểu vì sao:**

1. `tổng_15m` **bao gồm** giao dịch hiện tại → giao dịch đẩy user qua ngưỡng cũng được hưởng x2.
2. Giao dịch `BLOCKED` hoặc `FAILED` **không** tính vào `tổng_15m` và không sinh điểm.

**Sliding window (slide 1 phút) chứ không tumbling:** tumbling 15 phút sẽ cho kết quả phụ thuộc
vào việc khách tình cờ mua lúc 14:59 hay 15:01 — cùng hành vi, khác phần thưởng. Khách sẽ khiếu
nại, và họ đúng. Cái giá của sliding: mỗi record thuộc 15 cửa sổ ⇒ state gấp ~15 lần.

**`experiment_group` nằm ngay trong công thức** — nhóm `CONTROL` không bao giờ được x2. Đây là
chỗ duy nhất trong toàn hệ thống tạo ra được phép đo nhân quả cho `fct_loyalty_analytics`.

---

## 5. Stream Health Metrics

### Tư duy nghiệp vụ

Câu hỏi thật của ban điều hành không phải "pipeline có chạy không" mà là
**"số trên dashboard lúc này có tin được không?"**. Một pipeline "đang chạy" nhưng trễ 40 phút
còn nguy hiểm hơn pipeline chết — vì người ta vẫn ra quyết định dựa trên nó.

### Bốn chỉ số

| Chỉ số | Công thức | Đọc thế nào |
|---|---|---|
| **Event-time lag** | `processing_time − event_time` (p50/p95/p99) | Dữ liệu già bao nhiêu khi ta chạm tới nó. p99 tăng vọt = một nguồn nào đó đang tắc |
| **Watermark lag** | `processing_time − current_watermark` | Cửa sổ bị giữ lại bao lâu. **Nếu đứng im mà lag tăng tuyến tính ⇒ gần như chắc chắn là idle partition** |
| **Out-of-order rate** | `count(event_time < watermark khi tới) / total` | Chỉnh `BoundedOutOfOrderness` dựa trên số này, đừng đoán |
| **Late-event rate** | `count(side output) / total` | Trực tiếp là lượng dữ liệu bị mất khỏi tính toán real-time |

Đẩy vào topic `stream_metrics` mỗi 10 giây → JDBC sink → `serving.stream_health` → Grafana.

**Quy tắc:** dashboard điều hành phải hiển thị "Cập nhật lúc HH:MM:SS (trễ 4s)". Số không có
dấu thời gian là số không dùng được để ra quyết định.

---

## 6. State & Fault Tolerance

```python
env.enable_checkpointing(60_000)                      # 60s
cfg = env.get_checkpoint_config()
cfg.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)   # nội bộ Flink
cfg.set_min_pause_between_checkpoints(30_000)
cfg.set_checkpoint_timeout(120_000)
cfg.set_tolerable_checkpoint_failure_number(3)
cfg.enable_externalized_checkpoints(RETAIN_ON_CANCELLATION)
# RocksDB + incremental
```

**`EXACTLY_ONCE` ở đây là nội bộ Flink, không phải end-to-end.** Sink file chỉ commit khi
checkpoint xong; job chết giữa chừng thì phần đã ghi mà chưa commit sẽ được ghi lại → **Bronze
có bản trùng**. Đây là lý do Silver *bắt buộc* phải dedup. Ai nói "Flink exactly-once nên khỏi
dedup" là chưa gặp sự cố lần nào.

**State TTL** — thiếu cái này thì job sống được vài ngày rồi OOM:

| State | TTL | Lý do |
|---|---|---|
| Impossible travel (vị trí gần nhất) | 24h | Quá 24h thì không còn nghĩa "impossible" |
| CEP card testing streak | 10 phút | Pattern chỉ dài 2 phút |
| Loyalty sliding window | tự hết theo window | Flink tự dọn |
| Broadcast rules/profiles | không TTL | Phải giữ toàn bộ; dọn bằng sự kiện xóa (`op='d'`) |

**Late data → Side Output → Bronze.** Không vứt. Dữ liệu trễ vẫn là tiền thật của khách thật.
Real-time bỏ lỡ nó, nhưng T+1 phải có nó — nếu không, doanh thu báo cáo sẽ thấp hơn thực tế
một cách hệ thống, và không ai tìm ra vì sao.

---

## 7. Danh sách kiểm tra khi làm Bước 3

- [ ] Watermark có `withIdleness(30s)` — bẫy phổ biến nhất
- [ ] Debezium unwrap `after` + xử lý `op='d'`
- [ ] Broadcast state chỉ ghi trong `processBroadcastElement`
- [ ] Có xử lý giai đoạn khởi động khi rule/profile chưa nạp xong
- [ ] Mọi keyed state đều có TTL hoặc timer dọn
- [ ] Late data đi side output, không bị nuốt
- [ ] Sink Kafka đặt `transactional.id` khi bật exactly-once
- [ ] Rolling policy của FileSink: 1 phút / 128MB, partition `dt=YYYY-MM-DD`
- [ ] Job vẫn chạy sau khi kill TaskManager và cho khôi phục từ checkpoint
