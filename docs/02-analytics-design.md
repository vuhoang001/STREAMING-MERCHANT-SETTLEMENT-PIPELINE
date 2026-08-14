# 02 — Thiết kế Analytics & Data Marts (Bước 4)

Tài liệu này giải thích **tư duy nghiệp vụ đằng sau từng công thức** trước khi viết SQL.
Một công thức không giải thích được thì không bảo vệ được trước ban điều hành.

---

## 0. Kiến trúc tầng

```
🥉 BRONZE   Parquet thô từ Flink. Append-only, CÓ TRÙNG. Không sửa gì.
            enriched_transactions/ · fraud_alerts/ · loyalty_points/
            blocked_transactions/ · late_events/ · fraud_labels/ · loyalty_redemptions/
              │
🥈 SILVER   Sự thật sạch, một dòng một sự kiện.
            dedup theo transaction_id · quy đổi VND · hash/mask PII · gộp late_events
              │
🥇 GOLD     4 data mart trả lời câu hỏi kinh doanh.
```

### Silver — ba việc, không hơn

**1. Dedup** — `ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingested_at DESC) = 1`.
Bắt buộc, vì sink của Flink là at-least-once (xem [`01`](01-streaming-design.md#6-state--fault-tolerance)).

**2. Quy đổi VND** — join `seed_fx_rate` **theo ngày giao dịch**, không dùng tỷ giá hôm nay.
Lý do: báo cáo tháng 3 phải cho ra cùng một con số dù chạy lại vào tháng 12. Tỷ giá bị sửa hồi
tố thì sửa seed và `dbt run` lại — số tự đúng, không phải backfill thủ công.

**3. PII** — `device_id` → `sha256(device_id || salt)` (giữ được khả năng GROUP BY để phát hiện
card testing, nhưng không đọc ngược được). `ip_address` → giữ /24 (`115.78.42.x`) đủ để phân
tích địa lý, bỏ octet cuối là bỏ khả năng định danh cá nhân. Đây là ranh giới GDPR/PDPA:
**giữ tính phân tích, bỏ tính định danh.**

Silver **không** tính chỉ số kinh doanh. Trộn logic nghiệp vụ vào Silver là mất khả năng
tái sử dụng cho mart sau.

---

## 1. `fct_fraud_performance` — Kinh tế học của chống gian lận

### Câu hỏi thật của ban điều hành

Không phải *"bắt được bao nhiêu gian lận"* — mà:

> **"Hệ thống chống gian lận đang kiếm tiền hay đang đốt tiền?"**

Mọi hệ thống chống gian lận đều là một cái cân. Siết chặt: chặn được nhiều gian lận, đồng thời
làm phiền nhiều khách thật. Nới lỏng: khách vui, mất tiền vì gian lận. **Không tồn tại điểm
"chặn hết gian lận, không phiền ai".** Việc của mart này là định giá cả hai phía của cái cân
để có thể chọn điểm cân bằng bằng con số thay vì bằng cảm tính.

### Ma trận nhầm lẫn — nền tảng của mọi chỉ số

Cần join `fraud_alerts`/`blocked_transactions` với **`fraud_labels`** (nhãn sự thật, về sau T+7..T+45):

|  | Thực tế: gian lận | Thực tế: hợp lệ |
|---|---|---|
| **Hệ thống chặn/cảnh báo** | TP — tiền cứu được | **FP — khách bị làm phiền** |
| **Hệ thống cho qua** | **FN — tiền mất thật** | TN — bình thường |

```sql
false_positive_rate = FP / (FP + TP)     -- trong số ca ta chặn, bao nhiêu % là oan
recall              = TP / (TP + FN)     -- trong số gian lận thật, ta bắt được bao nhiêu %
precision           = TP / (TP + FP)
```

> **Vì sao mẫu số là `FP + TP` chứ không phải tổng giao dịch:** ban điều hành hỏi
> *"cứ 100 ca ta chặn thì bao nhiêu ca là oan?"*. Chia cho tổng giao dịch sẽ ra 0,02% —
> nghe rất đẹp và hoàn toàn vô dụng cho việc ra quyết định.

**Không có `fraud_labels` thì cả bốn chỉ số trên không tồn tại.** Đây là lý do luồng đó được
thêm vào contract. Nếu chưa có nhãn, dùng **proxy**: giao dịch bị chặn mà user *thử lại và
thành công* cùng merchant trong 30 phút → gần như chắc chắn là FP. Proxy tốt hơn không đo,
nhưng phải ghi rõ là proxy trong `schema.yml`.

### Định giá friction

```sql
friction_cost = FP_count × avg_txn_value × gross_margin_rate      -- doanh thu mất ngay
              + FP_vip_count × vip_churn_risk × vip_annual_value  -- rủi ro mất khách VIP
```

**Vì sao tách riêng VIP:** một khách BRONZE bị chặn nhầm là mất một giao dịch. Một khách VIP bị
chặn nhầm giữa nhà hàng, trước mặt đối tác, là **rủi ro mất cả vòng đời khách hàng** — có thể
gấp 200 lần giá trị giao dịch. Đây chính là "Customer Friction" trong đề bài, và đây là cách
biến nó thành tiền. Trọng số theo tier là **giả định kinh doanh** — phải để trong `seeds/`
để CFO chỉnh được mà không cần sửa SQL.

### Net Fraud Loss Prevented

```sql
gross_fraud_blocked   = SUM(amount_vnd) khi is_fraud_confirmed AND blocked
net_loss_prevented    = gross_fraud_blocked × prevention_efficacy   -- ~0.6–0.8
                      − fraud_leaked_amount                          -- FN: chargeback thật
                      − friction_cost
                      − review_ops_cost                              -- ca REVIEW × chi phí/ca
```

> **`prevention_efficacy` là chỗ đa số báo cáo nói dối.** Chặn một giao dịch gian lận 15 triệu
> **không** đồng nghĩa cứu được 15 triệu: kẻ gian sẽ thử lại ở nơi khác, một phần giao dịch đó
> vốn đã fail vì thẻ hết tiền, một phần nếu lọt vẫn đòi lại được. Báo cáo cộng thẳng số tiền
> chặn được rồi tuyên bố "tiết kiệm 40 tỷ" là báo cáo sai. Hệ số 0.6–0.8 là **giả định phải
> khai báo công khai** trong seed, không được giấu trong SQL.

### Grain & cột chính

`fct_fraud_performance` — **grain: 1 dòng = (ngày × loại rule × account_tier × channel)**

`date_day, rule_type, account_tier, channel, alerts_fired, txns_blocked, tp, fp, fn,
false_positive_rate, precision, recall, gross_fraud_blocked_vnd, friction_cost_vnd,
net_loss_prevented_vnd, label_coverage_rate`

`label_coverage_rate` = tỷ lệ giao dịch đã có nhãn. Ngày gần nhất sẽ có coverage thấp
(chargeback chưa về) ⇒ **FPR của 7 ngày gần nhất luôn chưa đáng tin**. Bắt buộc hiển thị cột
này cạnh FPR trên dashboard, nếu không người xem sẽ đọc số chưa chín.

---

## 2. `fct_loyalty_analytics` — ROI và nhân quả

### Câu hỏi thật

> **"Chính sách x2 điểm tạo ra doanh thu tăng thêm, hay chỉ tặng điểm cho những giao dịch
> vốn đã xảy ra?"**

### Vấn đề: tương quan không phải nhân quả

Cách làm ngây thơ: so người được x2 với người không được x2 → thấy nhóm x2 chi tiêu cao hơn 40%
→ kết luận "chính sách hiệu quả". **Sai hoàn toàn.** Người được x2 là người đã tiêu > 5 triệu
trong 15 phút — tức là ta đang so *người chi tiêu nhiều* với *người chi tiêu ít*. Chính sách
không tạo ra khác biệt đó; nó chỉ **chọn** ra người vốn đã khác biệt. Đây là **selection bias**,
và nó là lỗi phân tích tốn tiền nhất trong ngành loyalty.

### Giải pháp: holdout

10% user (hash theo `user_id`, cố định) vào `CONTROL` — vượt ngưỡng vẫn **không** được x2.
Hai nhóm giống nhau về mọi mặt trừ chính sách ⇒ chênh lệch là nhân quả.

```sql
incremental_lift = (ARPU_treatment − ARPU_control) / ARPU_control

incremental_revenue = (ARPU_treatment − ARPU_control) × active_users_treatment
point_cost          = points_issued_treatment × cost_per_point_vnd
                    − points_issued_control_scaled          -- điểm dù sao cũng phải phát
program_roi         = (incremental_revenue × gross_margin_rate) / point_cost
```

> **`program_roi < 1` nghĩa là mỗi đồng điểm bỏ ra thu về chưa tới một đồng lợi nhuận gộp —
> chương trình đang đốt tiền.** Đây là con số duy nhất trong mart này mà CFO quan tâm.
> Trừ đi `points_issued_control_scaled` vì phần điểm cơ bản (x1) vốn phải phát dù có chính
> sách hay không; chỉ phần *tăng thêm* mới là chi phí của chính sách.

**Phải kiểm tra trước khi tin kết quả:** hai nhóm có cân bằng không (số user, tier mix, GMV
nền trước khi chạy chương trình)? Nếu holdout lệch, mọi con số phía sau vô nghĩa. Đưa test này
vào `schema.yml`.

### Loyalty Liability — con số CFO sẽ hỏi

```sql
outstanding_points   = SUM(points_issued) − SUM(points_redeemed)
liability_vnd        = outstanding_points × cost_per_point_vnd
redemption_rate      = points_redeemed / points_issued
breakage_rate        = 1 − redemption_rate        -- điểm không bao giờ được tiêu
```

**Điểm chưa tiêu là công nợ trên bảng cân đối kế toán**, không phải chi phí marketing đã tiêu.
Kế toán cần con số này để trích lập dự phòng. `breakage_rate` (thường 20–30%) thực chất là
*lãi* — điểm phát ra nhưng không ai đòi. Nhưng breakage quá cao (> 50%) là tín hiệu xấu:
phần thưởng không hấp dẫn, chương trình không tạo được gắn kết.

### Cohort theo `account_tier`

Grain: **(ngày × account_tier × experiment_group)**. Kỳ vọng khác nhau theo tier — VIP thường
đã trung thành sẵn nên lift thấp (tặng điểm cho người dù sao cũng ở lại = lãng phí ngân sách),
BRONZE/SILVER mới là nhóm lift cao nhất. Nếu số liệu cho thấy đúng vậy → khuyến nghị: **dồn
ngân sách điểm vào tier thấp.** Đó là loại khuyến nghị mà mart này sinh ra để tạo.

---

## 3. `fct_merchant_profitability` — Biên lợi nhuận thật

### Câu hỏi thật

> **"Merchant nào GMV lớn nhưng đang làm chúng ta lỗ?"**

Đội sales được thưởng theo GMV. Nên họ sẽ ký những hợp đồng GMV khổng lồ với take-rate mỏng dính,
tỷ lệ hoàn tiền cao và chargeback dày đặc. Trên báo cáo GMV thì đẹp, trên P&L thì âm.
Mart này tồn tại để phát hiện chuyện đó.

### Chuỗi công thức

```sql
gross_revenue   = GMV × take_rate(industry)          -- ta thu của merchant
interchange_fee = GMV × interchange_rate(channel)    -- ta TRẢ cho ngân hàng/tổ chức thẻ
refund_cost     = refund_amount × refund_fee_rate + refund_count × fixed_refund_cost
fraud_cost      = chargeback_amount + chargeback_count × chargeback_penalty_fee
processing_cost = txn_count × cost_per_txn           -- hạ tầng, phân bổ

net_revenue     = gross_revenue − interchange_fee − refund_cost − fraud_cost − processing_cost
net_take_rate   = net_revenue / GMV                  -- ★ chỉ số cốt lõi
```

> **Vì sao `interchange_fee` phải theo `channel` chứ không theo ngành:** phí liên ngân hàng
> phụ thuộc *cách thẻ được đọc*, không phụ thuộc merchant bán gì. POS (quẹt thẻ vật lý, có
> chip+PIN) rủi ro thấp nhất nên phí rẻ nhất; WEB (card-not-present) đắt nhất vì ngân hàng gánh
> rủi ro gian lận cao hơn. Gán phí theo ngành là sai bản chất và sẽ ra kết luận sai về
> merchant nào có lãi.

> **Vì sao `chargeback_penalty_fee` là khoản cố định theo *số lần*, không theo số tiền:**
> tổ chức thẻ phạt theo *sự vụ* (15–100 USD/ca) bất kể giá trị. Hệ quả nghiệp vụ quan trọng:
> **một merchant có nhiều chargeback giá trị nhỏ có thể lỗ nặng hơn merchant có một chargeback
> lớn.** Nếu mô hình hóa phí phạt theo % số tiền, ta sẽ hoàn toàn không thấy nhóm merchant độc
> hại này.

### Vì sao chia theo ngành hàng

| Ngành | Đặc trưng kinh tế | Rủi ro ẩn |
|---|---|---|
| **F&B** | Ticket nhỏ, tần suất cao, refund gần như không có | `cost_per_txn` cố định ăn mòn biên — 30k/giao dịch thì phí xử lý cố định có thể chiếm hết lãi |
| **Retail** | Ticket trung bình, refund 5–10% | Mùa vụ; refund sau Tết dồn cục |
| **Travel** | **Ticket rất lớn, chargeback rất cao** | Khách đặt vé trước 3 tháng rồi hủy/khiếu nại. **Ngành duy nhất thường xuyên âm biên dù GMV to nhất.** Đây là phát hiện điển hình của mart này |

Grain: **(ngày × merchant_id × industry)**, cộng cột `net_take_rate` và cờ `is_margin_negative`.

**Bất biến phải test:** `net_revenue <= gross_revenue`, `take_rate BETWEEN 0 AND 0.1`,
`refund_amount <= gross_amount` (hoàn nhiều hơn bán = lỗi dữ liệu hoặc gian lận nội bộ —
cả hai đều cần biết ngay).

---

## 4. `dim_customer_lifecycle` — Máy trạng thái vòng đời

### Câu hỏi thật

> **"Ai sắp rời bỏ, và có kịp cứu không?"**

Giữ một khách rẻ hơn kiếm khách mới 5–7 lần. Nhưng chỉ cứu được nếu phát hiện lúc họ *đang
trượt*, không phải lúc đã đi.

### Trạng thái

```
NEW ──► ACTIVE ──► HIGHLY_ACTIVE
         │  ▲            │
         ▼  └────────────┘
      AT_RISK ──► DORMANT ──► CHURNED
         │
         └──► HIGH_RISK_SUSPENDED   (nhánh rủi ro, ưu tiên cao nhất)
```

### Sai lầm phải tránh: ngưỡng toàn cục

Cách làm phổ biến: *"không giao dịch 30 ngày → AT_RISK"*. **Sai với cả hai loại khách.**
Người mua cà phê hằng ngày im lặng 10 ngày là **báo động đỏ** — nhưng luật 30 ngày không thấy.
Người đóng tiền điện hằng tháng im lặng 25 ngày là **hoàn toàn bình thường** — nhưng nếu hạ
ngưỡng xuống 10 ngày thì họ bị gắn cờ oan mỗi tháng.

### Giải pháp: nhịp độ cá nhân (personal cadence)

```sql
median_gap    = MEDIAN(khoảng cách giữa 2 giao dịch liên tiếp của CHÍNH user đó)
days_silent   = CURRENT_DATE − last_txn_date
silence_ratio = days_silent / NULLIF(median_gap, 0)     -- ★ chỉ số cốt lõi
```

`silence_ratio > 2` nghĩa là **khách này đang im lặng lâu gấp đôi thói quen của chính họ** —
so sánh khách với chính mình, không so với trung bình toàn hệ thống.

```sql
CASE
  WHEN risk_status = 'HIGH_RISK_SUSPENDED'            THEN 'HIGH_RISK_SUSPENDED'
  WHEN days_since_first_txn <= 30                     THEN 'NEW'
  WHEN days_silent > 90                               THEN 'CHURNED'
  WHEN days_silent > 60                               THEN 'DORMANT'
  WHEN silence_ratio > 2 AND txn_count_90d >= 3       THEN 'AT_RISK'
  WHEN txn_count_30d >= p80_toàn_hệ_thống             THEN 'HIGHLY_ACTIVE'
  ELSE 'ACTIVE'
END
```

`txn_count_90d >= 3` là điều kiện chống nhiễu: cần ít nhất 3 giao dịch thì `median_gap` mới có
nghĩa. Không có nó, một khách mua đúng 2 lần sẽ có `median_gap` vô nghĩa và bị gắn cờ ngẫu nhiên.

### Ma trận chuyển trạng thái — thứ có giá trị nhất ở mart này

Trạng thái *hôm nay* chỉ là ảnh chụp. Cái đáng tiền là **dòng chảy**:

```sql
-- SCD Type 2 nhẹ: state_from → state_to → transition_date
SELECT state_from, state_to, COUNT(*), SUM(gmv_30d_trước)
FROM transitions WHERE date_day = CURRENT_DATE GROUP BY 1,2
```

Hôm nay **1.240 khách trượt từ HIGHLY_ACTIVE → AT_RISK, mang theo 3,2 tỷ GMV/tháng** — đó mới
là dòng tin nhắn khiến ban điều hành hành động. "Hiện có 40.000 khách AT_RISK" thì không.

Đây cũng là lý do mart này là **dimension có lịch sử (SCD2)** chứ không phải bảng trạng thái
hiện tại: không lưu lịch sử thì không tính được ma trận chuyển dịch, và không đo được chiến
dịch giữ chân có tác dụng hay không.

---

## 5. Seeds — mọi giả định kinh doanh nằm ở đây

| Seed | Nội dung | Ai sở hữu |
|---|---|---|
| `seed_fx_rate` | `date, currency, rate_to_vnd` | Finance |
| `seed_take_rate` | `industry, take_rate` | Sales/Pricing |
| `seed_interchange_rate` | `channel, rate` | Payment Ops |
| `seed_merchant_industry` | `merchant_id, industry` | Sales |
| `seed_cost_assumptions` | `cost_per_txn, cost_per_point, chargeback_penalty_fee, prevention_efficacy, gross_margin_rate, vip_churn_risk` | **Finance/CFO** |

**Nguyên tắc bất di bất dịch: không một con số giả định nào được hardcode trong SQL.**
Khi CFO nói *"đổi prevention_efficacy từ 0.7 xuống 0.55 xem sao"*, việc đó phải là sửa một ô
trong CSV rồi `dbt run`, không phải mở 4 file SQL đi tìm. Đây cũng là cách làm **kịch bản
what-if** — thứ ban điều hành luôn hỏi ngay sau khi xem báo cáo lần đầu.

---

## 6. Data quality — `schema.yml`

**Test cấu trúc** (bắt lỗi pipeline): `unique`, `not_null` trên khóa; `accepted_values` cho
`status`, `channel`, `account_tier`, `lifecycle_state`; `relationships` từ fact về dim.

**Test bất biến nghiệp vụ** (bắt lỗi *logic* — quan trọng hơn):

```
net_revenue <= gross_revenue                     -- không thể lãi hơn doanh thu
false_positive_rate BETWEEN 0 AND 1
points_redeemed <= points_issued cộng dồn        -- không tiêu được điểm chưa phát
refund_amount <= gross_amount (theo merchant/ngày)
SUM(gold GMV) = SUM(silver amount_vnd)           -- đối soát chéo tầng
tổng điểm dbt tính lại ≈ tổng điểm Flink phát ra (sai lệch < 0.5%)
```

Câu cuối là bài kiểm tra thật của toàn hệ thống: **hai đường tính độc lập phải gặp nhau.**
Lệch quá ngưỡng ⇒ có late data chưa gộp, hoặc job restart ghi trùng mà dedup sót, hoặc định
nghĩa "tổng 15 phút" giữa Flink và dbt không khớp. Cả ba đều là lỗi thật cần tìm ra.

**Freshness:** cảnh báo nếu Bronze không có file mới trong 15 phút. Pipeline chết lúc 2h sáng
mà 9h sáng mới biết là đã quá muộn.
