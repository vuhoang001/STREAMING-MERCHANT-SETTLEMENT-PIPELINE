# 03 — Dashboard Spec & Business Query Suite (Bước 5)

---

# PHẦN A — Thiết kế Dashboard

## A.0 Nguyên tắc chung

**Mỗi panel phải trả lời một câu hỏi và dẫn tới một hành động.** Panel nào không đổi được
quyết định của ai thì gỡ đi — nó chỉ làm loãng những panel còn lại.

Ba luật áp cho mọi panel trong lab này:

| Luật | Lý do |
|---|---|
| **Mọi số đều có dấu thời gian** — "Cập nhật 09:14:22 (trễ 4s)" | Số không biết già bao nhiêu là số không dùng để quyết định được. Lấy từ `serving.stream_health` |
| **Không bao giờ dùng hai trục y trên một biểu đồ** | Hai thang đo căn chỉnh tùy tiện sẽ *bịa ra* một mối tương quan không có trong dữ liệu. Cần so hai đại lượng khác thang ⇒ hai biểu đồ, hoặc quy về chỉ số hóa base=100 |
| **Màu trạng thái (đỏ/cam/vàng/xanh) chỉ dùng cho trạng thái**, không bao giờ làm "màu series thứ 4" | Đỏ trên dashboard rủi ro phải luôn có nghĩa "xấu". Dùng nó để phân biệt kênh POS/WEB là phá vỡ quy ước duy nhất mà người xem tin tưởng |

---

## A.1 Risk Command Center (Grafana · nguồn: `serving` · refresh 5s)

**Người dùng:** trực ban Risk Ops, nhìn màn hình suốt ca.
**Quyết định cần ra:** *"Có đang bị tấn công không? Có cần bật rule mới ngay không?"*

| # | Panel | Câu hỏi | Dạng | Vì sao dạng này |
|---|---|---|---|---|
| 1 | **Alert feed** | Chuyện gì vừa xảy ra? | **Bảng** cuộn, mới nhất trên cùng, cột severity có chip màu trạng thái | Người trực đọc *từng dòng* để hành động, không đọc xu hướng. Đây là dữ liệu văn bản — không phải biểu đồ |
| 2 | **Geo heatmap** | Bất thường tập trung ở đâu? | **Heatmap trên bản đồ**, thang **một màu** nhạt→đậm | Nhiệm vụ là so *độ lớn* ⇒ thang tuần tự một màu. **Không dùng rainbow**: người xem phải tra bảng chú giải mới biết xanh lá lớn hơn hay nhỏ hơn vàng |
| 3 | **Fraud alerts/phút theo loại** | Loại tấn công nào đang tăng? | **Line**, 3 series (`CARD_TESTING`, `IMPOSSIBLE_TRAVEL`, `RULE_BLOCK`), nhãn trực tiếp ở đầu mút | 3 series là ngưỡng thoải mái cho phân biệt bằng màu. Có nhãn trực tiếp thì không phụ thuộc chú giải |
| 4 | **Block rate hiện tại** | Có đang chặn quá tay không? | **Stat tile**: số lớn + delta so với cùng giờ tuần trước + sparkline | Một con số hiện tại ⇒ ô số, không phải biểu đồ cột một cột. So với **cùng giờ tuần trước** vì lưu lượng thanh toán có chu kỳ ngày/tuần rất mạnh |
| 5 | **Top device đang bị nghi** | Chặn thiết bị nào ngay bây giờ? | **Bảng** top-10, kèm nút "Đẩy rule chặn" | Trực ban cần *danh sách để hành động*, kèm đường dẫn tới `rules_publisher` |
| 6 | **Stream health** | Số trên màn hình này có tin được không? | **3 stat tile**: watermark lag p99 · late-event rate · events/s | Panel này quan trọng nhất mà hay bị quên. Watermark lag tăng tuyến tính ⇒ số ở 5 panel kia **đang sai** |

**Bẫy phải tránh ở panel 3:** cám dỗ vẽ chung "số alert" (0–200) và "GMV" (0–8 tỷ) trên một
biểu đồ hai trục để "xem tương quan". Đó là cách tạo ra một tương quan hoàn toàn do việc chọn
thang đo sinh ra. Muốn so ⇒ chỉ số hóa cả hai về base=100 tại đầu ca, một trục.

---

## A.2 Executive Dashboard (Metabase · lai batch + stream · refresh 60s)

**Người dùng:** ban điều hành, xem 5 phút mỗi sáng.
**Quyết định cần ra:** *"Tuần này phân bổ ngân sách và ưu tiên vào đâu?"*

Bố cục: **một hàng KPI trên cùng**, rồi biểu đồ chi tiết bên dưới. Một hàng lọc duy nhất
(khoảng ngày, kênh, ngành hàng) đặt phía trên tất cả, không nhét bộ lọc riêng vào từng thẻ.

| # | Panel | Câu hỏi | Dạng | Vì sao |
|---|---|---|---|---|
| 1 | **Hàng KPI** — GMV hôm nay · Net Revenue · Net Take-Rate · Success Rate · Net Fraud Prevented | Sức khỏe tổng thể? | **KPI row** 5 stat tile, mỗi ô có delta + sparkline 30 ngày | Số headline ⇒ ô số. Không nhóm chúng thành biểu đồ cột — 5 đại lượng khác đơn vị, cột cạnh nhau là vô nghĩa |
| 2 | **GMV theo phút** (hôm nay vs trung vị 4 tuần cùng thứ) | Hôm nay bất thường không? | **Line 2 series**: thực tế (đậm) + dải kỳ vọng (nền xám nhạt p10–p90) | Không có dải kỳ vọng thì mọi dao động đều trông như sự cố. Dải này biến "GMV giảm" thành "GMV giảm **dưới ngưỡng bình thường**" — khác nhau về hành động |
| 3 | **Success rate theo kênh** | Kênh nào đang hỏng? | **Line 3 series** (POS/WEB/APP), nhãn trực tiếp | Cùng đơn vị (%), cùng thang ⇒ chung một trục là hợp lệ. POS tụt riêng ⇒ lỗi thiết bị đầu cuối; cả ba cùng tụt ⇒ lỗi hệ thống ta |
| 4 | **Net take-rate theo ngành** | Ngành nào lãi/lỗ? | **Diverging bar** quanh mốc 0 | Nhiệm vụ là *cực tính* (âm hay dương) ⇒ hai màu đối nghịch, mốc giữa xám trung tính. Cột thường sẽ vùi mất dấu âm — mà dấu âm mới là toàn bộ thông điệp |
| 5 | **Top 10 merchant âm biên** | Đàm phán lại với ai? | **Bảng** có cột GMV, net_take_rate, cờ cảnh báo | 10 dòng × 4 cột số ⇒ bảng đọc nhanh hơn mọi biểu đồ |
| 6 | **Fraud: chặn được vs friction** | Cái cân đang lệch bên nào? | **Hai biểu đồ cạnh nhau** cùng đơn vị VNĐ: `net_loss_prevented` và `friction_cost` | Cố nhét vào một biểu đồ hai trục là sai. Cùng đơn vị tiền ⇒ **có thể** chồng lên một trục — nhưng tách đôi vẫn dễ đọc hơn |
| 7 | **Dòng chuyển vòng đời** | Bao nhiêu khách đang trượt? | **Bảng chuyển trạng thái** (from → to → số khách → GMV mang theo), tô đậm dòng `HIGHLY_ACTIVE → AT_RISK` | Nhấn mạnh *một* dòng quan trọng, phần còn lại làm nền — hiệu quả hơn tô 8 màu cho 8 trạng thái |
| 8 | **Loyalty ROI + Liability** | Chương trình điểm lãi hay lỗ? | **2 stat tile**: `program_roi` (kèm khoảng tin cậy) và `liability_vnd` | ROI không có khoảng tin cậy là con số nguy hiểm — holdout 10% thì sai số lấy mẫu không nhỏ |

**Ba thứ cố tình không có trên dashboard này:**
- **Biểu đồ tròn tỷ trọng kênh** — mắt người so góc rất tệ; cột ngang chính xác hơn ở mọi tình huống.
- **Bảng xếp hạng merchant theo GMV** — khuyến khích đúng cái tư duy sai mà mart #3 sinh ra để chống lại.
- **Số alert luỹ kế từ đầu tháng** — chỉ tăng, không bao giờ hành động được.

---

## A.3 Ranh giới nguồn dữ liệu

| Panel | Nguồn | Độ trễ | Vì sao |
|---|---|---|---|
| Risk Command Center | `serving.*` (Postgres, Flink JDBC sink) | 2–5s | Cần nhanh, chấp nhận số gần đúng |
| KPI hàng trên + GMV/phút | `serving.kpi_minute` | ~60s | Số "sống" cho cảm giác nhịp độ |
| Còn lại (fraud econ, ROI, biên LN, vòng đời) | Gold marts (dbt, T+1) | 1 ngày | **Cần đúng, không cần nhanh.** FPR đòi nhãn chargeback về sau nhiều tuần — ép nó real-time là tạo ra một con số sai |

Ghi rõ nguồn + độ trễ trên từng panel. Người xem phải biết mình đang nhìn số nhanh hay số đúng.

---

# PHẦN B — Business Query Suite (`analytics/`)

12 câu hỏi thật. Mỗi câu: một file SQL, một mart, một hành động.

## B.1 Rủi ro & Gian lận

**Q1. Rule mới của Risk Ops triển khai hôm qua có đáng tiền không?**
`fct_fraud_performance` · so sánh 7 ngày trước/sau theo `rule_type`
→ Δ`net_loss_prevented` và Δ`friction_cost`. **Nếu friction tăng nhanh hơn số tiền cứu được ⇒ tắt rule.**
*Bẫy:* phải so cùng thứ trong tuần (lưu lượng T7/CN khác hẳn T2), và chỉ đọc trên khoảng có
`label_coverage_rate` đủ cao.

**Q2. Tuần này chặn nhầm bao nhiêu khách VIP, tốn bao nhiêu tiền?**
`fct_fraud_performance` lọc `account_tier='VIP'` → `fp_count`, `friction_cost_vnd`, danh sách user.
→ Hành động: đội chăm sóc gọi lại xin lỗi; cân nhắc rule miễn trừ VIP.

**Q3. Card testing: thiết bị nào đang tấn công, ta tránh được bao nhiêu thiệt hại?**
`fct_fraud_performance` + Silver alerts · group theo `device_hash`
→ số chuỗi tấn công, tổng tiền giao dịch lớn đã chặn.

**Q4. Ta đang bỏ lọt loại gian lận nào?**
`fraud_labels` LEFT JOIN alerts, lấy các ca `is_fraud_confirmed = true` mà **không** có alert
→ phân tích FN theo kênh/ngành. **Đây là câu hỏi khó chịu nhất và giá trị nhất** — nó chỉ ra
khoảng mù của hệ thống, tức là rule tiếp theo cần viết.

## B.2 Merchant & Lợi nhuận

**Q5. Merchant nào GMV lớn nhưng đang làm ta lỗ?**
`fct_merchant_profitability` · `WHERE net_take_rate < 0 ORDER BY gmv DESC LIMIT 20`
→ Hành động: đàm phán lại take-rate hoặc chấm dứt hợp đồng.

**Q6. Chi phí nào ăn mòn biên lợi nhuận ngành Travel?**
Phân rã `gross_revenue → interchange → refund → fraud → processing → net` theo ngành.
→ Nếu `fraud_cost` chiếm ưu thế ⇒ siết rule riêng cho Travel thay vì cắt giảm toàn hệ thống.

**Q7. Nếu `cost_per_txn` tăng 20%, ngành nào chuyển sang âm biên?**
Kịch bản what-if: sửa `seed_cost_assumptions`, `dbt run`, so hai lần chạy.
→ F&B ticket nhỏ sẽ gãy trước — đây là bài kiểm tra sức chịu đựng của mô hình định giá.

**Q8. Success rate theo kênh có đang trượt không?**
Silver · tỷ lệ SUCCESS theo `channel` × ngày, kèm phân rã `failure_reason`.
→ POS tụt riêng ⇒ lỗi thiết bị đầu cuối. Cả ba cùng tụt ⇒ lỗi phía ta.

## B.3 Khách hàng & Loyalty

**Q9. Chính sách x2 điểm có thực sự tạo doanh thu tăng thêm không?**
`fct_loyalty_analytics` · TREATMENT vs CONTROL → `incremental_lift`, `program_roi`.
→ **`program_roi < 1` ⇒ đề xuất dừng hoặc chỉnh ngưỡng.** Kèm kiểm tra cân bằng hai nhóm trước
khi trình bày bất cứ con số nào.

**Q10. Nên dồn ngân sách điểm vào tier nào?**
Cùng mart, tách theo `account_tier` → lift theo từng tier.
→ Nếu VIP lift ≈ 0 ⇒ đang tặng điểm cho người dù sao cũng ở lại; chuyển ngân sách xuống
BRONZE/SILVER.

**Q11. Công nợ điểm thưởng đang là bao nhiêu?**
`outstanding_points × cost_per_point` + `redemption_rate` + `breakage_rate`.
→ Con số cho kế toán trích lập dự phòng. Breakage > 50% ⇒ phần thưởng không hấp dẫn.

**Q12. Ai đang trượt khỏi nhóm khách tốt, mang theo bao nhiêu GMV?**
`dim_customer_lifecycle` · `HIGHLY_ACTIVE → AT_RISK` trong 7 ngày, kèm GMV 30 ngày trước đó.
→ Danh sách cho chiến dịch giữ chân, **xếp theo GMV chứ không theo số lượng khách.**

---

## B.4 Bốn câu hỏi meta — hỏi trước khi tin bất kỳ số nào ở trên

| # | Câu hỏi | Truy vấn |
|---|---|---|
| M1 | Dữ liệu hôm nay đã đủ chưa? | `stream_health`: late-event rate, watermark lag p99 |
| M2 | Nhãn gian lận đã chín chưa? | `label_coverage_rate` theo ngày — dưới 80% thì đừng đọc FPR |
| M3 | Hai đường tính độc lập có gặp nhau không? | Tổng điểm Flink phát ra vs tổng điểm dbt tính lại (lệch < 0.5%) |
| M4 | Tổng tầng có khớp không? | `SUM(gold GMV) = SUM(silver amount_vnd)` |

> Một Data Engineer giỏi không phải người dựng được dashboard đẹp, mà là người **biết khi nào
> con số trên dashboard không đáng tin và nói ra trước khi ai đó dùng nó để ra quyết định.**
> Bốn câu M1–M4 chính là phần đó của nghề.
