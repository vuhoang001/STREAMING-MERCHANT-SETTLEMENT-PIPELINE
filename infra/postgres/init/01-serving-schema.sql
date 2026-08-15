-- Chạy tự động lần đầu Postgres khởi tạo (docker-entrypoint-initdb.d).
-- Chỉ dựng KHUNG ở Bước 1. DDL bảng KPI/alert chi tiết thuộc Bước 3.

-- Serving plane: nơi Flink JDBC-sink ghi KPI phút / alert feed / geo heatmap cho Grafana.
CREATE SCHEMA IF NOT EXISTS serving;
COMMENT ON SCHEMA serving IS
  'Serving plane real-time. Flink ghi vào đây qua JDBC sink; Grafana đọc. Bảng cụ thể thêm ở Bước 3.';

-- Ghi chú CDC: wal_level=logical được bật qua command flag trong docker-compose,
-- KHÔNG phải ở đây. user_profiles (nguồn CDC) sẽ được tạo khi làm Debezium.
