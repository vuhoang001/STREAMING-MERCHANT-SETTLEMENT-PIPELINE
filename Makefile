# ═══════════════════════════════════════════════════════════════════════════
# OmniPay — điều khiển hạ tầng. Bật từng phần cho nhẹ máy bằng docker profiles.
#
#   make setup          # tạo infra/.env từ .env.example (chạy 1 lần)
#   make build          # build Flink image (PyFlink + connector jars)
#   make up-infra       # Redpanda + MinIO + Postgres (nền)
#   make up-streaming   # infra + Flink   ← làm streaming thì dùng cái này
#   make topics         # tạo các Kafka topic
#   make ps / logs      # trạng thái / log
#   make down           # tắt (giữ dữ liệu)
#   make clean          # tắt + XÓA volume (làm lại từ đầu)
#
# Gõ `make` hoặc `make help` để xem đầy đủ.
# ═══════════════════════════════════════════════════════════════════════════

COMPOSE := docker compose -f infra/docker-compose.yml --env-file infra/.env

# Topic theo data contract ở README §3 (input) + docs/01 (output).
TOPICS := payment_events user_profiles dynamic_fraud_rules loyalty_redemptions \
          fraud_labels blocked_transactions fraud_alerts loyalty_points \
          stream_metrics late_events

.DEFAULT_GOAL := help
.PHONY: help setup build up-infra up-flink up-streaming up-viz up \
        down down-flink clean ps logs logs-flink topics topics-list sql psql restart-flink \
        flink-run flink-jobs flink-cancel

help: ## Danh sách lệnh
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup: ## Tạo infra/.env từ .env.example (không ghi đè nếu đã có)
	@test -f infra/.env && echo "infra/.env đã tồn tại, bỏ qua." \
	  || (cp infra/.env.example infra/.env && echo "Đã tạo infra/.env")

# ── Build ──
build: ## Build Flink image (chạy trước lần up-flink đầu tiên)
	$(COMPOSE) --profile flink build

# ── Up ──
up-infra: ## Bật nền: Redpanda + Console + MinIO + Postgres
	$(COMPOSE) --profile infra up -d

up-flink: ## Bật riêng Flink (infra phải đang chạy)
	$(COMPOSE) --profile flink up -d

up-streaming: ## Bật infra + Flink (dùng khi làm streaming)
	$(COMPOSE) --profile streaming up -d

up-viz: ## Bật Grafana + Metabase (Bước 5)
	$(COMPOSE) --profile viz up -d

up: ## Bật tất cả
	$(COMPOSE) --profile all up -d

# ── Down ──
down: ## Tắt tất cả (GIỮ dữ liệu trong volume)
	$(COMPOSE) --profile all down

down-flink: ## Tắt riêng Flink (giữ infra chạy)
	$(COMPOSE) --profile flink down

clean: ## Tắt + XÓA volume (Redpanda/MinIO/Postgres về trắng)
	$(COMPOSE) --profile all down -v

restart-flink: ## Restart Flink JM+TM (sau khi đổi FLINK_PROPERTIES)
	$(COMPOSE) --profile flink up -d --force-recreate flink-jobmanager flink-taskmanager

# ── Quan sát ──
ps: ## Trạng thái container
	$(COMPOSE) --profile all ps

logs: ## Tail log tất cả
	$(COMPOSE) --profile all logs -f --tail=100

logs-flink: ## Tail log Flink
	$(COMPOSE) --profile flink logs -f --tail=100 flink-jobmanager flink-taskmanager

# ── Kafka topics ──
topics: ## Tạo các Kafka topic (idempotent)
	$(COMPOSE) exec redpanda rpk topic create $(TOPICS) -p 6 -r 1 || true

topics-list: ## Liệt kê topic
	$(COMPOSE) exec redpanda rpk topic list

# ── Tiện ích ──
sql: ## Mở Flink SQL Client
	$(COMPOSE) exec flink-jobmanager ./bin/sql-client.sh

psql: ## Mở psql vào Postgres
	$(COMPOSE) exec postgres psql -U omnipay -d omnipay

# ── Flink job ──
# JOB là đường dẫn TƯƠNG ĐỐI so với flink_app/. Ví dụ:
#   make flink-run JOB=jobs/f1_read_print.py
flink-run: ## Submit job PyFlink (detached). Dùng: make flink-run JOB=jobs/xxx.py
	$(COMPOSE) exec flink-jobmanager flink run -d -py /opt/flink/jobs/$(JOB)

flink-jobs: ## Liệt kê job đang chạy trong cluster
	$(COMPOSE) exec flink-jobmanager flink list

flink-cancel: ## Hủy job: make flink-cancel JID=<job-id>
	$(COMPOSE) exec flink-jobmanager flink cancel $(JID)
