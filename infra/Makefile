# Lệnh tắt cho hạ tầng. Chạy trong thư mục infra/.
COMPOSE = docker compose

.PHONY: env build up-core up-streaming up-serving up-orchestration up-all workers ps logs smoke smoke-kafka down clean \
        es-setup replay-prepare produce stream

env:            ## Tạo file .env từ mẫu (chỉ khi chưa có)
	@test -f .env || (cp .env.example .env && echo 'Đã tạo .env, hãy đổi mật khẩu')

build: env      ## Build image Spark và Airflow
	$(COMPOSE) --profile orchestration build

up-core: env    ## MinIO + Spark (luôn cần)
	$(COMPOSE) up -d

up-streaming: env ## + Kafka, Kafka UI, tạo topic
	$(COMPOSE) --profile streaming up -d

up-serving: env ## + Elasticsearch, Kibana
	$(COMPOSE) --profile serving up -d

up-orchestration: env ## + Airflow
	$(COMPOSE) --profile orchestration up -d

up-all: env     ## Bật tất cả (cần khoảng 16 GB RAM cho Docker)
	$(COMPOSE) --profile streaming --profile serving --profile orchestration up -d

workers:        ## Đổi số Spark worker: make workers N=2
	$(COMPOSE) up -d --scale spark-worker=$(or $(N),1) spark-worker

ps:
	$(COMPOSE) --profile streaming --profile serving --profile orchestration ps

logs:           ## Xem log một service: make logs S=spark-master
	$(COMPOSE) --profile streaming --profile serving --profile orchestration logs -f $(S)

smoke:          ## Kiểm tra Spark + MinIO + Delta + Sedona
	$(COMPOSE) exec spark-client spark-submit /opt/infra/scripts/smoke_test.py

smoke-kafka:    ## Kiểm tra thêm Kafka (cần up-streaming)
	$(COMPOSE) exec spark-client spark-submit /opt/infra/scripts/smoke_test.py --kafka

# ---- streaming
DATE ?= 2022-07-05
VEHICLES ?= 2000
SPEEDUP ?= 60
REPLAY ?= /data/replay/$(DATE)_$(DATE)_$(VEHICLES)xe

es-setup:       ## Tạo index ES + data view Kibana
	$(COMPOSE) exec spark-client python3 /opt/jobs/streaming/es_setup.py

replay-prepare: ## Tạo dữ liệu phát lại từ Bronze
	$(COMPOSE) exec spark-client spark-submit /opt/jobs/streaming/prepare_replay.py --date $(DATE) --vehicles $(VEHICLES)

produce:        ## Phát lại vào Kafka
	$(COMPOSE) exec spark-client python3 /opt/jobs/streaming/producer.py --replay $(REPLAY) --speedup $(SPEEDUP)

stream:         ## Chạy Spark Streaming
	$(COMPOSE) exec spark-client spark-submit /opt/jobs/streaming/stream_process.py

down:           ## Tắt hết, GIỮ dữ liệu
	$(COMPOSE) --profile streaming --profile serving --profile orchestration down

clean:          ## Tắt hết và XÓA toàn bộ volume (mất dữ liệu MinIO, Kafka, ES, Airflow)
	$(COMPOSE) --profile streaming --profile serving --profile orchestration down -v
