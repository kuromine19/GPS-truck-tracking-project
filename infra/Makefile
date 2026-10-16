# Lệnh tắt cho hạ tầng. Chạy trong thư mục infra/.
COMPOSE = docker compose

.PHONY: env build up-core up-streaming up-serving up-orchestration up-all workers ps logs smoke smoke-kafka down clean

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

down:           ## Tắt hết, GIỮ dữ liệu
	$(COMPOSE) --profile streaming --profile serving --profile orchestration down

clean:          ## Tắt hết và XÓA toàn bộ volume (mất dữ liệu MinIO, Kafka, ES, Airflow)
	$(COMPOSE) --profile streaming --profile serving --profile orchestration down -v
