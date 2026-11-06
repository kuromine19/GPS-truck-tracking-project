#!/usr/bin/env bash
# Chạy MỘT lần khi Codespace được tạo: cài công cụ, tạo infra/.env từ mẫu + Codespaces secrets.
set -euo pipefail

sudo apt-get update -qq && sudo apt-get install -y -qq make > /dev/null
pip install --quiet --user pyspark==3.5.3 duckdb

cd infra
if [ ! -f .env ]; then
  cp .env.example .env
  # Codespaces secrets (Settings > Codespaces > Secrets) – không có thì giữ giá trị mẫu
  if [ -n "${GPS_MINIO_PASSWORD:-}" ]; then
    sed -i "s|^MINIO_ROOT_PASSWORD=.*|MINIO_ROOT_PASSWORD=${GPS_MINIO_PASSWORD}|" .env
    sed -i "s|^AWS_SECRET_ACCESS_KEY=.*|AWS_SECRET_ACCESS_KEY=${GPS_MINIO_PASSWORD}|" .env
  fi
  if [ -n "${GPS_VEHICLE_KEY_SALT:-}" ]; then
    sed -i "s|^VEHICLE_KEY_SALT=.*|VEHICLE_KEY_SALT=${GPS_VEHICLE_KEY_SALT}|" .env
  fi
  echo "Đã tạo infra/.env"
fi
mkdir -p ../data

cat <<'MSG'

============================================================
 Codespace sẵn sàng. Bắt đầu:
   cd infra && make build && make up-core && make smoke
 Giao diện: tab PORTS (cạnh tab TERMINAL), bấm biểu tượng quả địa cầu.
 Xong việc: make down, rồi Stop codespace để tiết kiệm giờ.
============================================================
MSG
