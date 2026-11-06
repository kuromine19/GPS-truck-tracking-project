FROM mcr.microsoft.com/devcontainers/python:1-3.10-bookworm
# Kho Yarn trong image gốc có khoá GPG không hợp lệ, làm apt-get update (và feature docker-in-docker) lỗi.
RUN find /etc/apt/sources.list.d -name '*yarn*' -delete