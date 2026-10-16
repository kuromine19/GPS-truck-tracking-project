# Image Spark dùng chung cho master, worker, client.
# Dựa trên python:3.10 để trùng phiên bản Python với image Airflow (bắt buộc với PySpark).
FROM python:3.10-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}" \
    SPARK_CONF_DIR=/opt/spark-conf \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

COPY download_jars.sh /tmp/download_jars.sh
RUN test -d "$SPARK_HOME/jars" \
 && chmod +x "$SPARK_HOME"/bin/* \
 && bash /tmp/download_jars.sh "$SPARK_HOME/jars"

RUN mkdir -p /opt/spark-conf /opt/jobs /data
WORKDIR /opt/jobs
