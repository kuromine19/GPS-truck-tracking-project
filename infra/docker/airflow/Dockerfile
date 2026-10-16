# Airflow + Java + PySpark để SparkSubmitOperator chạy driver ngay trong container (client mode).
FROM apache/airflow:2.10.3-python3.10

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
COPY download_jars.sh /opt/download_jars.sh

USER airflow
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-apache-spark==4.11.3" \
      -r /tmp/requirements-spark.txt

ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark
RUN test -d "$SPARK_HOME/jars" && bash /opt/download_jars.sh "$SPARK_HOME/jars"
ENV SPARK_CONF_DIR=/opt/spark-conf \
    PYSPARK_PYTHON=python3
