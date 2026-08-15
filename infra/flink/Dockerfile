# ─────────────────────────────────────────────────────────────
# Flink image tự build = Flink + PyFlink + connector jars.
# README §8 nói rõ image này PHẢI tự build, không kéo :latest.
#
# Nếu build fail ở bước tải jar → gần như chắc là VERSION connector
# bên dưới không khớp Maven. curl -f sẽ báo lỗi ngay, sửa ARG rồi build lại.
# ─────────────────────────────────────────────────────────────
ARG FLINK_VERSION=1.19.1
FROM flink:${FLINK_VERSION}-scala_2.12-java11

USER root

# ── 1. Python 3 + curl (curl để healthcheck /config của JobManager) ──
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl; \
    ln -sf /usr/bin/python3 /usr/bin/python; \
    rm -rf /var/lib/apt/lists/*

# ── 2. PyFlink (khớp đúng version Flink) ──
ARG FLINK_VERSION=1.19.1
RUN pip3 install --no-cache-dir apache-flink==${FLINK_VERSION}

# ── 3. Connector jars → /opt/flink/lib (tự nạp classpath) ──
# Ghim version. Bump ở đây khi lên Flink mới.
ARG KAFKA_CONNECTOR_VERSION=3.2.0-1.19
ARG JDBC_CONNECTOR_VERSION=3.2.0-1.19
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fSL -o flink-sql-connector-kafka.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o flink-connector-jdbc.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o postgresql.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# ── 4. Bật S3 filesystem (ghi checkpoint + Bronze lên MinIO) ──
# Plugin đã ship sẵn trong /opt/flink/opt, chỉ cần bật bằng cách copy vào plugins/.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

USER flink
