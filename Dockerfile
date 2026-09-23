# =====================================================================
# Dockerfile: Airflow & PySpark Pipeline Environment
# =====================================================================

FROM apache/airflow:2.8.3-python3.11

USER root

# Install OpenJDK JRE, procps, and networking tools for local PySpark execution
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
