# Use official Airflow image with Python 3.11
FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        chromium \
        chromium-driver \
        wget \
        unzip \
        curl && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/chromium /usr/bin/google-chrome

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow[statsd]==2.8.1" \
    "apache-airflow-providers-apache-spark<5" \
    "apache-airflow-providers-apache-hive<9" \
    selenium
