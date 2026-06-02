ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
COPY patch_starrocks_adapter.py /tmp/patch_starrocks_adapter.py

RUN pip install --no-cache-dir -r /requirements.txt \
    && python /tmp/patch_starrocks_adapter.py
