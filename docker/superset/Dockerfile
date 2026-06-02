FROM apache/superset:4.1.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements-local.txt
COPY superset_config.py /app/superset_config.py

RUN pip install --no-cache-dir -r /app/requirements-local.txt

USER superset
