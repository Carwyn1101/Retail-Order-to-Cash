FROM apache/airflow:2.11.0-python3.12

# dbt needs Git; use the same pin as the approved standalone dbt runtime.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git=1:2.39.5-0+deb12u3 \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/airflow/retail-venv \
    && chown airflow:root /opt/airflow/retail-venv

USER airflow
COPY dbt/requirements.txt /tmp/retail-requirements.txt
# Keep dbt's dependency graph separate from the unchanged Airflow installation.
RUN python -m venv /opt/airflow/retail-venv \
    && env -u PIP_CONSTRAINT PIP_USER=false /opt/airflow/retail-venv/bin/python \
       -m pip install --no-cache-dir -r /tmp/retail-requirements.txt \
    && /opt/airflow/retail-venv/bin/python -m pip check
