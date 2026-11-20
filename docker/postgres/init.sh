#!/bin/bash
set -euo pipefail
# Official entrypoint runs this only for a fresh named volume.
# psql variables use SQL literal quoting; passwords never appear in logs.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=retail_password="$RETAIL_DB_PASSWORD" \
  --set=airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE ROLE retail LOGIN PASSWORD :'retail_password';
CREATE ROLE airflow_meta LOGIN PASSWORD :'airflow_password';
CREATE DATABASE retail OWNER retail;
CREATE DATABASE airflow OWNER airflow_meta;
REVOKE ALL ON DATABASE retail FROM PUBLIC;
REVOKE ALL ON DATABASE airflow FROM PUBLIC;
GRANT CONNECT ON DATABASE retail TO retail;
GRANT CONNECT ON DATABASE airflow TO airflow_meta;
SQL
