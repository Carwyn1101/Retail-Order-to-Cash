#!/bin/bash
set -euo pipefail
airflow db migrate
# Check for the user so repeated init does not hide real create failures.
airflow users list --output json > /tmp/local-users.json
if ! python -c 'import json; import sys; sys.exit(0 if any(u["username"] == "admin" for u in json.load(open("/tmp/local-users.json"))) else 1)'; then
  airflow users create --username admin --firstname Synthetic --lastname Local \
    --role Admin --email admin@example.invalid --password "$AIRFLOW_ADMIN_PASSWORD"
fi
