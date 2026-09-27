#!/usr/bin/env bash
# End-to-end setup of the App Health Agent on Elastic 9.4.
#
#   ./quickstart.sh my_components.csv [my_checks.yaml]
#
# my_components.csv = your component -> indices table (see config/components.example.csv).
# Minimal columns: component,log_indices,metric_indices,trace_indices
#
# Needs: ES_URL, KIBANA_URL, and ELASTIC_API_KEY (or ELASTIC_USERNAME + ELASTIC_PASSWORD);
# optional ELASTIC_CA_CERT. Safe to re-run: every step is idempotent.
set -euo pipefail
cd "$(dirname "$0")"

CSV="${1:?usage: ./quickstart.sh my_components.csv [my_checks.yaml]}"
CHECKS="${2:-}"
PY="${PYTHON:-python3}"

: "${ES_URL:?set ES_URL}"
: "${KIBANA_URL:?set KIBANA_URL}"
if [[ -z "${ELASTIC_API_KEY:-}" && -z "${ELASTIC_USERNAME:-}" ]]; then
  echo "set ELASTIC_API_KEY or ELASTIC_USERNAME/ELASTIC_PASSWORD" >&2; exit 1
fi

step() { printf '\n=== %s\n' "$*"; }

step "1/7 Python dependencies"
"$PY" -c "import requests, yaml" 2>/dev/null || "$PY" -m pip install -q -r requirements.txt

step "2/7 Component table, checks table and ingest pipeline"
"$PY" components.py init

step "3/7 Load your components from $CSV"
"$PY" components.py import "$CSV"
if [[ -n "$CHECKS" ]]; then
  "$PY" components.py import-checks "$CHECKS"
fi
"$PY" components.py list

step "4/7 Validate every component's queries against your data"
if ! "$PY" components.py validate; then
  echo "!! Some queries failed (see FAIL lines above). Fix those rows with"
  echo "!! 'components.py add <component> ...' and re-run. Continuing with the deploy."
fi

step "5/7 Deploy tools + agent to Kibana"
"$PY" setup_agent.py apply

step "6/7 Test the agent's tools through Kibana"
"$PY" setup_agent.py test

step "7/7 Read-only role limited to the registered indices"
"$PY" components.py role

cat <<'EOF'

Done.
  Kibana > Agents > "App Health Agent"   (assign role app_health_agent_user to its users)
  Try:  python setup_agent.py ask "health check of <component>"
Add a component later:   python components.py add <name> --log-indices ... && python components.py role
EOF
