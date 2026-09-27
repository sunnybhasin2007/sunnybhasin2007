# App Health Agent for Kibana Agent Builder (Elastic 9.4)

Ask the Kibana **Agent Builder** chat things like:

- *"health check of kafka"*
- *"any issues in payments in the last 4 hours?"*
- *"why is payments-api failing? find the root cause"*

The agent looks the application up in a **component table** (an Elasticsearch
index you maintain), finds the indices that hold that component's **logs, metrics
and traces**, and runs its checks **only on those indices**. It never searches
the whole cluster.

Adding an application means adding one row to the table. You don't touch or
redeploy the agent.

## Quick start (end to end, one script)

1. Put your components and their indices in a CSV (start from `config/components.minimal.csv`):
   ```csv
   component,log_indices,metric_indices,trace_indices
   kafka,logs-kafka.otel-*,metrics-kafka.otel-*,
   payments-api,logs-payments_api.otel-*,,traces-payments_api.otel-*
   ```
   Several patterns in one cell: separate them with `;`. Optional extra columns:
   `aliases`, `depends_on`, `owner`, `description`, and `service_name` if a component shares indices with others.
2. Run:
   ```bash
   export ES_URL=https://es:9200 KIBANA_URL=https://kibana:5601 ELASTIC_API_KEY=...   # + ELASTIC_CA_CERT
   ./quickstart.sh my_components.csv            # optional 2nd arg: checks YAML
   ```
   It creates the table, loads your CSV, validates every query on your data, deploys the
   agent and tools to Kibana, tests them and creates the read-only role.
3. Kibana → Agents → **App Health Agent** → ask anything about a component.

## How it works

```
 you: "is kafka ok?"
        │
        ▼
 App Health Agent ── 1. apphealth.list_components ──► app-health-components (the table)
        │                "kafka" / "brokers" / "event bus" → component "kafka"
        │          ── 2. apphealth.get_component("kafka")
        │                 log_indices, metric_indices, trace_indices,
        │                 filters (service.name == "kafka") and READY-TO-RUN queries
        │          ── 3. apphealth.get_component_checks("kafka")  (custom checks, e.g. consumer lag)
        │          ── 4. platform.core.execute_esql  × (log_health, metric_freshness, consumer_lag, ...)
        ▼
 "DEGRADED - broker-2 has sent no logs for 24 min; 12% errors 'Shrinking ISR ...';
  consumer group payments lag peaked at 89,812 ..."
```

| Piece | Purpose |
|---|---|
| `app-health-components` index | **The table.** One row per component with its log/metric/trace indices, OTel `service.name`, aliases, owner, `depends_on`. |
| `app-health-components-render` ingest pipeline | Runs on every write to the table. It checks the row and generates the component's ready-to-run ES\|QL queries (`queries.*`) from `config/query_templates.yaml`. Rows added from Dev Tools get queries too. |
| `app-health-checks` index | Optional custom ES\|QL checks per component (Kafka consumer lag, etc.). |
| 3 agent tools | `list_components`, `get_component`, `get_component_checks`. They stay the same however many apps you add. |
| `platform.core.execute_esql` | Runs the stored queries. |
| `components.py` | CLI to manage the table: add, import/export CSV, checks, validate, role. |
| `setup_agent.py` | Deploys the tools and the agent to Kibana. |

### Stored queries per component

| Signal | Queries |
|---|---|
| logs | `log_health` (per host: events, errors, warnings, error rate, minutes since last log), `log_errors` (top error patterns), `log_timeline` (5-min buckets), `log_search` (text search), `log_by_trace` (all logs of a trace) |
| metrics | `metric_freshness` (are metrics still arriving, per index) |
| traces | `trace_summary` (per operation: calls, errors, avg/p95 ms), `trace_errors` (failing spans + example trace id), `trace_timeline` |

Field names default to **OpenTelemetry-native** data as stored by Elastic
(`logs-*.otel-*`, `traces-*.otel-*`, `metrics-*.otel-*`): `severity_number`,
`body.text`, `resource.attributes.service.name`, `resource.attributes.host.name`,
span `duration` (ns), `status.code`, `trace_id`. Set `schema: ecs` on a row for
Filebeat/ECS or Elastic APM data. You can also override any single field on a row.

## Setup (once)

```bash
pip install -r requirements.txt

export ES_URL=https://es.mycorp.local:9200
export KIBANA_URL=https://kibana.mycorp.local:5601
export ELASTIC_API_KEY=<base64 id:key>          # or ELASTIC_USERNAME / ELASTIC_PASSWORD
export ELASTIC_CA_CERT=/etc/pki/elastic-ca.crt  # on-prem CA (or use --insecure)

python components.py init        # pipeline + component table + checks table
python setup_agent.py apply      # 3 tools + the agent
python setup_agent.py test       # runs the 3 tools through Kibana
```

Then open **Kibana → Agents → App Health Agent**. The agent uses your default
LLM connector, or pick one in the chat.

> Can't run Python against the cluster? Run `python setup_agent.py render` anywhere
> and paste `out/devtools_console.txt` into **Kibana → Dev Tools**.

## Adding your applications

### One at a time
```bash
python components.py add kafka \
  --display-name "Apache Kafka" \
  --aliases "kafka cluster, brokers, event bus" \
  --service-name kafka \
  --log-indices "logs-*.otel-*" \
  --metric-indices "metrics-*.otel-*" \
  --depends-on zookeeper \
  --owner messaging-team

python components.py add payments-api --service-name payments-api \
  --log-indices "logs-*.otel-*" --trace-indices "traces-*.otel-*" --depends-on kafka
```
`add` on an existing component only changes the fields you pass.

`--service-name` makes every query filter on
`resource.attributes.service.name == "<name>"`. This matters because the OTel
exporter usually writes all services into the same data streams
(e.g. `logs-generic.otel-default`). If a component needs a different filter,
set it directly, for example:
`--log-filter 'resource.attributes.host.name LIKE "kafka-*"'` or
`--metric-filter 'resource.attributes.service.name IN ("kafka", "kafka-jmx")'`.

### Many at once (CSV)
```bash
python components.py export components.csv    # edit in Excel / a text editor
python components.py import components.csv     # adds or replaces rows by component id
```
See `config/components.example.csv`. List columns (`aliases`, `depends_on`,
`*_indices`) take `;`-separated values.

### From Kibana Dev Tools (no script)
```
PUT app-health-components/_doc/zookeeper?refresh=true
{
  "component": "zookeeper",
  "aliases": ["zk"],
  "service_name": "zookeeper",
  "log_indices": ["logs-*.otel-*"],
  "depends_on": []
}
```
The pipeline generates the queries. `GET app-health-components/_doc/zookeeper` shows them.

### Custom checks
```bash
python components.py add-check kafka consumer_lag \
  --description "Consumer lag per group/topic; high or growing = consumers falling behind" \
  --query-file kafka_lag.esql
python components.py import-checks config/checks.example.yaml
```
Write `{MINUTES}` where the time window goes, and the agent fills it in:
`WHERE @timestamp >= NOW() - {MINUTES} minutes AND resource.attributes.service.name == "kafka"`.
Use the metric names your OTel receiver emits. Check them with
`GET metrics-*.otel-*/_mapping` or in Discover.

### Always validate
```bash
python components.py validate            # every query of every component
python components.py validate kafka --minutes 240
```
This runs every stored query and check against your real data, and shows
`Unknown index`, `Unknown column` or bad-filter errors before a user hits them in chat.

### Other commands
`list`, `show NAME` (row + rendered queries + checks), `remove NAME`,
`remove-check NAME ID`, `rerender` (after editing `config/query_templates.yaml`,
run `init` then `rerender`).

## Restricting who can use it, and what it can read
```bash
python components.py role      # creates role app_health_agent_user
```
The role gives read access to the registered indices and the two tables, plus
Kibana **Agent Builder: read** and **Actions and Connectors: read**. Assign it
(or map your LDAP/AD group to it) for the users who should use the agent. Tools
run with the chat user's permissions, so even ad-hoc ES|QL from the agent can't
read other indices. Re-run `role` after adding components with new index patterns.
I checked this on 9.4.6: a user with this role got `Unknown index` when querying
an unregistered index through the agent's ES|QL tool.

## Files
```
components.py              manage the component table (CLI)
setup_agent.py             deploy tools + agent to Kibana
apphealth_lib.py           shared code: pipeline, mappings, HTTP client
config/agent.yaml          agent name, index names, tool prefix, role features
config/query_templates.yaml  ES|QL templates and OTel/ECS field presets
config/components.example.csv, config/checks.example.yaml   examples
```

## Troubleshooting
- **`Unknown column [severity_number]`** (or similar): that component isn't
  OTel-native. Set `--schema ecs` or override `--error-condition`, `--host-field`, etc.
- **`Unknown index`**: the pattern matches nothing. Check `GET _cat/indices/logs-*otel*?v`.
- **0 rows but data exists**: the filter is wrong. Check the `service.name` value in
  Discover, or set `--log-filter`.
- **`MATCH` errors in `log_search`**: the message field must be a text field;
  override `--message-field`.
- **Non-default Kibana space**: set `kibana_space` in `config/agent.yaml`.
- `python setup_agent.py destroy` removes the agent and its tools. The table is kept.
